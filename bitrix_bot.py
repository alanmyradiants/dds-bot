import os
import io
import csv
import json
import base64
import threading
import requests
from urllib.parse import parse_qs
from flask import Flask, request, jsonify
import anthropic

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

ANTHROPIC_API_KEY       = os.getenv("ANTHROPIC_API_KEY", "").strip()
BITRIX_WEBHOOK_URL      = os.getenv("BITRIX_WEBHOOK_URL", "https://joto.bitrix24.ru/rest/1/ge7hgsje88e51nuw").rstrip("/")
BITRIX_DISK_WEBHOOK_URL = os.getenv("BITRIX_DISK_WEBHOOK_URL", "https://joto.bitrix24.ru/rest/1/g4s7w21uysosjds7").rstrip("/")
BOT_CLIENT_ID           = os.getenv("BOT_CLIENT_ID", "glhjxdm0jwb216zd3kdau2mwtf4z0fbu")

# Извлекаем токен из disk-вебхука для Bearer-авторизации
DISK_TOKEN = BITRIX_DISK_WEBHOOK_URL.rstrip("/").split("/")[-1]

DDS_CATEGORIES = [
    "Поступления от покупателей",
    "Оплата поставщикам",
    "Заработная плата",
    "Налоги и взносы",
    "Аренда",
    "Коммунальные услуги",
    "Банковские комиссии",
    "Кредиты и займы (получение)",
    "Кредиты и займы (погашение)",
    "Проценты по кредитам",
    "Дивиденды",
    "Покупка ОС и НМА",
    "Прочие поступления",
    "Прочие выплаты",
    "Внутренние переводы",
]

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ─────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────

def safe_preview(value, limit=2000):
    try:
        text = str(value)
    except Exception:
        text = repr(value)
    return text[:limit] + " ...[truncated]" if len(text) > limit else text


def parse_request_data():
    try:
        if request.is_json:
            return request.get_json(force=True) or {}
    except Exception:
        pass
    if request.form:
        return request.form.to_dict()
    raw = request.get_data(as_text=True)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
        try:
            parsed = parse_qs(raw)
            return {k: v[0] for k, v in parsed.items()}
        except Exception:
            pass
    return {}


# ─────────────────────────────────────────────
# Bitrix API
# ─────────────────────────────────────────────

def bitrix_post(method_name, payload, timeout=20):
    url = f"{BITRIX_WEBHOOK_URL}/{method_name}.json"
    response = requests.post(url, json=payload, timeout=timeout)
    print(f"{method_name} POST status={response.status_code}")
    print(f"{method_name} POST response={safe_preview(response.text, 3000)}")
    return response


def send_message(dialog_id, text):
    if not dialog_id:
        print("send_message skipped: no dialog_id")
        return
    try:
        bitrix_post(
            "imbot.message.add",
            {"DIALOG_ID": dialog_id, "MESSAGE": text, "CLIENT_ID": BOT_CLIENT_ID},
            timeout=15,
        )
    except Exception as e:
        print(f"send_message error: {e}")


def send_file(dialog_id, filename, content_bytes):
    if not dialog_id:
        print("send_file skipped: no dialog_id")
        return

    encoded = base64.b64encode(content_bytes).decode()

    # Вариант 1: im.disk.file.commit через основной вебхук
    try:
        resp = requests.post(
            f"{BITRIX_WEBHOOK_URL}/im.disk.file.commit.json",
            json={
                "DIALOG_ID": dialog_id,
                "FILE_NAME": filename,
                "FILE_CONTENT": encoded,
            },
            timeout=60,
        )
        print(f"im.disk.file.commit status={resp.status_code}")
        print(f"im.disk.file.commit response={safe_preview(resp.text, 500)}")
        result = resp.json()
        if resp.status_code == 200 and "error" not in result:
            print("send_file: im.disk.file.commit success")
            return
    except Exception as e:
        print(f"im.disk.file.commit error: {e}")

    # Вариант 2: загрузка через multipart на диск + ссылка
    try:
        storage_resp = requests.get(
            f"{BITRIX_WEBHOOK_URL}/disk.storage.getlist.json",
            timeout=20
        )
        print(f"storage.getlist status={storage_resp.status_code}")
        storages = storage_resp.json().get("result", [])
        user_storage = next((s for s in storages if str(s.get("TYPE")) == "1"), None)
        if not user_storage and storages:
            user_storage = storages[0]

        if user_storage:
            root_folder_id = user_storage.get("ROOT_OBJECT_ID") or user_storage.get("ID")
            print(f"Root folder ID: {root_folder_id}")

            upload_resp = requests.post(
                f"{BITRIX_WEBHOOK_URL}/disk.folder.uploadfile.json",
                data={"id": root_folder_id, "data[NAME]": filename},
                files={"file": (filename, content_bytes, "text/csv")},
                timeout=60,
            )
            print(f"uploadfile multipart status={upload_resp.status_code}")
            print(f"uploadfile multipart response={safe_preview(upload_resp.text, 1000)}")

            upload_result = upload_resp.json().get("result", {})
            download_url = (
                upload_result.get("DOWNLOAD_URL")
                or upload_result.get("DETAIL_URL")
            )
            if download_url:
                send_message(dialog_id, f"📎 [url={download_url}]Скачать {filename}[/url]")
                return

    except Exception as e:
        print(f"send_file upload error: {e}")

    send_message(dialog_id, f"⚠️ Не удалось отправить файл автоматически. Обратитесь к администратору.")


# ─────────────────────────────────────────────
# Работа с файлом из Bitrix
# ─────────────────────────────────────────────

def find_pdf_in_payload(data):
    result = {
        "file_id": None, "chat_id": None,
        "url_download": None, "url_show": None,
        "unified_link": None, "filename": None,
    }
    for key, val in data.items():
        key_upper = key.upper()
        if "FILES" in key_upper and key_upper.endswith("][NAME]") and val:
            val_str = str(val)
            if val_str.lower().endswith(".pdf"):
                base = key[: -len("][NAME]")]

                def get_field(*suffixes):
                    for suffix in suffixes:
                        for candidate in [base + suffix, base + suffix.lower(), base + suffix.upper()]:
                            if candidate in data:
                                return data[candidate]
                    return None

                result["filename"] = val_str
                result["file_id"] = get_field("][ID]", "][id]")
                result["chat_id"] = get_field("][CHATID]", "][chatId]")
                result["url_download"] = get_field("][URLDOWNLOAD]", "][urlDownload]")
                result["url_show"] = get_field("][URLSHOW]", "][urlShow]")
                result["unified_link"] = get_field("][VIEWERATTRS][UNIFIEDLINK]", "][viewerAttrs][unifiedLink]")
                return result

    file_id = data.get("data[PARAMS][FILE_ID][0]") or data.get("data[PARAMS][PARAMS][FILE_ID][0]")
    if file_id:
        result["file_id"] = file_id
        result["filename"] = "document.pdf"
    return result


def extract_download_url(file_info):
    for key in ["DOWNLOAD_URL", "downloadUrl", "DOWNLOAD_URL_MACHINE", "URL_DOWNLOAD"]:
        value = file_info.get(key)
        if value:
            return value
    return None


def try_download(url, extra_headers=None):
    """
    Скачивает файл по URL.
    Принимает: application/pdf, application/octet-stream, или байты начинающиеся с %PDF.
    Отвергает только явный HTML (вероятно, страница логина).
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,application/octet-stream,*/*",
    }
    if extra_headers:
        headers.update(extra_headers)

    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    content_type = (resp.headers.get("Content-Type") or "").lower()
    first_bytes = resp.content[:20] if resp.content else b""
    content_len = len(resp.content)

    print(f"try_download status={resp.status_code} "
          f"content_type={content_type} "
          f"size={content_len} "
          f"first={first_bytes}")

    if resp.status_code != 200:
        print(f"try_download FAIL: non-200 status")
        return None

    # Явный HTML — скорее всего редирект на страницу логина
    if "text/html" in content_type:
        print(f"try_download FAIL: got HTML (login page?), body={safe_preview(resp.text, 300)}")
        return None

    # Принимаем PDF, octet-stream, или любой файл начинающийся с %PDF
    is_pdf_content_type = "application/pdf" in content_type
    is_octet_stream     = "application/octet-stream" in content_type
    is_pdf_magic        = first_bytes.startswith(b"%PDF")

    if is_pdf_content_type or is_octet_stream or is_pdf_magic:
        print(f"try_download OK: got {content_len} bytes")
        return resp.content

    # Неизвестный тип — принимаем если размер разумный (>1KB) и не HTML
    if content_len > 1024:
        print(f"try_download OK (unknown type, size OK): {content_type}, {content_len} bytes")
        return resp.content

    print(f"try_download FAIL: unrecognized content_type={content_type}, size={content_len}")
    return None


def get_pdf_bytes(file_id, fallback_url=None):
    # Вариант 1: disk.file.get через основной вебхук
    try:
        url = f"{BITRIX_WEBHOOK_URL}/disk.file.get.json"
        response = requests.get(url, params={"id": file_id}, timeout=20)
        print(f"disk.file.get via main webhook status={response.status_code}")
        if response.status_code == 200:
            payload = response.json()
            if "result" in payload and payload["result"]:
                download_url = extract_download_url(payload["result"])
                if download_url:
                    print(f"Variant 1 DOWNLOAD_URL: {safe_preview(download_url, 200)}")
                    result = try_download(download_url)
                    if result:
                        return result
    except Exception as e:
        print(f"disk.file.get via main webhook failed: {e}")

    # Вариант 2: disk.file.get через disk-вебхук
    try:
        url = f"{BITRIX_DISK_WEBHOOK_URL}/disk.file.get.json"
        response = requests.get(url, params={"id": file_id}, timeout=20)
        print(f"disk.file.get via disk webhook status={response.status_code}")
        if response.status_code == 200:
            payload = response.json()
            print(f"disk.file.get result keys: {list(payload.get('result', {}).keys())}")
            if "result" in payload and payload["result"]:
                download_url = extract_download_url(payload["result"])
                print(f"Variant 2 extracted download_url: {safe_preview(download_url, 200)}")
                if download_url:
                    result = try_download(download_url)
                    if result:
                        return result
                    print("Variant 2 try_download returned None")
                else:
                    print(f"Variant 2: extract_download_url=None. Full result: {safe_preview(payload['result'], 1000)}")
    except Exception as e:
        print(f"disk.file.get via disk webhook failed: {e}")

    # Вариант 3: fallback_url с Bearer токеном основного вебхука
    if fallback_url:
        main_token = BITRIX_WEBHOOK_URL.rstrip("/").split("/")[-1]
        print(f"Variant 3: fallback_url with main Bearer token")
        result = try_download(fallback_url, {"Authorization": f"Bearer {main_token}"})
        if result:
            return result

        # Вариант 4: fallback_url с Bearer токеном disk-вебхука
        print(f"Variant 4: fallback_url with disk Bearer token")
        result = try_download(fallback_url, {"Authorization": f"Bearer {DISK_TOKEN}"})
        if result:
            return result

    raise ValueError(
        "Не удалось скачать PDF. "
        "Проверьте логи — нужно посмотреть какой статус возвращает disk.file.get."
    )


# ─────────────────────────────────────────────
# Обработка через Claude
# ─────────────────────────────────────────────

def extract_transactions(pdf_bytes):
    if not ANTHROPIC_API_KEY:
        raise ValueError("Не указан ANTHROPIC_API_KEY")

    pdf_b64 = base64.b64encode(pdf_bytes).decode()
    system_prompt = (
        "Из банковской выписки извлеки транзакции и распредели по статьям ДДС.\n"
        f"Статьи: {', '.join(DDS_CATEGORIES)}\n"
        "Верни ТОЛЬКО JSON массив без markdown:\n"
        '[{"date":"ДД.ММ.ГГГГ","description":"текст","amount":100.0,'
        '"type":"in","category":"статья","counterparty":"контрагент"}]\n'
        "type: in=поступление, out=списание. amount всегда положительное."
    )

    with client.messages.stream(
        model="claude-opus-4-6",
        max_tokens=32000,
        system=system_prompt,
        messages=[{
            "role": "user",
            "content": [
                {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": pdf_b64}},
                {"type": "text", "text": "Извлеки все транзакции."},
            ],
        }],
    ) as stream:
        text = stream.get_final_text()
    print(f"Claude FULL response: {safe_preview(text, 5000)}")

    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"Транзакции не найдены. Ответ Claude: {safe_preview(text, 300)}")

    json_str = text[start:end + 1]
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        last_close = json_str.rfind("}")
        if last_close == -1:
            raise ValueError("Не удалось распарсить ответ ИИ")
        return json.loads(json_str[:last_close + 1] + "]")


def to_csv(transactions):
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(["Дата", "Контрагент", "Описание", "Приход", "Расход", "Статья ДДС"])
    for t in transactions:
        amount = float(t.get("amount", 0) or 0)
        inc = f"{amount:.2f}" if t.get("type") == "in" else ""
        exp = f"{amount:.2f}" if t.get("type") == "out" else ""
        writer.writerow([
            t.get("date", ""), t.get("counterparty", ""),
            t.get("description", ""), inc, exp, t.get("category", ""),
        ])
    return ("\ufeff" + out.getvalue()).encode("utf-8")


# ─────────────────────────────────────────────
# Фоновая обработка PDF
# ─────────────────────────────────────────────

def process_pdf_async(dialog_id, file_id, fallback_url):
    try:
        pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url)
        send_message(dialog_id, "🔍 Анализирую выписку через ИИ...")
        transactions = extract_transactions(pdf_bytes)
        total_in  = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "in")
        total_out = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "out")
        csv_bytes = to_csv(transactions)
        send_message(
            dialog_id,
            f"✅ Готово! Найдено {len(transactions)} транзакций.\n"
            f"📈 Поступления: {total_in:,.2f} ₽\n"
            f"📉 Списания: {total_out:,.2f} ₽",
        )
        send_file(dialog_id, "ДДС_выписка.csv", csv_bytes)
    except Exception as e:
        print(f"process_pdf_async ERROR: {e}")
        send_message(dialog_id, f"❌ Ошибка: {str(e)}")


# ─────────────────────────────────────────────
# Webhook handler
# ─────────────────────────────────────────────

@app.route("/bot", methods=["GET", "POST"])
def bot_handler():
    if request.method == "GET":
        return jsonify({"result": "ok"})

    data = parse_request_data()

    print("===== WEBHOOKS =====")
    print(f"BOT:  {BITRIX_WEBHOOK_URL}")
    print(f"DISK: {BITRIX_DISK_WEBHOOK_URL}")
    print("===== INCOMING REQUEST =====")
    print(safe_preview(data, 10000))

    event = data.get("event", "")
    print(f"EVENT: {event}")

    if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
        return jsonify({"result": "ok", "skipped": True})

    dialog_id = (
        data.get("data[PARAMS][DIALOG_ID]")
        or data.get("data[PARAMS][TO_CHAT_ID]")
    )
    message_text = str(data.get("data[PARAMS][MESSAGE]", "")).strip().lower()

    file_info = find_pdf_in_payload(data)
    print("===== FOUND FILE INFO =====")
    print(safe_preview(file_info, 4000))

    file_id      = file_info.get("file_id")
    filename     = file_info.get("filename") or ""
    fallback_url = file_info.get("url_download")

    if filename.lower().endswith(".pdf") and file_id:
        send_message(dialog_id, "📄 Получил PDF, начинаю обработку...")
        thread = threading.Thread(
            target=process_pdf_async,
            args=(dialog_id, file_id, fallback_url),
            daemon=True,
        )
        thread.start()

    elif message_text in ("привет", "start", "/start", "помощь", "help", ""):
        send_message(
            dialog_id,
            "👋 Привет! Пришли PDF-выписку из банка — "
            "я разнесу транзакции по статьям ДДС и верну CSV-файл.",
        )
    else:
        send_message(dialog_id, "Пришли PDF-выписку из банка.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("===== STARTING APP =====")
    print(f"BOT WEBHOOK:  {BITRIX_WEBHOOK_URL}")
    print(f"DISK WEBHOOK: {BITRIX_DISK_WEBHOOK_URL}")
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
