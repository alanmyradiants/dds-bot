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

    # Шаг 1: находим корневую папку пользователя на Диске
    try:
        storage_resp = requests.get(
            f"{BITRIX_WEBHOOK_URL}/disk.storage.getlist.json",
            timeout=20
        )
        print(f"storage.getlist status={storage_resp.status_code}")
        storages = storage_resp.json().get("result", [])
        # Берём личное хранилище пользователя (type=1)
        user_storage = next((s for s in storages if s.get("TYPE") == "1"), None)
        if not user_storage:
            user_storage = storages[0] if storages else None

        if user_storage:
            root_folder_id = user_storage.get("ROOT_OBJECT_ID") or user_storage.get("ID")
            print(f"Root folder ID: {root_folder_id}")

            # Шаг 2: загружаем файл в корневую папку
            upload_resp = requests.post(
                f"{BITRIX_WEBHOOK_URL}/disk.folder.uploadfile.json",
                json={
                    "id": root_folder_id,
                    "fileContent": [filename, encoded],
                    "data": {"NAME": filename},
                },
                timeout=60,
            )
            print(f"uploadfile status={upload_resp.status_code}")
            print(f"uploadfile response={safe_preview(upload_resp.text, 2000)}")

            upload_result = upload_resp.json().get("result", {})
            download_url = upload_result.get("DOWNLOAD_URL") or upload_result.get("DETAIL_URL")

            if download_url:
                send_message(
                    dialog_id,
                    f"📎 Файл готов: [url={download_url}]{filename}[/url]"
                )
                return

    except Exception as e:
        print(f"send_file upload error: {e}")

    # Запасной: отправляем через im.disk.file.commit
    try:
        resp = bitrix_post(
            "im.disk.file.commit",
            {"DIALOG_ID": dialog_id, "FILE_NAME": filename, "FILE_CONTENT": encoded},
            timeout=60,
        )
        if resp.status_code == 200 and "error" not in resp.json():
            print("send_file: im.disk.file.commit success")
            return
    except Exception as e:
        print(f"im.disk.file.commit error: {e}")

    send_message(dialog_id, f"⚠️ Не удалось отправить файл. Обратитесь к администратору.")


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


def get_disk_file_info(file_id):
    url = f"{BITRIX_DISK_WEBHOOK_URL}/disk.file.get.json"
    response = requests.get(url, params={"id": file_id}, timeout=20)
    print(f"disk.file.get GET status={response.status_code}")
    print(f"disk.file.get GET response={safe_preview(response.text, 4000)}")
    if response.status_code != 200:
        raise ValueError(f"disk.file.get вернул HTTP {response.status_code}")
    payload = response.json()
    if "error" in payload:
        raise ValueError(payload.get("error_description") or payload.get("error"))
    result = payload.get("result", {})
    if not result:
        raise ValueError("disk.file.get вернул пустой result")
    return result


def extract_download_url(file_info):
    for key in ["DOWNLOAD_URL", "downloadUrl", "DOWNLOAD_URL_MACHINE", "URL_DOWNLOAD"]:
        value = file_info.get(key)
        if value:
            return value
    return None


def try_download(url, extra_headers=None):
    """Попытка скачать PDF по URL."""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf,application/octet-stream,*/*",
    }
    if extra_headers:
        headers.update(extra_headers)
    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    content_type = (resp.headers.get("Content-Type") or "").lower()
    print(f"try_download status={resp.status_code} content_type={content_type} first={resp.content[:20]}")
    if resp.status_code == 200 and (
        "application/pdf" in content_type or resp.content.startswith(b"%PDF")
    ):
        return resp.content
    return None


def get_pdf_bytes(file_id, fallback_url=None):
    # Вариант 1: disk.file.get через основной вебхук (im+disk+imbot)
    try:
        url = f"{BITRIX_WEBHOOK_URL}/disk.file.get.json"
        response = requests.get(url, params={"id": file_id}, timeout=20)
        print(f"disk.file.get via main webhook status={response.status_code}")
        if response.status_code == 200:
            payload = response.json()
            if "result" in payload and payload["result"]:
                download_url = extract_download_url(payload["result"])
                if download_url:
                    print(f"DOWNLOAD URL: {safe_preview(download_url, 300)}")
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
            if "result" in payload and payload["result"]:
                download_url = extract_download_url(payload["result"])
                if download_url:
                    result = try_download(download_url)
                    if result:
                        return result
    except Exception as e:
        print(f"disk.file.get via disk webhook failed: {e}")

    # Вариант 3: fallback_url с Bearer токеном основного вебхука
    if fallback_url:
        main_token = BITRIX_WEBHOOK_URL.rstrip("/").split("/")[-1]
        print(f"Trying fallback_url with main webhook Bearer token...")
        result = try_download(fallback_url, {"Authorization": f"Bearer {main_token}"})
        if result:
            return result

        # Вариант 4: fallback_url с Bearer токеном disk-вебхука
        print(f"Trying fallback_url with disk webhook Bearer token...")
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

    msg = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=8000,
        system=system_prompt,
        messages=[{
            "role": "user",
            "content": [
                {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": pdf_b64}},
                {"type": "text", "text": "Извлеки все транзакции."},
            ],
        }],
    )

    text = msg.content[0].text
    print(f"Claude response preview: {safe_preview(text, 1500)}")

    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end < start:
        raise ValueError("Транзакции не найдены в ответе ИИ")

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

    file_id    = file_info.get("file_id")
    filename   = file_info.get("filename") or ""
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
