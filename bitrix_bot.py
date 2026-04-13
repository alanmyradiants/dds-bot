import os
import io
import csv
import json
import base64
import requests
from urllib.parse import parse_qs
from flask import Flask, request, jsonify
import anthropic

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL", "")

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


def parse_request_data():
    """Парсим данные запроса в любом формате."""
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


def send_message(dialog_id, text):
    if not dialog_id:
        print("send_message skipped: no dialog_id")
        return

    try:
        r = requests.post(
            f"{BITRIX_WEBHOOK_URL}/im.message.add.json",
            json={"DIALOG_ID": dialog_id, "MESSAGE": text},
            timeout=10,
        )
        print(f"send_message status={r.status_code}")
    except Exception as e:
        print(f"send_message error: {e}")


def send_file(dialog_id, filename, content_bytes):
    if not dialog_id:
        print("send_file skipped: no dialog_id")
        return

    try:
        encoded = base64.b64encode(content_bytes).decode()
        r = requests.post(
            f"{BITRIX_WEBHOOK_URL}/im.disk.file.commit.json",
            json={
                "DIALOG_ID": dialog_id,
                "FILE_NAME": filename,
                "FILE_CONTENT": encoded,
            },
            timeout=30,
        )
        print(f"send_file status={r.status_code}")
    except Exception as e:
        print(f"send_file error: {e}")


def get_file_download_url(file_id, chat_id=None, unified_link=None):
    """
    Получаем рабочую ссылку для скачивания файла.
    unified_link используем только как самый крайний fallback.
    """
    parts = BITRIX_WEBHOOK_URL.rstrip("/").split("/")
    token = parts[-1]
    domain = "/".join(parts[:3])

    direct_url = (
        f"{domain}/bitrix/components/bitrix/im.messenger/download.file.php"
        f"?fileId={file_id}&auth={token}"
    )

    if chat_id:
        try:
            r = requests.get(
                f"{BITRIX_WEBHOOK_URL}/im.chat.files.get.json",
                params={"CHAT_ID": chat_id, "LIMIT": 50},
                timeout=10,
            )
            payload = r.json()
            result = payload.get("result", {})
            print(f"im.chat.files.get raw: {str(payload)[:700]}")

            files = result.get("files", [])
            if isinstance(files, dict):
                files = list(files.values())

            for f in files:
                if str(f.get("id")) == str(file_id):
                    url_download = f.get("urlDownload") or f.get("url_download")
                    if url_download:
                        print(f"Found urlDownload via im.chat.files.get: {url_download[:200]}")
                        return url_download

                    disk_id = f.get("diskId") or f.get("DISK_ID")
                    if disk_id:
                        r2 = requests.get(
                            f"{BITRIX_WEBHOOK_URL}/disk.file.get.json",
                            params={"id": disk_id},
                            timeout=10,
                        )
                        payload2 = r2.json()
                        disk_result = payload2.get("result", {})
                        print(f"disk.file.get raw: {str(payload2)[:500]}")

                        download_url = disk_result.get("DOWNLOAD_URL")
                        if download_url:
                            print(f"Found DOWNLOAD_URL via disk.file.get: {download_url[:200]}")
                            return download_url
        except Exception as e:
            print(f"get_file_download_url error: {e}")

    if unified_link:
        print(f"Fallback unifiedLink available but not preferred: {unified_link[:200]}")

    return direct_url


def download_file(url):
    """
    Скачиваем файл и убеждаемся, что это реально PDF, а не HTML.
    """
    parts = BITRIX_WEBHOOK_URL.rstrip("/").split("/")
    token = parts[-1]

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Authorization": f"Bearer {token}",
    }

    last_error = None
    urls_to_try = [url]

    url_with_auth = f"{url}{'&' if '?' in url else '?'}auth={token}"
    if url_with_auth != url:
        urls_to_try.append(url_with_auth)

    for try_url in urls_to_try:
        try:
            r = session.get(
                try_url,
                headers=headers,
                timeout=60,
                allow_redirects=True,
            )

            content_type = (r.headers.get("Content-Type") or "").lower()
            first_bytes = r.content[:12]

            print(
                f"Download url={try_url[:180]} "
                f"status={r.status_code} "
                f"content_type={content_type} "
                f"size={len(r.content)} "
                f"first_bytes={first_bytes}"
            )

            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}"
                continue

            if "application/pdf" in content_type or r.content.startswith(b"%PDF"):
                return r.content

            preview = ""
            try:
                preview = r.text[:300].replace("\n", " ")
            except Exception:
                preview = str(r.content[:120])

            last_error = f"Получили не PDF, а {content_type}: {preview}"

        except Exception as e:
            last_error = str(e)

    raise ValueError(f"Не удалось скачать PDF. {last_error}")


def extract_transactions(pdf_bytes):
    """
    Отправляем PDF в Claude и просим вернуть JSON массив транзакций.
    """
    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    system_prompt = f"""Из банковской выписки извлеки транзакции и распредели по статьям ДДС.
Статьи: {', '.join(DDS_CATEGORIES)}
Верни ТОЛЬКО JSON массив:
[{{"date":"ДД.ММ.ГГГГ","description":"текст","amount":100.0,"type":"in","category":"статья","counterparty":"контрагент"}}]
type: in=поступление, out=списание. amount всегда положительное."""

    msg = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=8000,
        system=system_prompt,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": pdf_b64,
                        },
                    },
                    {"type": "text", "text": "Извлеки все транзакции."},
                ],
            }
        ],
    )

    text = msg.content[0].text
    print(f"Claude response preview: {text[:1000]}")

    start = text.find("[")
    end = text.rfind("]")

    if start == -1 or end == -1 or end < start:
        raise ValueError("Транзакции не найдены в ответе ИИ")

    json_str = text[start:end + 1]
    return json.loads(json_str)


def to_csv(transactions):
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", quoting=csv.QUOTE_ALL)
    writer.writerow(["Дата", "Контрагент", "Описание", "Приход", "Расход", "Статья ДДС"])

    for t in transactions:
        amount = float(t.get("amount", 0) or 0)
        inc = f"{amount:.2f}" if t.get("type") == "in" else ""
        exp = f"{amount:.2f}" if t.get("type") == "out" else ""

        writer.writerow([
            t.get("date", ""),
            t.get("counterparty", ""),
            t.get("description", ""),
            inc,
            exp,
            t.get("category", ""),
        ])

    return ("\ufeff" + out.getvalue()).encode("utf-8")


def find_pdf_in_payload(data):
    """
    Ищем PDF в плоском payload Bitrix.
    Возвращаем словарь с найденными полями.
    """
    result = {
        "file_id": None,
        "url_direct": None,
        "chat_id": None,
        "unified_link": None,
        "filename": None,
    }

    # 1. Ищем нормальный FILES[*][urlDownload]
    for key, val in data.items():
        if "FILES" in key and key.endswith("][urlDownload]") and val:
            name_key = key.replace("][urlDownload]", "][name]")
            fname = data.get(name_key, "")

            if fname.lower().endswith(".pdf"):
                result["url_direct"] = val
                result["filename"] = fname

                chat_key = key.replace("][urlDownload]", "][chatId]")
                result["chat_id"] = data.get(chat_key)

                unified_key = key.replace("][urlDownload]", "][viewerAttrs][unifiedLink]")
                result["unified_link"] = data.get(unified_key)

                id_key = key.replace("][urlDownload]", "][id]")
                result["file_id"] = data.get(id_key)
                return result

    # 2. Если urlDownload не найден, ищем любой pdf по name/id
    temp_candidates = {}

    for key, val in data.items():
        if "FILES" in key and key.endswith("][name]") and val:
            if str(val).lower().endswith(".pdf"):
                base = key[:-len("][name]")]
                temp_candidates[base] = {"filename": val}

    for base, info in temp_candidates.items():
        id_key = f"{base}][id]"
        chat_key = f"{base}][chatId]"
        unified_key = f"{base}][viewerAttrs][unifiedLink]"

        result["filename"] = info.get("filename")
        result["file_id"] = data.get(id_key)
        result["chat_id"] = data.get(chat_key)
        result["unified_link"] = data.get(unified_key)
        return result

    # 3. Самый запасной вариант: FILE_ID[0]
    file_id_key = data.get("data[PARAMS][FILE_ID][0]")
    if file_id_key:
        result["file_id"] = file_id_key
        result["filename"] = "document.pdf"

    return result


@app.route("/bot", methods=["POST", "GET"])
def bot_handler():
    if request.method == "GET":
        return jsonify({"result": "ok"})

    data = parse_request_data()
    print(f"PARSED DATA: {str(data)[:2000]}")

    event = data.get("event", "")

    params = data.get("data[PARAMS]") or data.get("data", {})
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            params = {}

    dialog_id = (
        data.get("data[PARAMS][DIALOG_ID]")
        or data.get("data[PARAMS][TO_CHAT_ID]")
        or params.get("DIALOG_ID")
        or params.get("TO_CHAT_ID")
    )

    message_text = str(
        data.get("data[PARAMS][MESSAGE]") or params.get("MESSAGE", "")
    ).strip().lower()

    print(f"EVENT: {event}, DIALOG: {dialog_id}")

    if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
        return jsonify({"result": "ok"})

    file_keys = {k: v for k, v in data.items() if "FILE" in k.upper()}
    print(f"ALL FILE KEYS: {str(file_keys)[:3000]}")

    file_info = find_pdf_in_payload(data)
    pdf_file_id = file_info["file_id"]
    pdf_url_direct = file_info["url_direct"]
    pdf_chat_id = file_info["chat_id"]
    pdf_unified_link = file_info["unified_link"]
    pdf_filename = file_info["filename"] or "document.pdf"

    print(
        f"FOUND PDF: filename={pdf_filename}, "
        f"file_id={pdf_file_id}, chat_id={pdf_chat_id}, "
        f"url_direct={str(pdf_url_direct)[:150]}, "
        f"unified_link={str(pdf_unified_link)[:150]}"
    )

    pdf_url = None

    # Главный приоритет — прямой urlDownload из payload
    if pdf_url_direct:
        pdf_url = pdf_url_direct
        print(f"USING DIRECT URL: {pdf_url[:200]}")

    # Только если его нет — строим ссылку через file_id
    elif pdf_file_id:
        pdf_url = get_file_download_url(
            file_id=pdf_file_id,
            chat_id=pdf_chat_id,
            unified_link=pdf_unified_link,
        )
        print(f"RESOLVED URL: {pdf_url[:200]}")

    if pdf_url:
        send_message(dialog_id, "📄 Получил выписку, обрабатываю... Это займет около 30 секунд.")

        try:
            print(f"FINAL PDF URL: {pdf_url[:300]}")
            pdf_bytes = download_file(pdf_url)

            send_message(dialog_id, "🔍 Анализирую выписку через ИИ...")

            transactions = extract_transactions(pdf_bytes)

            total_in = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "in")
            total_out = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "out")

            csv_bytes = to_csv(transactions)

            send_message(
                dialog_id,
                f"✅ Готово! Найдено {len(transactions)} транзакций.\n"
                f"📈 Поступления: {total_in:,.2f} ₽\n"
                f"📉 Списания: {total_out:,.2f} ₽"
            )
            send_file(dialog_id, "ДДС_выписка.csv", csv_bytes)

        except Exception as e:
            print(f"ERROR: {e}")
            send_message(dialog_id, f"❌ Ошибка: {str(e)}")

    elif message_text in ("привет", "start", "/start", "помощь", "help"):
        send_message(
            dialog_id,
            "👋 Привет! Пришли PDF-выписку из банка, и я разнесу транзакции по статьям ДДС и верну CSV."
        )
    else:
        send_message(dialog_id, "Пришли PDF-выписку из банка.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
