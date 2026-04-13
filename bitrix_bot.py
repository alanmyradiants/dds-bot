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

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()

# Входящий webhook №46
BITRIX_WEBHOOK_URL = "https://joto.bitrix24.ru/rest/1/ge7hgsje88e51nuw".rstrip("/")

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

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        parsed = parse_qs(raw)
        return {k: v[0] for k, v in parsed.items()}
    except Exception:
        return {}


def bitrix_post(method, payload, timeout=20):
    url = f"{BITRIX_WEBHOOK_URL}/{method}.json"
    r = requests.post(url, json=payload, timeout=timeout)
    print(f"{method} POST status={r.status_code}")
    print(f"{method} POST response={safe_preview(r.text, 3000)}")
    return r


def bitrix_get(method, params=None, timeout=20):
    url = f"{BITRIX_WEBHOOK_URL}/{method}.json"
    r = requests.get(url, params=params or {}, timeout=timeout)
    print(f"{method} GET status={r.status_code}")
    print(f"{method} GET response={safe_preview(r.text, 4000)}")
    return r


def send_message(dialog_id, text):
    if not dialog_id:
        print("send_message skipped: no dialog_id")
        return

    try:
        bitrix_post(
            "im.message.add",
            {"DIALOG_ID": dialog_id, "MESSAGE": text},
            timeout=15,
        )
    except Exception as e:
        print(f"send_message error: {e}")


def send_file(dialog_id, filename, content_bytes):
    """
    Отправка файла через im.disk.file.commit.
    Если Bitrix вернет ошибку, она появится в логах.
    """
    if not dialog_id:
        print("send_file skipped: no dialog_id")
        return

    try:
        encoded = base64.b64encode(content_bytes).decode()
        r = bitrix_post(
            "im.disk.file.commit",
            {
                "DIALOG_ID": dialog_id,
                "FILE_NAME": filename,
                "FILE_CONTENT": encoded,
            },
            timeout=60,
        )

        print("im.disk.file.commit RAW RESPONSE:", r.text)

        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")

        payload = r.json()
        if "error" in payload:
            raise ValueError(payload.get("error_description") or payload.get("error"))

        print("Файл успешно отправлен в Bitrix")

    except Exception as e:
        print(f"send_file error: {e}")
        raise


def find_pdf_in_payload(data):
    result = {
        "file_id": None,
        "chat_id": None,
        "url_download": None,
        "url_show": None,
        "unified_link": None,
        "filename": None,
    }

    for key, val in data.items():
        if "FILES" in key and key.endswith("][name]") and val:
            if str(val).lower().endswith(".pdf"):
                base = key[:-len("][name]")]
                result["filename"] = val
                result["file_id"] = data.get(f"{base}][id]")
                result["chat_id"] = data.get(f"{base}][chatId]")
                result["url_download"] = data.get(f"{base}][urlDownload]")
                result["url_show"] = data.get(f"{base}][urlShow]")
                result["unified_link"] = data.get(f"{base}][viewerAttrs][unifiedLink]")
                return result

    file_id = data.get("data[PARAMS][FILE_ID][0]") or data.get("data[PARAMS][PARAMS][FILE_ID][0]")
    if file_id:
        result["file_id"] = file_id
        result["filename"] = "document.pdf"

    return result


def get_disk_file_info(file_id):
    response = bitrix_get("disk.file.get", params={"id": file_id}, timeout=20)

    if response.status_code != 200:
        raise ValueError(f"disk.file.get вернул HTTP {response.status_code}")

    payload = response.json()

    if "error" in payload:
        raise ValueError(payload.get("error_description") or payload.get("error"))

    result = payload.get("result", {})
    if not result:
        raise ValueError("disk.file.get вернул пустой result")

    return result


def extract_download_url_from_disk_info(file_info):
    possible_keys = [
        "DOWNLOAD_URL",
        "downloadUrl",
        "DOWNLOAD_URL_MACHINE",
        "URL_DOWNLOAD",
        "urlDownload",
    ]

    for key in possible_keys:
        value = file_info.get(key)
        if value:
            return value

    for _, outer_val in file_info.items():
        if isinstance(outer_val, dict):
            for inner_key in possible_keys:
                value = outer_val.get(inner_key)
                if value:
                    return value

    return None


def download_pdf(url):
    try:
        print(f"DOWNLOAD START: {safe_preview(url, 300)}")

        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/pdf,*/*",
            },
            timeout=30,
            allow_redirects=True,
        )

        print(f"download_pdf status={response.status_code}")
        print(f"download_pdf final_url={safe_preview(response.url, 500)}")
        print(f"download_pdf content_type={response.headers.get('Content-Type')}")

        if response.status_code != 200:
            return None, f"Ошибка скачивания: HTTP {response.status_code}"

        content = response.content

        if not content:
            return None, "Пустой файл"

        print(f"download_pdf size={len(content)} bytes")
        print(f"download_pdf first_bytes={content[:20]}")

        content_type = (response.headers.get("Content-Type") or "").lower()

        if "pdf" in content_type:
            return content, None

        if content.startswith(b"%PDF"):
            return content, None

        if len(content) > 1000:
            return content, None

        try:
            preview = content[:500].decode("utf-8", errors="ignore").replace("\n", " ")
        except Exception:
            preview = str(content[:100])

        return None, f"Получили не PDF: {preview}"

    except requests.exceptions.Timeout:
        return None, "Таймаут при скачивании PDF"
    except requests.exceptions.ConnectionError:
        return None, "Ошибка соединения при скачивании PDF"
    except requests.exceptions.RequestException as e:
        return None, f"Ошибка requests: {str(e)}"
    except Exception as e:
        return None, f"Ошибка скачивания PDF: {str(e)}"


def get_pdf_bytes(file_id, fallback_url=None):
    disk_info = get_disk_file_info(file_id)
    print("===== DISK FILE INFO =====")
    print(safe_preview(disk_info, 5000))

    download_url = extract_download_url_from_disk_info(disk_info)

    if download_url:
        print(f"DOWNLOAD URL FROM DISK: {safe_preview(download_url, 1000)}")
        pdf_bytes, error = download_pdf(download_url)
        if error:
            raise ValueError(error)
        return pdf_bytes

    if fallback_url:
        print("DOWNLOAD URL from disk.file.get not found, trying fallback urlDownload")
        print(f"FALLBACK URL: {safe_preview(fallback_url, 1000)}")
        pdf_bytes, error = download_pdf(fallback_url)
        if error:
            raise ValueError(error)
        return pdf_bytes

    raise ValueError("Не найдена ссылка на скачивание PDF")


def extract_transactions(pdf_bytes):
    if not ANTHROPIC_API_KEY:
        raise ValueError("Не указан ANTHROPIC_API_KEY")

    try:
        print("Claude START")

        pdf_b64 = base64.b64encode(pdf_bytes).decode()

        msg = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=4000,
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
                        {
                            "type": "text",
                            "text": f"""Из банковской выписки извлеки транзакции и распредели по статьям ДДС.
Статьи: {', '.join(DDS_CATEGORIES)}
Верни ТОЛЬКО JSON массив:
[{{"date":"ДД.ММ.ГГГГ","description":"текст","amount":100.0,"type":"in","category":"статья","counterparty":"контрагент"}}]
type: in=поступление, out=списание. amount всегда положительное."""
                        },
                    ],
                }
            ],
        )

        print("Claude OK")

        text = msg.content[0].text
        print(f"Claude response preview: {safe_preview(text, 1500)}")

        start = text.find("[")
        end = text.rfind("]")

        if start == -1 or end == -1 or end < start:
            raise ValueError("Транзакции не найдены в ответе ИИ")

        json_str = text[start:end + 1]
        return json.loads(json_str)

    except Exception as e:
        print(f"Claude ERROR: {e}")
        raise ValueError(f"Ошибка Claude: {str(e)}")


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


@app.route("/bot", methods=["GET", "POST"])
def bot_handler():
    if request.method == "GET":
        return jsonify({"result": "ok"})

    dialog_id = None

    try:
        data = parse_request_data()

        print("===== WEBHOOK IN USE =====")
        print(BITRIX_WEBHOOK_URL)

        print("===== INCOMING REQUEST =====")
        print(safe_preview(data, 10000))

        event = data.get("event", "")
        print(f"EVENT: {event}")

        if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
            return jsonify({"result": "ok", "skipped": True})

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
            data.get("data[PARAMS][MESSAGE]")
            or params.get("MESSAGE", "")
        ).strip().lower()

        file_info = find_pdf_in_payload(data)
        print("===== FOUND FILE INFO =====")
        print(safe_preview(file_info, 4000))

        file_id = file_info.get("file_id")
        filename = file_info.get("filename") or "document.pdf"
        fallback_url = file_info.get("url_download")

        if filename.lower().endswith(".pdf") and file_id:
            send_message(dialog_id, "📄 Получил PDF, начинаю обработку...")

            pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url)

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

            send_file(dialog_id, "dds.csv", csv_bytes)

            send_message(dialog_id, "📎 CSV-файл отправлен.")

        elif message_text in ("привет", "start", "/start", "помощь", "help"):
            send_message(
                dialog_id,
                "👋 Привет! Пришли PDF-выписку из банка, и я разнесу транзакции по статьям ДДС и верну CSV."
            )
        else:
            send_message(dialog_id, "Пришли PDF-выписку из банка.")

        return jsonify({"result": "ok"})

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        if dialog_id:
            try:
                send_message(dialog_id, f"❌ Ошибка: {str(e)}")
            except Exception as send_err:
                print(f"failed to send error message: {send_err}")
        return jsonify({"result": "error", "message": str(e)}), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print("===== STARTING APP =====")
    print("BITRIX_WEBHOOK_URL =", BITRIX_WEBHOOK_URL)
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
