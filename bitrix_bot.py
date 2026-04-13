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
    text = str(value)
    return text[:limit] + " ...[truncated]" if len(text) > limit else text


def parse_request_data():
    if request.is_json:
        return request.get_json(force=True) or {}

    if request.form:
        return request.form.to_dict()

    raw = request.get_data(as_text=True)

    try:
        return json.loads(raw)
    except:
        pass

    try:
        parsed = parse_qs(raw)
        return {k: v[0] for k, v in parsed.items()}
    except:
        return {}


def bitrix_post(method, payload):
    url = f"{BITRIX_WEBHOOK_URL}/{method}.json"
    r = requests.post(url, json=payload, timeout=15)
    print(f"{method} → {r.status_code}")
    return r


def send_message(dialog_id, text):
    if dialog_id:
        bitrix_post("im.message.add", {
            "DIALOG_ID": dialog_id,
            "MESSAGE": text
        })


def send_file(dialog_id, filename, content):
    if dialog_id:
        encoded = base64.b64encode(content).decode()
        bitrix_post("im.disk.file.commit", {
            "DIALOG_ID": dialog_id,
            "FILE_NAME": filename,
            "FILE_CONTENT": encoded
        })


def find_pdf(data):
    for k, v in data.items():
        if "FILES" in k and k.endswith("][name]") and str(v).endswith(".pdf"):
            base = k[:-len("][name]")]
            return {
                "file_id": data.get(f"{base}][id]"),
                "url": data.get(f"{base}][urlDownload]")
            }
    return {"file_id": None, "url": None}


def download_pdf(url):
    r = requests.get(url, timeout=30)

    if r.status_code != 200:
        raise ValueError("Ошибка скачивания")

    if not r.content.startswith(b"%PDF"):
        raise ValueError("Не PDF")

    return r.content


def extract_transactions(pdf_bytes):
    if not ANTHROPIC_API_KEY:
        raise ValueError("Нет ANTHROPIC_API_KEY")

    print("Claude START")

    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    msg = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=4000,
        timeout=30,
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
                        "text": f"""Извлеки транзакции в JSON.
Статьи: {', '.join(DDS_CATEGORIES)}
Формат:
[{{"date":"ДД.ММ.ГГГГ","description":"текст","amount":100.0,"type":"in","category":"статья","counterparty":"контрагент"}}]"""
                    }
                ],
            }
        ],
    )

    print("Claude OK")

    text = msg.content[0].text

    start = text.find("[")
    end = text.rfind("]")

    if start == -1:
        raise ValueError("JSON не найден")

    return json.loads(text[start:end + 1])


def to_csv(transactions):
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", quoting=csv.QUOTE_ALL)

    writer.writerow(["Дата", "Контрагент", "Описание", "Приход", "Расход", "Статья"])

    for t in transactions:
        amount = float(t.get("amount", 0))
        writer.writerow([
            t.get("date", ""),
            t.get("counterparty", ""),
            t.get("description", ""),
            amount if t.get("type") == "in" else "",
            amount if t.get("type") == "out" else "",
            t.get("category", "")
        ])

    return ("\ufeff" + out.getvalue()).encode()


@app.route("/bot", methods=["POST"])
def bot():
    data = parse_request_data()

    print("INCOMING:", safe_preview(data, 2000))

    dialog_id = data.get("data[PARAMS][DIALOG_ID]") or data.get("data[PARAMS][TO_CHAT_ID]")

    file = find_pdf(data)

    if file["file_id"]:
        try:
            send_message(dialog_id, "📄 Получил файл")

            pdf = download_pdf(file["url"])

            send_message(dialog_id, "🔍 Анализ...")

            transactions = extract_transactions(pdf)

            csv_file = to_csv(transactions)

            send_message(dialog_id, f"✅ Готово: {len(transactions)} операций")

            send_file(dialog_id, "dds.csv", csv_file)

        except Exception as e:
            send_message(dialog_id, f"❌ Ошибка: {str(e)}")

    else:
        send_message(dialog_id, "Пришли PDF")

    return jsonify({"ok": True})


@app.route("/health")
def health():
    return "ok"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
