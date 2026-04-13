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
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

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
    """Парсим данные запроса в любом формате"""
    # Пробуем JSON
    try:
        if request.is_json:
            return request.get_json(force=True) or {}
    except:
        pass
    # Пробуем form data
    if request.form:
        return request.form.to_dict()
    # Пробуем raw body
    raw = request.get_data(as_text=True)
    if raw:
        try:
            return json.loads(raw)
        except:
            pass
        try:
            parsed = parse_qs(raw)
            return {k: v[0] for k, v in parsed.items()}
        except:
            pass
    return {}


def send_message(dialog_id, text):
    try:
        requests.post(
            f"{BITRIX_WEBHOOK_URL}/im.message.add.json",
            json={"DIALOG_ID": dialog_id, "MESSAGE": text},
            timeout=10
        )
    except Exception as e:
        print(f"send_message error: {e}")


def send_file(dialog_id, filename, content_bytes):
    try:
        encoded = base64.b64encode(content_bytes).decode()
        requests.post(
            f"{BITRIX_WEBHOOK_URL}/im.disk.file.commit.json",
            json={"DIALOG_ID": dialog_id, "FILE_NAME": filename, "FILE_CONTENT": encoded},
            timeout=30
        )
    except Exception as e:
        print(f"send_file error: {e}")


def download_file(url):
    # Добавляем авторизацию для скачивания из Битрикс
    if "bitrix24.ru" in url:
        # Извлекаем токен из BITRIX_WEBHOOK_URL
        token = BITRIX_WEBHOOK_URL.rstrip("/").split("/")[-1]
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}auth={token}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def extract_transactions(pdf_bytes):
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
        messages=[{"role": "user", "content": [
            {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": pdf_b64}},
            {"type": "text", "text": "Извлеки все транзакции."}
        ]}]
    )
    text = msg.content[0].text
    start, end = text.find("["), text.rfind("]")
    if start == -1: raise ValueError("Транзакции не найдены")
    return json.loads(text[start:end+1])


def to_csv(transactions):
    out = io.StringIO()
    w = csv.writer(out, delimiter=";", quoting=csv.QUOTE_ALL)
    w.writerow(["Дата", "Контрагент", "Описание", "Приход", "Расход", "Статья ДДС"])
    for t in transactions:
        inc = f"{t['amount']:.2f}" if t.get("type") == "in" else ""
        exp = f"{t['amount']:.2f}" if t.get("type") == "out" else ""
        w.writerow([t.get("date",""), t.get("counterparty",""), t.get("description",""), inc, exp, t.get("category","")])
    return ("\ufeff" + out.getvalue()).encode("utf-8")


@app.route("/bot", methods=["POST", "GET"])
def bot_handler():
    if request.method == "GET":
        return jsonify({"result": "ok"})

    data = parse_request_data()
    print(f"PARSED DATA: {str(data)[:600]}")

    event = data.get("event", "")

    # Битрикс присылает данные в data[PARAMS]
    params = data.get("data[PARAMS]") or data.get("data", {})
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except:
            params = {}

    # Достаём dialog_id
    dialog_id = (
        data.get("data[PARAMS][DIALOG_ID]") or
        data.get("data[PARAMS][TO_CHAT_ID]") or
        params.get("DIALOG_ID") or
        params.get("TO_CHAT_ID")
    )

    message_text = str(
        data.get("data[PARAMS][MESSAGE]") or
        params.get("MESSAGE", "")
    ).lower()

    print(f"EVENT: {event}, DIALOG: {dialog_id}")

    if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
        return jsonify({"result": "ok"})

    # Ищем PDF файл в данных от Битрикс
    # Структура: data[PARAMS][FILES][ID][urlShow], data[PARAMS][FILES][ID][name]
    pdf_url = None
    file_ids = set()
    for key in data.keys():
        if "FILES" in key and key.endswith("][id]"):
            parts = key.split("[")
            # Берём ID файла
            for i, p in enumerate(parts):
                if p.startswith("FILES"):
                    if i+1 < len(parts):
                        fid = parts[i+1].rstrip("]")
                        file_ids.add(fid)

    for fid in file_ids:
        name_key = f"data[PARAMS][FILES][{fid}][name]"
        url_key = f"data[PARAMS][FILES][{fid}][urlDownload]"
        url_key2 = f"data[PARAMS][FILES][{fid}][urlShow]"
        fname = data.get(name_key, "")
        if fname.lower().endswith(".pdf"):
            pdf_url = data.get(url_key) or data.get(url_key2)
            break
    
    # Запасной вариант — любой URL с FILES в ключе
    if not pdf_url:
        for key, val in data.items():
            if "FILES" in key and ("urlDownload" in key or "urlShow" in key) and val:
                pdf_url = val
                break

    if pdf_url:
        send_message(dialog_id, "📄 Получил выписку, обрабатываю... ~30 секунд.")
        try:
            pdf_bytes = download_file(pdf_url)
            send_message(dialog_id, "🔍 Анализирую через ИИ...")
            transactions = extract_transactions(pdf_bytes)
            total_in = sum(t["amount"] for t in transactions if t.get("type") == "in")
            total_out = sum(t["amount"] for t in transactions if t.get("type") == "out")
            csv_bytes = to_csv(transactions)
            send_message(dialog_id,
                f"✅ Готово! {len(transactions)} транзакций\n"
                f"📈 Поступления: {total_in:,.2f} ₽\n"
                f"📉 Списания: {total_out:,.2f} ₽"
            )
            send_file(dialog_id, "ДДС_выписка.csv", csv_bytes)
        except Exception as e:
            print(f"ERROR: {e}")
            send_message(dialog_id, f"❌ Ошибка: {str(e)}")
    elif message_text in ("привет", "start", "/start", "помощь", "help", ""):
        send_message(dialog_id,
            "👋 Привет! Пришлите PDF-выписку из Сбербанка — "
            "я распределю транзакции по статьям ДДС и верну CSV для 1С."
        )
    else:
        send_message(dialog_id, "Пришлите PDF-выписку из Сбербанка.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
