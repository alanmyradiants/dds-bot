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


def get_file_url_by_id(file_id):
    """Строим URL напрямую через show_file.php"""
    parts = BITRIX_WEBHOOK_URL.rstrip("/").split("/")
    token = parts[-1]
    domain = "/".join(parts[:3])
    return f"{domain}/bitrix/components/bitrix/im.messenger/download.file.php?fileId={file_id}&auth={token}"


def download_file(url):
    parts = BITRIX_WEBHOOK_URL.rstrip("/").split("/")
    token = parts[-1]
    
    # Пробуем с токеном в заголовке
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        print(f"Download status={r.status_code} size={len(r.content)} first_bytes={r.content[:8]}")
        if r.status_code == 200 and r.content[:4] == b"%PDF":
            return r.content
    except Exception as e:
        print(f"Download error: {e}")

    # Пробуем с токеном в URL параметре
    sep = "&" if "?" in url else "?"
    url_with_auth = f"{url}{sep}auth={token}"
    try:
        r = requests.get(url_with_auth, timeout=60, allow_redirects=True)
        print(f"Download2 status={r.status_code} size={len(r.content)} first_bytes={r.content[:8]}")
        if r.status_code == 200 and r.content[:4] == b"%PDF":
            return r.content
    except Exception as e:
        print(f"Download2 error: {e}")

    # Пробуем через unifiedLink из данных Битрикс
    raise ValueError(f"Не удалось скачать PDF. Получили: {r.content[:50]}")


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

    # Логируем все ключи связанные с файлами
    file_keys = {k: v for k, v in data.items() if "FILE" in k.upper()}
    print(f"ALL FILE KEYS: {file_keys}")

    pdf_file_id = None
    pdf_url_direct = None

    # Ищем прямой URL скачивания
    for key, val in data.items():
        if "FILES" in key and key.endswith("][urlDownload]") and val:
            name_key = key.replace("][urlDownload]", "][name]")
            fname = data.get(name_key, ".pdf")
            if fname.lower().endswith(".pdf"):
                pdf_url_direct = val
                break

    # Также пробуем unifiedLink
    if not pdf_url_direct:
        for key, val in data.items():
            if "FILES" in key and key.endswith("][unifiedLink]") and val:
                name_key = key.replace("][unifiedLink]", "][name]")
                fname = data.get(name_key, ".pdf")
                if fname.lower().endswith(".pdf"):
                    pdf_url_direct = val
                    break

    # Если urlDownload не нашли — берём FILE_ID
    if not pdf_url_direct:
        file_id_key = data.get("data[PARAMS][FILE_ID][0]")
        if file_id_key:
            pdf_file_id = file_id_key

        # Вариант 2: data[PARAMS][FILES][ID][id]
        if not pdf_file_id:
            for key, val in data.items():
                if "FILES" in key and key.endswith("][id]") and val:
                    pdf_file_id = val
                    break

    # Получаем URL файла
    pdf_url = None
    if pdf_url_direct:
        parts = BITRIX_WEBHOOK_URL.rstrip("/").split("/")
        token = parts[-1]
        domain = "/".join(parts[:3])
        # Если URL относительный — добавляем домен
        if pdf_url_direct.startswith("/"):
            pdf_url = domain + pdf_url_direct + f"&auth={token}"
        else:
            pdf_url = pdf_url_direct + f"&auth={token}"
        print(f"DIRECT URL: {pdf_url[:100]}")
    elif pdf_file_id:
        pdf_url = get_file_url_by_id(pdf_file_id)
        print(f"FILE_ID: {pdf_file_id}, URL: {pdf_url}")

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
