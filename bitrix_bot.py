"""
Битрикс24 бот: PDF выписка → CSV для ДДС
==========================================
Установка:
  pip install flask requests anthropic

Запуск:
  python bot.py

Для продакшена нужен публичный URL (Railway, Render, VPS).
"""

import os
import io
import csv
import json
import tempfile
import requests
from flask import Flask, request, jsonify
import anthropic

app = Flask(__name__)

# === НАСТРОЙКИ (заполните своими данными) ===
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "sk-ant-ВАШИ_КЛЮЧ")
BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL", "https://ВАШИ_ПОРТАЛ.bitrix24.ru/rest/1/ВАШИ_ТОКЕН")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # токен бота из Битрикс

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


def send_message(dialog_id, text):
    """Отправить текстовое сообщение в чат Битрикс"""
    requests.post(f"{BITRIX_WEBHOOK_URL}/im.message.add.json", json={
        "DIALOG_ID": dialog_id,
        "MESSAGE": text,
    })


def send_file(dialog_id, filename, content_bytes):
    """Отправить файл в чат Битрикс через disk.file.uploadbyurl или прямую загрузку"""
    # Загружаем файл через im.disk.file.commit
    import base64
    encoded = base64.b64encode(content_bytes).decode()
    requests.post(f"{BITRIX_WEBHOOK_URL}/im.disk.file.commit.json", json={
        "DIALOG_ID": dialog_id,
        "FILE_NAME": filename,
        "FILE_CONTENT": encoded,
    })


def download_file(url):
    """Скачать файл из Битрикс (с авторизацией через webhook)"""
    # Добавляем токен если нужно
    response = requests.get(url, timeout=30)
    return response.content


def extract_transactions_from_pdf(pdf_bytes):
    """Отправить PDF в Claude API и получить транзакции"""
    import base64
    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    system_prompt = f"""Ты — финансовый аналитик. Из банковской выписки Сбербанка извлеки все транзакции и распредели их по статьям ДДС.

Доступные статьи ДДС:
{chr(10).join(DDS_CATEGORIES)}

Верни ТОЛЬКО валидный JSON массив (без markdown, без текста):
[
  {{
    "date": "ДД.ММ.ГГГГ",
    "description": "Краткое описание",
    "amount": 12345.67,
    "type": "in",
    "category": "Одна из статей ДДС выше",
    "counterparty": "Название контрагента"
  }}
]

Правила:
- amount всегда положительное число
- type: "in" для поступлений, "out" для списаний
- Игнорируй строки с балансом/остатком, только движения"""

    message = client.messages.create(
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
                    {"type": "text", "text": "Извлеки все транзакции и распредели по статьям ДДС."},
                ],
            }
        ],
    )

    text = message.content[0].text
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        raise ValueError("Не удалось найти транзакции в ответе")

    return json.loads(text[start:end + 1])


def transactions_to_csv(transactions):
    """Преобразовать список транзакций в CSV байты"""
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";", quoting=csv.QUOTE_ALL)

    writer.writerow(["Дата", "Контрагент", "Описание", "Приход", "Расход", "Статья ДДС"])

    for t in transactions:
        inc = f"{t['amount']:.2f}" if t.get("type") == "in" else ""
        exp = f"{t['amount']:.2f}" if t.get("type") == "out" else ""
        writer.writerow([
            t.get("date", ""),
            t.get("counterparty", ""),
            t.get("description", ""),
            inc,
            exp,
            t.get("category", ""),
        ])

    # UTF-8 с BOM для корректного открытия в Excel
    return ("\ufeff" + output.getvalue()).encode("utf-8")


@app.route("/bot", methods=["POST"])
def bot_handler():
    """Основной обработчик событий от Битрикс24"""
    data = request.json or request.form.to_dict()

    event = data.get("event", "")
    event_data = data.get("data", {})

    # Обрабатываем только входящие сообщения
    if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
        return jsonify({"result": "ok"})

    dialog_id = event_data.get("DIALOG_ID") or event_data.get("FROM_USER_ID")
    message_text = event_data.get("MESSAGE", "").lower()
    attach = event_data.get("ATTACH", [])
    files = event_data.get("FILES", [])

    # Ищем PDF вложение
    pdf_url = None
    pdf_name = "выписка.pdf"

    for f in (files or []):
        name = f.get("name", "").lower()
        if name.endswith(".pdf"):
            pdf_url = f.get("urlDownload") or f.get("link")
            pdf_name = f.get("name", pdf_name)
            break

    # Если прислали PDF — обрабатываем
    if pdf_url:
        send_message(dialog_id, "📄 Получил выписку, обрабатываю... Это займёт ~30 секунд.")
        try:
            pdf_bytes = download_file(pdf_url)
            send_message(dialog_id, "🔍 Анализирую транзакции через ИИ...")

            transactions = extract_transactions_from_pdf(pdf_bytes)

            total_in = sum(t["amount"] for t in transactions if t.get("type") == "in")
            total_out = sum(t["amount"] for t in transactions if t.get("type") == "out")

            csv_bytes = transactions_to_csv(transactions)

            # Отправляем статистику
            send_message(dialog_id,
                f"✅ Готово! Обработано {len(transactions)} транзакций\n"
                f"📈 Поступления: {total_in:,.2f} ₽\n"
                f"📉 Списания: {total_out:,.2f} ₽\n"
                f"📎 Отправляю CSV файл..."
            )

            # Отправляем CSV файл
            send_file(dialog_id, "ДДС_выписка.csv", csv_bytes)

        except Exception as e:
            send_message(dialog_id, f"❌ Ошибка при обработке: {str(e)}\nПопробуйте ещё раз или пришлите другой файл.")

    elif message_text in ("привет", "start", "/start", "помощь", "help"):
        send_message(dialog_id,
            "👋 Привет! Я бот для обработки банковских выписок.\n\n"
            "📌 Как использовать:\n"
            "Просто пришлите PDF-выписку из Сбербанка — я автоматически распределю все транзакции по статьям ДДС и верну вам готовый CSV файл для 1С.\n\n"
            "📂 Поддерживается: выписки Сбербанк в формате PDF"
        )
    else:
        send_message(dialog_id, "Пришлите PDF-выписку из Сбербанка, и я подготовлю CSV для ДДС.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
