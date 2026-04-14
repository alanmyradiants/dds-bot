import os
import io
import json
import base64
import threading
import requests
from datetime import datetime
from urllib.parse import parse_qs
from flask import Flask, request, jsonify
import anthropic
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

ANTHROPIC_API_KEY       = os.getenv("ANTHROPIC_API_KEY", "").strip()
BITRIX_WEBHOOK_URL      = os.getenv("BITRIX_WEBHOOK_URL", "https://joto.bitrix24.ru/rest/1/ge7hgsje88e51nuw").rstrip("/")
BITRIX_DISK_WEBHOOK_URL = os.getenv("BITRIX_DISK_WEBHOOK_URL", "https://joto.bitrix24.ru/rest/1/g4s7w21uysosjds7").rstrip("/")
BOT_CLIENT_ID           = os.getenv("BOT_CLIENT_ID", "glhjxdm0jwb216zd3kdau2mwtf4z0fbu")
SHEET_ID                = "1i7a-UaUzzTJ5kVI5U18_fE6hkYb_i1uK0Fw_FTFhuNs"

DISK_TOKEN = BITRIX_DISK_WEBHOOK_URL.rstrip("/").split("/")[-1]

# ─────────────────────────────────────────────
# Google Sheets credentials (переиспользуем от ТаскБота)
# ─────────────────────────────────────────────
GOOGLE_CREDS_JSON = {
    "type": "service_account",
    "project_id": "joto-taskbot",
    "private_key_id": "7718e5c0c0210cfbed4265e512d983e37f0a2e93",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDH3Gq5Mcq4DcJ5\nv6nRiB5752w+TObS99J/R5/wqfVEcZG7EAe744UY8lVnl0SoqziYR+Dd/N3QMASh\n5BrXkw5VZMQjutCJ14pPx6qMdeASqMggdrTRY6bam4YgpNrUbjL0+LqOS/6Z3P+5\nMPcPapmalTqIAD4OZt4hZesLHpKvHAsDlMDpsJFHqOdSgkTjuZfkAVHoWCmWGaHK\n4WZPVaL86zmxgqGSr24XGlw6FEzZD0HJuSpkVFP/B/XSUdVa1fqn82GbTgU87Vpz\nEGZicpcLPAxpj8WJbLaIZf//5Ycd6NxYB4C0lVVkSDLCboY976y1j1nUcxp0t9EE\nWL4+L99hAgMBAAECgf9iu+m+o10fHRrHFScogMLNOcqYfxwQ6LCcmN/dlwk69dBm\ns2qB9LqNVINFDRJ66CXEOalK3MrMsIOjyjZJeEzwDIlrZnE2zCUaVEqjwH29n/ew\n4vPUPs8kqC2/auCwZNqI8WdNGSPC/pRmRqr16Es9ztkWVt6Fl69JlxUorQbEyLhG\ne6xZ7P147225ajveQtwloZJ9YlGVDDuzEHLQ20jXcxaeUlWIdWRzBignXVoouTcN\nHUC4IYEKFFm9J8VGShPtzfoIhDjTFusLU570laM2d5KT4kLwakxMhrZbyLDPtvBx\n/xHaPGU5qRIODKeocWXaFhCUAherBszsuatSFxUCgYEA9A/yq6791fE7hPy49hcn\ncK4Y9UvYy2SZ6Gk5v8Hb6wtsVAwzdwIV5Fvl5/If68qrHEdHH43sAkrfMlbg0uXn\nLhGfGgerPyZywuhwf6q8tjCPLfeyWKPIdrxo1jCo03cMluJ1xj40DjJLmdtqIgOZ\nGJVSUIGcdWDNxFUD2BPEN3cCgYEA0aL/bKWNsAnymouZjiIIBlPa4nW2iHsTCd6t\nbHcWXt6eOmaYtkgGK3t815pYhiAgGEAjBRoU/6W3ps8FWzraYXttIgX51cGd2tAm\nbKcu/3ZeXAUpmMFSwnYr3Mri6uBpWuf2w3wfgYrGAYoS8RwmF1mr4B1GzuDSAjVE\n/Yl3hecCgYAAqqZ8B49T7UO/Wj1bFrcZ3K/ew6VE8PJmqxroRixGmRJjrGDbm1rZ\n89JN7uBdcYFEI4GzOV0CqJexeIFGsjAOdSfdF1ZFZuJ7W80q3BmF2d4aPwnyqgfb\nIyqaIyni4flb1CSENRlJTKPeOLYyf5YEdivyYlg+DdSiC6VmCq/HgwKBgQCLb9Zj\nJq7ag5NZVjdZwasCwm3ZqSAzIWGlc/Z4KbG4gmxOPgWfYMKx015Tbfcpp16Ror9o\nWlPTQx+nlRVj+/5bTqRlOAJYOoNLkp2sMXtiMhJLNKfZUeVBMSa1okFSpteMvrN0\njS/Lk0lmprc4pldzupJG7FI3snQdQd9UoEXeywKBgQCSwDuVtZQ9XW5/IP0en6Cy\nj4A+cnHVecdkISHSkN9WQNqhrqFcGij2gn3+YQzJp0gF8962N9pSevqegumtsbbu\nrxyratoJlmAnESwWMPfe2MQePvv/YW9vFgw0Y7zhYv4E3CaVe4xWQZzFHv4gkkd+\nTQZx7sKSdtu3leIKWL1SEw==\n-----END PRIVATE KEY-----\n",
    "client_email": "taskbot@joto-taskbot.iam.gserviceaccount.com",
    "client_id": "109280707972066163791",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/taskbot%40joto-taskbot.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

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
    "Фотосессия",
    "Транспорт",
    "Питание / Кафе",
    "Супермаркеты",
    "Здоровье",
    "Развлечения",
    "Одежда",
    "Подписки личные",
    "Подписки бизнес",
    "Хостинг / IT",
    "Снятие наличных — уточнить",
    "Перевод — уточнить",
]

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"


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
# Google Sheets
# ─────────────────────────────────────────────

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_CREDS_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def ensure_sheet_headers(service):
    """Создаём заголовки если таблица пустая."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Транзакции!A1:H1"
        ).execute()
        if not result.get("values"):
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range="Транзакции!A1",
                valueInputOption="RAW",
                body={"values": [[
                    "Дата загрузки", "Дата операции", "Контрагент",
                    "Описание", "Приход", "Расход", "Статья ДДС", "Тип"
                ]]}
            ).execute()
    except Exception as e:
        print(f"ensure_sheet_headers error: {e}")


def write_to_sheets(transactions, period_label):
    """Записывает транзакции в Google Sheets."""
    try:
        service = get_sheets_service()
        ensure_sheet_headers(service)

        upload_date = datetime.now().strftime("%d.%m.%Y %H:%M")
        rows = []
        clarify_list = []

        for t in transactions:
            amount = float(t.get("amount", 0) or 0)
            inc = f"{amount:,.2f}".replace(".", ",") if t.get("type") == "in" else ""
            exp = f"{amount:,.2f}".replace(".", ",") if t.get("type") == "out" else ""
            category = t.get("category", "Прочие выплаты")
            t_type = "Поступление" if t.get("type") == "in" else "Списание"

            # Помечаем что нужно уточнить
            if "уточнить" in category.lower():
                clarify_list.append({
                    "date": t.get("date", ""),
                    "counterparty": t.get("counterparty", ""),
                    "amount": amount,
                    "type": t_type,
                    "description": t.get("description", ""),
                })

            rows.append([
                upload_date,
                t.get("date", ""),
                t.get("counterparty", ""),
                t.get("description", ""),
                inc,
                exp,
                category,
                t_type,
            ])

        # Добавляем строки в таблицу
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Транзакции!A:H",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()

        print(f"✅ Записано {len(rows)} строк в Google Sheets")
        return clarify_list

    except Exception as e:
        print(f"write_to_sheets ERROR: {e}")
        raise


def load_rules():
    """Загружает правила категоризации из вкладки Правила."""
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Правила!A:C"
        ).execute()
        rows = result.get("values", [])
        rules = {}
        for row in rows[1:]:  # пропускаем заголовок
            if len(row) >= 2:
                keyword = row[0].upper().strip()
                category = row[1].strip()
                rules[keyword] = category
        print(f"Загружено правил: {len(rules)}")
        return rules
    except Exception as e:
        print(f"load_rules error: {e}")
        return {}


def ensure_rules_sheet():
    """Создаём вкладку Правила если нет."""
    try:
        service = get_sheets_service()
        # Проверяем заголовок
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Правила!A1:C1"
        ).execute()
        if not result.get("values"):
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range="Правила!A1",
                valueInputOption="RAW",
                body={"values": [["Ключевое слово", "Категория", "Тип (Личное/Бизнес)"]]}
            ).execute()
            # Заполняем базовые правила
            initial_rules = [
                ["F-STORE1", "Фотосессия", "Бизнес"],
                ["FOTOSTUDIYA BASE", "Фотосессия", "Бизнес"],
                ["ФОТОСТУДИЯ BASE", "Фотосессия", "Бизнес"],
                ["TIMEWEB", "Хостинг / IT", "Бизнес"],
                ["RUSPROFILE", "Хостинг / IT", "Бизнес"],
                ["VANYAVPN", "Хостинг / IT", "Бизнес"],
                ["PODPISKA CHEREZ PRODAMUS", "Подписки бизнес", "Бизнес"],
                ["AIACADEMY", "Подписки бизнес", "Бизнес"],
                ["YANDEX*5815*PLUS", "Подписки личные", "Личное"],
                ["KOMETA.FIT", "Подписки личные", "Личное"],
                ["OTO*SMART GLOCAL", "Подписки личные", "Личное"],
                ["LITRES", "Подписки личные", "Личное"],
                ["GETCONTACT", "Подписки личные", "Личное"],
                ["YANDEX*4121*GO", "Транспорт", "Личное"],
                ["YANDEX*7299*GO", "Транспорт", "Личное"],
                ["YANDEX*7512*DRIVE", "Транспорт", "Личное"],
                ["CITYDRIVE", "Транспорт", "Личное"],
                ["BELKACAR", "Транспорт", "Личное"],
                ["IMP_BELKACAR", "Транспорт", "Личное"],
                ["YM*AMPP", "Транспорт", "Личное"],
                ["VYDRA", "Питание / Кафе", "Личное"],
                ["DUBROVKA", "Питание / Кафе", "Личное"],
                ["KAFE PIZZALINA", "Питание / Кафе", "Личное"],
                ["SPORT BAR MF", "Питание / Кафе", "Личное"],
                ["BURGER KING", "Питание / Кафе", "Личное"],
                ["SURF COFFEE", "Питание / Кафе", "Личное"],
                ["DODO PIZZA", "Питание / Кафе", "Личное"],
                ["VCAFE", "Питание / Кафе", "Личное"],
                ["XPLAT*EXPRESS VEND", "Питание / Кафе", "Личное"],
                ["FM MOSKVA", "Питание / Кафе", "Личное"],
                ["PYATEROCHKA", "Супермаркеты", "Личное"],
                ["ROSFERMA", "Супермаркеты", "Личное"],
                ["VV_9024", "Супермаркеты", "Личное"],
                ["DIXY", "Супермаркеты", "Личное"],
                ["SBERBANK ONL@IN VKLAD", "Внутренние переводы", ""],
                ["SBSCR_Wildberries", "Прочие выплаты", "Личное"],
                ["SP_SCHARIKOPODSCHIP", "Развлечения", "Личное"],
                ["PADL TAYM", "Развлечения", "Личное"],
                ["LITRES", "Развлечения", "Личное"],
                ["KOPIRKA", "Развлечения", "Личное"],
                ["ABDULLAEV", "Здоровье", "Личное"],
                ["GORZDRAV", "Здоровье", "Личное"],
                ["APTEKA", "Здоровье", "Личное"],
                ["ELIZE", "Здоровье", "Личное"],
                ["ATM", "Снятие наличных — уточнить", ""],
                ["Перевод для", "Перевод — уточнить", ""],
                ["Перевод от М. Алан", "Внутренние переводы", ""],
                ["T2 MOSCOW", "Коммунальные услуги", "Личное"],
                ["PAY.MTS", "Коммунальные услуги", "Личное"],
                ["Автоплатёж МТС", "Коммунальные услуги", "Личное"],
                ["AO KOMKOR", "Коммунальные услуги", "Бизнес"],
                ["SBERCHAEVYE", "Питание / Кафе", "Личное"],
            ]
            service.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range="Правила!A:C",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": initial_rules},
            ).execute()
            print(f"✅ Создана вкладка Правила с {len(initial_rules)} правилами")
    except Exception as e:
        print(f"ensure_rules_sheet error: {e}")


# ─────────────────────────────────────────────
# Bitrix API
# ─────────────────────────────────────────────

def bitrix_post(method_name, payload, timeout=20):
    url = f"{BITRIX_WEBHOOK_URL}/{method_name}.json"
    response = requests.post(url, json=payload, timeout=timeout)
    print(f"{method_name} POST status={response.status_code}")
    return response


def send_message(dialog_id, text):
    if not dialog_id:
        return
    try:
        bitrix_post(
            "imbot.message.add",
            {"DIALOG_ID": dialog_id, "MESSAGE": text, "CLIENT_ID": BOT_CLIENT_ID},
            timeout=15,
        )
    except Exception as e:
        print(f"send_message error: {e}")


# ─────────────────────────────────────────────
# Работа с PDF из Bitrix
# ─────────────────────────────────────────────

def find_pdf_in_payload(data):
    result = {
        "file_id": None, "chat_id": None,
        "url_download": None, "filename": None,
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
                result["url_download"] = get_field("][URLDOWNLOAD]", "][urlDownload]")
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
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/pdf,*/*"}
    if extra_headers:
        headers.update(extra_headers)
    resp = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
    content_type = (resp.headers.get("Content-Type") or "").lower()
    content_len = len(resp.content)
    print(f"try_download status={resp.status_code} ct={content_type} size={content_len}")
    if resp.status_code != 200:
        return None
    if "text/html" in content_type:
        return None
    if "application/pdf" in content_type or "application/octet-stream" in content_type or resp.content.startswith(b"%PDF"):
        return resp.content
    if content_len > 1024:
        return resp.content
    return None


def get_pdf_bytes(file_id, fallback_url=None):
    # Вариант 1: основной вебхук
    try:
        response = requests.get(f"{BITRIX_WEBHOOK_URL}/disk.file.get.json", params={"id": file_id}, timeout=20)
        print(f"disk.file.get via main webhook status={response.status_code}")
        if response.status_code == 200:
            payload = response.json()
            if payload.get("result"):
                dl = extract_download_url(payload["result"])
                if dl:
                    result = try_download(dl)
                    if result:
                        return result
    except Exception as e:
        print(f"main webhook failed: {e}")

    # Вариант 2: disk-вебхук
    try:
        response = requests.get(f"{BITRIX_DISK_WEBHOOK_URL}/disk.file.get.json", params={"id": file_id}, timeout=20)
        print(f"disk.file.get via disk webhook status={response.status_code}")
        if response.status_code == 200:
            payload = response.json()
            if payload.get("result"):
                dl = extract_download_url(payload["result"])
                if dl:
                    result = try_download(dl)
                    if result:
                        return result
    except Exception as e:
        print(f"disk webhook failed: {e}")

    # Вариант 3 & 4: fallback_url
    if fallback_url:
        for label, token in [("main", BITRIX_WEBHOOK_URL.rstrip("/").split("/")[-1]), ("disk", DISK_TOKEN)]:
            print(f"Trying fallback_url with {label} Bearer token")
            result = try_download(fallback_url, {"Authorization": f"Bearer {token}"})
            if result:
                return result

    raise ValueError("Не удалось скачать PDF. Проверьте логи.")


# ─────────────────────────────────────────────
# Claude AI — анализ выписки
# ─────────────────────────────────────────────

def extract_transactions(pdf_bytes, rules):
    if not ANTHROPIC_API_KEY:
        raise ValueError("Не указан ANTHROPIC_API_KEY")

    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    # Формируем правила для промпта
    rules_text = "\n".join([f"- {k} → {v}" for k, v in list(rules.items())[:50]])

    system_prompt = f"""Из банковской выписки извлеки транзакции и распредели по категориям.

Доступные категории:
{', '.join(DDS_CATEGORIES)}

Правила категоризации (применяй если контрагент совпадает):
{rules_text}

Дополнительные правила:
- Снятие наличных (ATM, банкомат) → "Снятие наличных — уточнить"
- Переводы физлицам (Перевод для Имя Фамилия) → "Перевод — уточнить"
- Переводы между своими счетами Сбербанк Вклад → "Внутренние переводы"
- Переводы от М. Алан Хазбиевич → "Внутренние переводы"

Верни ТОЛЬКО JSON массив без markdown:
[{{"date":"ДД.ММ.ГГГГ","description":"текст","amount":100.0,"type":"in","category":"категория","counterparty":"контрагент"}}]
type: in=поступление, out=списание. amount всегда положительное."""

    with client.messages.stream(
        model="claude-sonnet-4-6",
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

    print(f"Claude response: {safe_preview(text, 1000)}")

    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"Транзакции не найдены. Ответ: {safe_preview(text, 300)}")

    json_str = text[start:end + 1]
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        last_close = json_str.rfind("}")
        if last_close == -1:
            raise ValueError("Не удалось распарсить ответ ИИ")
        return json.loads(json_str[:last_close + 1] + "]")


# ─────────────────────────────────────────────
# Фоновая обработка PDF
# ─────────────────────────────────────────────

def process_pdf_async(dialog_id, file_id, fallback_url):
    try:
        # Скачиваем PDF
        pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url)
        send_message(dialog_id, "🔍 Анализирую выписку через ИИ...")

        # Загружаем правила из таблицы
        rules = load_rules()

        # Анализируем через Claude
        transactions = extract_transactions(pdf_bytes, rules)

        # Считаем итоги
        total_in  = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "in")
        total_out = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "out")

        # Записываем в Google Sheets
        send_message(dialog_id, "📊 Записываю в таблицу...")
        clarify_list = write_to_sheets(transactions, "")

        # Основное сообщение
        send_message(
            dialog_id,
            f"✅ Готово! Найдено {len(transactions)} транзакций.\n"
            f"📈 Поступления: {total_in:,.2f} ₽\n"
            f"📉 Списания: {total_out:,.2f} ₽\n\n"
            f"🔗 [url={SHEET_URL}]Открыть таблицу Расходы Сбер[/url]"
        )

        # Сообщение с уточнениями
        if clarify_list:
            clarify_text = "❓ Нужно уточнить категории:\n\n"
            for item in clarify_list[:15]:  # максимум 15
                clarify_text += f"• {item['date']} — {item['counterparty']} — {item['amount']:,.0f} ₽ ({item['type']})\n"
            if len(clarify_list) > 15:
                clarify_text += f"...и ещё {len(clarify_list) - 15} операций\n"
            clarify_text += "\nОтветь на это сообщение чтобы уточнить категории."
            send_message(dialog_id, clarify_text)

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
    print("===== INCOMING REQUEST =====")
    print(safe_preview(data, 5000))

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
    print(f"FILE INFO: {safe_preview(file_info, 500)}")

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
            "я разнесу транзакции по категориям и запишу в таблицу [url=" + SHEET_URL + "]Расходы Сбер[/url].",
        )
    else:
        send_message(dialog_id, "Пришли PDF-выписку из банка.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/init-sheets", methods=["GET"])
def init_sheets():
    """Инициализация структуры таблицы."""
    try:
        ensure_rules_sheet()
        return jsonify({"ok": True, "message": "Таблица инициализирована"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    print("===== STARTING APP =====")
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
