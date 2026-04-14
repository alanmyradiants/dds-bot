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
SHEET_URL  = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"

# ─────────────────────────────────────────────
# Google Sheets credentials
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

# ─────────────────────────────────────────────
# Категории и правила
# ─────────────────────────────────────────────

DDS_CATEGORIES = [
    "Поступления от покупателей",
    "Заработная плата",
    "Заработная плата (ПВЗ)",
    "Налоги и взносы",
    "Аренда",
    "Коммунальные услуги",
    "Банковские комиссии",
    "Кредиты и займы (получение)",
    "Кредиты и займы (погашение)",
    "Дивиденды",
    "Фотосессия",
    "Автоматизация",
    "Доставка",
    "Транспорт",
    "Питание / Кафе",
    "Супермаркеты",
    "Здоровье",
    "Развлечения",
    "Одежда",
    "Подписки личные",
    "Выдача наличных",
    "Внутренние переводы",
    "Личные переводы",
    "Долг",
    "Прочие поступления",
    "Прочие выплаты",
    "❓ Уточнить",
]

# Встроенные правила (дополняются из вкладки "Правила")
BUILTIN_RULES = {
    # Фотосессия (Бизнес)
    "F-STORE1": ("Фотосессия", "Бизнес"),
    "FOTOSTUDIYA BASE": ("Фотосессия", "Бизнес"),
    "ФОТОСТУДИЯ BASE": ("Фотосессия", "Бизнес"),
    "BASEPHOTOSTUDIO": ("Фотосессия", "Бизнес"),

    # Автоматизация (Бизнес)
    "TIMEWEB": ("Автоматизация", "Бизнес"),
    "RUSPROFILE": ("Автоматизация", "Бизнес"),
    "VANYAVPN": ("Автоматизация", "Бизнес"),
    "PRODAMUS": ("Автоматизация", "Бизнес"),
    "AIACADEMY": ("Автоматизация", "Бизнес"),
    "ANTHROPIC": ("Автоматизация", "Бизнес"),

    # Зарплата (Бизнес)
    "ДАРИЯ РУСЛАНОВНА": ("Заработная плата", "Бизнес"),
    "МНАЦАКАНЯН": ("Заработная плата", "Бизнес"),
    "СОСЛАНОВНА": ("Заработная плата (ПВЗ)", "Бизнес"),
    "ВАЛЕРЬЯНОВНА": ("Заработная плата (ПВЗ)", "Бизнес"),

    # Долг (Бизнес)
    "ТУРПАЛ-АЛИ": ("Долг", "Бизнес"),
    "САЙХАНОВИЧ": ("Долг", "Бизнес"),

    # Личные переводы
    "ВЯЧЕСЛАВОВНА": ("Личные переводы", "Личное"),
    "EVGENII": ("Личные переводы", "Личное"),
    "БАРАКАТУЛЛОИ": ("Личные переводы", "Личное"),
    "ХАЙРУЛЛО": ("Личные переводы", "Личное"),
    "ОКСАНА АЛЕКСЕЕВНА": ("Личные переводы", "Личное"),
    "ИНАЛ АСЛАНБЕКОВИЧ": ("Личные переводы", "Личное"),
    "ЕЛИЗАВЕТА МИХАЙЛОВНА": ("Личные переводы", "Личное"),
    "СТАНИСЛАВ ВЯЧЕСЛАВОВИЧ": ("Личные переводы", "Личное"),

    # Выдача наличных
    "ATM": ("Выдача наличных", "Личное"),

    # Внутренние переводы
    "SBERBANK ONL@IN VKLAD": ("Внутренние переводы", ""),
    "АЛАН ХАЗБИЕВИЧ": ("Внутренние переводы", ""),
    "T-БАНК": ("Внутренние переводы", ""),

    # Транспорт (Личное)
    "YANDEX*4121*GO": ("Транспорт", "Личное"),
    "YANDEX*7299*GO": ("Транспорт", "Личное"),
    "YANDEX*7512*DRIVE": ("Транспорт", "Личное"),
    "CITYDRIVE": ("Транспорт", "Личное"),
    "BELKACAR": ("Транспорт", "Личное"),
    "IMP_BELKACAR": ("Транспорт", "Личное"),
    "YM*AMPP": ("Транспорт", "Личное"),
    "YANDEX*4121*TAXI": ("Транспорт", "Личное"),

    # Питание / Кафе (Личное)
    "VYDRA": ("Питание / Кафе", "Личное"),
    "DUBROVKA": ("Питание / Кафе", "Личное"),
    "KAFE PIZZALINA": ("Питание / Кафе", "Личное"),
    "SPORT BAR MF": ("Питание / Кафе", "Личное"),
    "BURGER KING": ("Питание / Кафе", "Личное"),
    "SURF COFFEE": ("Питание / Кафе", "Личное"),
    "DODO PIZZA": ("Питание / Кафе", "Личное"),
    "VCAFE": ("Питание / Кафе", "Личное"),
    "XPLAT*EXPRESS VEND": ("Питание / Кафе", "Личное"),
    "FM MOSKVA": ("Питание / Кафе", "Личное"),
    "SBERCHAEVYE": ("Питание / Кафе", "Личное"),
    "BARVIKHA": ("Питание / Кафе", "Личное"),
    "REST SOVHOZNAYA": ("Питание / Кафе", "Личное"),
    "VETNAMSKOE": ("Питание / Кафе", "Личное"),

    # Супермаркеты (Личное)
    "PYATEROCHKA": ("Супермаркеты", "Личное"),
    "ROSFERMA": ("Супермаркеты", "Личное"),
    "VV_9024": ("Супермаркеты", "Личное"),
    "DIXY": ("Супермаркеты", "Личное"),

    # Здоровье (Личное)
    "ABDULLAEV": ("Здоровье", "Личное"),
    "GORZDRAV": ("Здоровье", "Личное"),
    "APTEKA": ("Здоровье", "Личное"),
    "ELIZE": ("Здоровье", "Личное"),
    "APTEKA ZDOROV": ("Здоровье", "Личное"),

    # Развлечения (Личное)
    "SP_SCHARIKOPODSCHIP": ("Развлечения", "Личное"),
    "PADL TAYM": ("Развлечения", "Личное"),
    "RUSPADEL": ("Развлечения", "Личное"),
    "KOPIRKA": ("Развлечения", "Личное"),
    "KOMETA.FIT": ("Подписки личные", "Личное"),
    "LITRES": ("Развлечения", "Личное"),
    "KAFE LUNDA": ("Развлечения", "Личное"),

    # Подписки личные
    "YANDEX*5815*PLUS": ("Подписки личные", "Личное"),
    "OTO*SMART GLOCAL": ("Подписки личные", "Личное"),
    "GETCONTACT": ("Подписки личные", "Личное"),
    "W1*GETCONTACT": ("Подписки личные", "Личное"),

    # Одежда (Личное)
    "KOTON": ("Одежда", "Личное"),
    "NAIPACHE": ("Одежда", "Личное"),

    # Коммунальные услуги
    "AO KOMKOR": ("Коммунальные услуги", "Бизнес"),
    "KOMKOR": ("Коммунальные услуги", "Бизнес"),
    "T2 MOSCOW": ("Коммунальные услуги", "Личное"),
    "PAY.MTS": ("Коммунальные услуги", "Личное"),
    "АВТОПЛАТЁЖ МТС": ("Коммунальные услуги", "Личное"),
    "MTS": ("Коммунальные услуги", "Личное"),
    "BERI ZARYAD": ("Коммунальные услуги", "Личное"),

    # Прочие
    "WILDBERRIES": ("Прочие выплаты", "Личное"),
    "SBSCR_WILDBERRIES": ("Прочие выплаты", "Личное"),
    "ЮМАНИ": ("Прочие выплаты", "Личное"),
    "YMANI": ("Прочие выплаты", "Личное"),
    "YM*IPEYE": ("Прочие выплаты", "Личное"),
    "DNS": ("Прочие выплаты", "Личное"),
    "F-STORE": ("Фотосессия", "Бизнес"),
    "NETMONET": ("Прочие выплаты", "Личное"),
    "HIPPOPARKING": ("Транспорт", "Личное"),
    "MAPP_SBERBANK": ("Прочие выплаты", ""),
    "LPMOTOR": ("Прочие выплаты", "Личное"),
}

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


def apply_rules(counterparty, description, amount, t_type):
    """Применяет правила категоризации."""
    text = f"{counterparty} {description}".upper()

    for keyword, (category, biz_type) in BUILTIN_RULES.items():
        if keyword.upper() in text:
            return category, biz_type

    # Переводы физлицам — уточнять
    if t_type == "out" and amount >= 1000:
        if any(w in text for w in ["ПЕРЕВОД ДЛЯ", "ПЕРЕВОД ОТ"]):
            if "АЛАН ХАЗБИЕВИЧ" not in text and "VKLAD" not in text:
                return "❓ Уточнить", ""

    return "Прочие выплаты", "Личное"


# ─────────────────────────────────────────────
# Google Sheets
# ─────────────────────────────────────────────

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_info(
        GOOGLE_CREDS_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def init_sheets():
    """Инициализация структуры таблицы."""
    service = get_sheets_service()

    # Получаем список существующих листов
    spreadsheet = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    existing_sheets = [s["properties"]["title"] for s in spreadsheet["sheets"]]

    requests_body = []

    # Создаём листы если не существуют
    for sheet_name in ["Транзакции", "Правила"]:
        if sheet_name not in existing_sheets:
            requests_body.append({
                "addSheet": {"properties": {"title": sheet_name}}
            })

    if requests_body:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": requests_body}
        ).execute()

    # Заголовки Транзакции
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Транзакции!A1"
    ).execute()
    if not result.get("values"):
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range="Транзакции!A1",
            valueInputOption="RAW",
            body={"values": [["Дата загрузки", "Дата операции", "Контрагент",
                              "Описание", "Приход", "Расход", "Категория",
                              "Личное/Бизнес", "Статус"]]}
        ).execute()

    # Заполняем правила
    rules_rows = [["Ключевое слово", "Категория", "Личное/Бизнес"]]
    for keyword, (category, biz_type) in BUILTIN_RULES.items():
        rules_rows.append([keyword, category, biz_type])

    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="Правила!A1",
        valueInputOption="RAW",
        body={"values": rules_rows}
    ).execute()

    print(f"✅ Таблица инициализирована, правил: {len(BUILTIN_RULES)}")


def write_to_sheets(transactions):
    """Записывает транзакции в Google Sheets."""
    service = get_sheets_service()
    upload_date = datetime.now().strftime("%d.%m.%Y %H:%M")
    rows = []
    clarify_list = []

    for t in transactions:
        amount = float(t.get("amount", 0) or 0)
        t_type = t.get("type", "out")
        counterparty = t.get("counterparty", "")
        description = t.get("description", "")

        # Применяем правила
        category, biz_type = apply_rules(counterparty, description, amount, t_type)

        inc = f"{amount:,.2f}".replace(".", ",") if t_type == "in" else ""
        exp = f"{amount:,.2f}".replace(".", ",") if t_type == "out" else ""
        status = "❓ Уточнить" if category == "❓ Уточнить" else "✅"

        if status == "❓ Уточнить":
            clarify_list.append({
                "date": t.get("date", ""),
                "counterparty": counterparty,
                "amount": amount,
                "type": "Поступление" if t_type == "in" else "Списание",
                "description": description,
            })

        rows.append([
            upload_date,
            t.get("date", ""),
            counterparty,
            description,
            inc,
            exp,
            category,
            biz_type,
            status,
        ])

    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="Транзакции!A:I",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

    print(f"✅ Записано {len(rows)} строк")
    return clarify_list


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
# PDF обработка
# ─────────────────────────────────────────────

def find_pdf_in_payload(data):
    result = {"file_id": None, "url_download": None, "filename": None}
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

    if fallback_url:
        for label, token in [("main", BITRIX_WEBHOOK_URL.rstrip("/").split("/")[-1]), ("disk", DISK_TOKEN)]:
            print(f"Trying fallback_url with {label} Bearer token")
            result = try_download(fallback_url, {"Authorization": f"Bearer {token}"})
            if result:
                return result

    raise ValueError("Не удалось скачать PDF. Проверьте логи.")


# ─────────────────────────────────────────────
# Claude AI
# ─────────────────────────────────────────────

def extract_transactions(pdf_bytes):
    if not ANTHROPIC_API_KEY:
        raise ValueError("Не указан ANTHROPIC_API_KEY")

    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    system_prompt = f"""Из банковской выписки извлеки ВСЕ транзакции.

Верни ТОЛЬКО JSON массив без markdown:
[{{"date":"ДД.ММ.ГГГГ","description":"текст","amount":100.0,"type":"in","counterparty":"контрагент"}}]

type: in=поступление, out=списание. amount всегда положительное.
Контрагент — краткое название (например "Пятёрочка", "Яндекс GO", "М. Дария Руслановна").
НЕ добавляй поле category — категории проставим отдельно."""

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

    print(f"Claude response: {safe_preview(text, 500)}")

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
# Фоновая обработка
# ─────────────────────────────────────────────

def process_pdf_async(dialog_id, file_id, fallback_url):
    try:
        pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url)
        send_message(dialog_id, "🔍 Анализирую выписку через ИИ...")

        transactions = extract_transactions(pdf_bytes)

        total_in  = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "in")
        total_out = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "out")

        send_message(dialog_id, "📊 Записываю в таблицу...")
        clarify_list = write_to_sheets(transactions)

        # Основное сообщение
        send_message(
            dialog_id,
            f"✅ Готово! Найдено {len(transactions)} транзакций.\n"
            f"📈 Поступления: {total_in:,.2f} ₽\n"
            f"📉 Списания: {total_out:,.2f} ₽\n\n"
            f"🔗 [url={SHEET_URL}]Открыть таблицу Расходы Сбер[/url]"
        )

        # Все операции требующие уточнения
        if clarify_list:
            # Разбиваем на сообщения по 20 штук (лимит Bitrix)
            chunk_size = 20
            for i in range(0, len(clarify_list), chunk_size):
                chunk = clarify_list[i:i + chunk_size]
                clarify_text = f"❓ Нужно уточнить ({i+1}–{min(i+chunk_size, len(clarify_list))} из {len(clarify_list)}):\n\n"
                for item in chunk:
                    clarify_text += f"• {item['date']} — {item['counterparty']} — {item['amount']:,.0f} ₽ ({item['type']})\n"
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
            f"👋 Привет! Пришли PDF-выписку из банка — "
            f"я разнесу транзакции по категориям и запишу в таблицу [url={SHEET_URL}]Расходы Сбер[/url].",
        )
    else:
        send_message(dialog_id, "Пришли PDF-выписку из банка.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/init-sheets", methods=["GET"])
def init_sheets_route():
    try:
        init_sheets()
        return jsonify({"ok": True, "message": "Таблица инициализирована"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


if __name__ == "__main__":
    print("===== STARTING APP =====")
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
