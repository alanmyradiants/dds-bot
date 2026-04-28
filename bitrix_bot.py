import os
import io
import json
import time
import base64
import threading
import requests
from datetime import datetime
from urllib.parse import parse_qs, urlparse
from flask import Flask, request, jsonify, Response
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

# Параметры Local Application в Битриксе (нужны для OAuth-флоу).
# CLIENT_ID — публичный, можно положить значение по умолчанию.
# CLIENT_SECRET — секретный, обязательно через Railway env.
BITRIX_APP_CLIENT_ID     = os.getenv("BITRIX_APP_CLIENT_ID", "").strip()
BITRIX_APP_CLIENT_SECRET = os.getenv("BITRIX_APP_CLIENT_SECRET", "").strip()
APP_PUBLIC_URL           = os.getenv("APP_PUBLIC_URL", "https://dds-bot-production.up.railway.app").rstrip("/")

DISK_TOKEN = BITRIX_DISK_WEBHOOK_URL.rstrip("/").split("/")[-1]
SHEET_URL  = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"

# ─────────────────────────────────────────────
# Google Sheets credentials
# ─────────────────────────────────────────────
GOOGLE_CREDS_JSON = json.loads(os.environ.get('GOOGLE_CREDENTIALS'))

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
# Help-текст
# ─────────────────────────────────────────────

def build_help_text():
    """Полная инструкция бота — отдаётся на 'инструкция / помощь / help'."""
    return (
        "🤖 [B]DDS-бот — инструкция:[/B]\n\n"
        "📄 [B]Что я делаю[/B]\n"
        "Принимаю PDF-выписки Сбербанка, через ИИ распознаю все транзакции "
        "(дата, контрагент, сумма, тип) и автоматически разношу их по категориям "
        f"в Google-таблицу [url={SHEET_URL}]Расходы Сбер[/url].\n\n"
        "📋 [B]Как пользоваться[/B]\n"
        "▪️ Просто пришли PDF-выписку из приложения Сбербанка в этот чат\n"
        "▪️ Я скажу что начал обработку, проанализирую через Claude, "
        "разнесу по категориям и пришлю отчёт\n"
        "▪️ В отчёте: количество транзакций, сумма поступлений и списаний, ссылка на таблицу\n"
        "▪️ Если нашлись операции с категорией [B]❓ Уточнить[/B] — пришлю их "
        "отдельно списком, чтобы Алан их разметил вручную в листе «Правила»\n\n"
        "🧠 [B]Категоризация[/B]\n"
        "▪️ У меня встроен список правил для частых контрагентов "
        "(Пятёрочка → Супермаркеты, Yandex GO → Транспорт и т.д.)\n"
        "▪️ Дополнительно подтягиваю правила из листа [B]Правила[/B] в таблице — "
        "туда можно дописывать свои контрагенты\n"
        "▪️ Дублирующиеся транзакции пропускаются (по коду авторизации или "
        "по комбинации дата+контрагент+сумма)\n\n"
        "🚫 [B]Что НЕ делаю[/B]\n"
        "▪️ Не отвечаю на сообщения без PDF (только на «инструкция / помощь»)\n"
        "▪️ Не работаю с выписками других банков — только Сбербанк\n"
        "▪️ Не редактирую таблицу задним числом\n\n"
        "[B]Команды-ключи[/B]\n"
        "▪️ [B]инструкция[/B] / [B]помощь[/B] / [B]что ты умеешь[/B] — этот текст\n"
        "▪️ [B]/help[/B] — то же самое\n\n"
        f"📊 [B]Таблица[/B]: {SHEET_URL}\n\n"
        "Если что-то не работает — напиши Алану."
    )


HELP_KEYWORDS = (
    "инструкци",          # инструкция / инструкции / инструкцию
    "помощь",
    "help",
    "/help",
    "команд",             # команды / команда
    "что ты умеешь",
    "что умеешь",
    "что ты можешь",
    "что можешь",
    "возможности",
    "что делаешь",
    "кто ты",
)


def is_help_query(text):
    """Проверяет, спрашивает ли пользователь инструкцию."""
    if not text:
        return False
    return any(kw in text for kw in HELP_KEYWORDS)


# ─────────────────────────────────────────────
# Проверка сервисов
# ─────────────────────────────────────────────

def check_all_services():
    """Проверяет все сервисы и возвращает список проблем."""
    problems = []

    # 1. Проверка Anthropic API
    try:
        if not ANTHROPIC_API_KEY:
            problems.append("❌ Anthropic API: ключ не указан")
        else:
            test = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=10,
                messages=[{"role": "user", "content": "hi"}]
            )
            print("✅ Anthropic API: OK")
    except anthropic.AuthenticationError:
        problems.append("❌ Anthropic API: неверный ключ или не оплачен")
    except anthropic.PermissionDeniedError:
        problems.append("❌ Anthropic API: доступ запрещён, проверьте оплату")
    except Exception as e:
        problems.append(f"❌ Anthropic API: ошибка — {str(e)[:100]}")

    # 2. Проверка Google Sheets
    try:
        svc = get_sheets_service()
        svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        print("✅ Google Sheets: OK")
    except Exception as e:
        err = str(e).lower()
        if "403" in err or "permission" in err:
            problems.append("❌ Google Sheets: нет доступа или ключ недействителен")
        elif "404" in err:
            problems.append("❌ Google Sheets: таблица не найдена")
        else:
            problems.append(f"❌ Google Sheets: ошибка — {str(e)[:100]}")

    # 3. Проверка Bitrix24
    try:
        resp = requests.get(
            f"{BITRIX_WEBHOOK_URL}/app.info.json",
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("error") == "WRONG_AUTH_TYPE" or data.get("error") == "expired_token":
                problems.append("❌ Bitrix24: вебхук истёк или не оплачен")
            else:
                print("✅ Bitrix24: OK")
        elif resp.status_code == 401:
            problems.append("❌ Bitrix24: вебхук не авторизован или истёк")
        else:
            problems.append(f"❌ Bitrix24: статус {resp.status_code}")
    except Exception as e:
        problems.append(f"❌ Bitrix24: ошибка соединения — {str(e)[:100]}")

    return problems


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


def parse_auth_from_event(data):
    """Извлекает auth-данные из входящего события Bitrix.

    Битрикс присылает auth-блок в одном из ТРЁХ форматов в зависимости
    от типа события и Content-Type:

      1) form-encoded с bracket notation (чат-события ONIMBOTMESSAGEADD):
         "auth[access_token]" = "..."
      2) JSON с вложенным объектом (некоторые OAuth-флоу):
         {"auth": {"access_token": "..."}}
      3) flat UPPERCASE (install/placement события Local App):
         "AUTH_ID" = "...", "REFRESH_ID" = "...", "APPLICATION_TOKEN" = "..."

    Пробуем все три по очереди — заполняем только пустые поля, не затирая.

    Возвращает dict с возможными ключами: access_token, application_token,
    domain, client_endpoint, refresh_token. Любой из них может быть пустой
    строкой, если Битрикс его не прислал.
    """
    fields = ("access_token", "application_token", "domain",
              "client_endpoint", "refresh_token")

    # Формат 1: bracket notation
    result = {f: str(data.get(f"auth[{f}]") or "").strip() for f in fields}

    # Формат 2: nested dict — заполняем только пустые поля, не затирая
    auth_obj = data.get("auth")
    if isinstance(auth_obj, dict):
        for f in fields:
            if not result[f]:
                result[f] = str(auth_obj.get(f) or "").strip()

    # Формат 3: flat UPPERCASE (Local App install/placement payload).
    # Битрикс шлёт AUTH_ID вместо access_token, REFRESH_ID вместо
    # refresh_token. domain и client_endpoint в этом формате обычно
    # отсутствуют — их нужно выводить из BITRIX_WEBHOOK_URL.
    flat_uppercase_map = {
        "access_token":      "AUTH_ID",
        "refresh_token":     "REFRESH_ID",
        "application_token": "APPLICATION_TOKEN",
    }
    for f, key in flat_uppercase_map.items():
        if not result[f]:
            result[f] = str(data.get(key) or "").strip()

    return result


def derive_client_endpoint(fallback_url=None):
    """Если в payload нет client_endpoint, выводим его из других известных
    источников: BITRIX_WEBHOOK_URL → 'https://joto.bitrix24.ru/rest/'.
    """
    candidates = [fallback_url] if fallback_url else []
    candidates.append(BITRIX_WEBHOOK_URL)
    candidates.append(BITRIX_DISK_WEBHOOK_URL)
    for url in candidates:
        if not url:
            continue
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}/rest/"
    return ""


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

    # Заголовки Транзакции — вставляем принудительно в строку 1
    headers = [["Дата загрузки", "Кто загрузил", "Дата операции", "Время", "Код авторизации", "Месяц", "Контрагент",
                "Описание", "Приход", "Расход", "Категория",
                "Личное/Бизнес", "Статус"]]

    # Всегда перезаписываем заголовки в строке 1
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="Транзакции!A1",
        valueInputOption="RAW",
        body={"values": headers}
    ).execute()
    print("Заголовки обновлены")

    # Заголовок Правила — принудительно перезаписываем только заголовок
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="Правила!A1:C1",
        valueInputOption="RAW",
        body={"values": [["Контрагент", "Категория", "Личное/Бизнес"]]}
    ).execute()
    print("✅ Таблица инициализирована")
    print("ℹ️ Правила заполнятся автоматически при загрузке PDF")


def get_existing_rules(service):
    """Загружает существующие правила из таблицы."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Правила!A:C"
        ).execute()
        rows = result.get("values", [])
        # Словарь: контрагент (upper) -> (категория, тип)
        rules = {}
        for row in rows[1:]:  # пропускаем заголовок
            if len(row) >= 2 and row[0]:
                rules[row[0].upper().strip()] = (
                    row[1] if len(row) > 1 else "",
                    row[2] if len(row) > 2 else ""
                )
        return rules
    except Exception as e:
        print(f"get_existing_rules error: {e}")
        return {}


def save_new_rules(service, new_rules):
    """Добавляет новые правила в таблицу (только те которых ещё нет)."""
    if not new_rules:
        return
    try:
        rows = [[k, v[0], v[1]] for k, v in new_rules.items()]
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Правила!A:C",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
        print(f"✅ Добавлено новых правил: {len(rows)}")
    except Exception as e:
        print(f"save_new_rules error: {e}")


def _column_letter(idx):
    """0 → A, 1 → B, ... 26 → AA."""
    letters = ""
    n = idx
    while True:
        letters = chr(ord("A") + n % 26) + letters
        n = n // 26 - 1
        if n < 0:
            break
    return letters


def _normalize_amount(value):
    """Приводит сумму к виду '1234.56' (убирает пробелы, запятые, лишние нули)."""
    s = str(value or "").replace("\xa0", "").replace(" ", "").replace(",", ".").strip()
    if not s:
        return ""
    try:
        return f"{float(s):.2f}"
    except ValueError:
        return s


def _composite_key(date, counterparty, amount):
    """Ключ для дедупа транзакций без auth_code."""
    return (
        str(date or "").strip(),
        str(counterparty or "").upper().strip(),
        _normalize_amount(amount),
    )


def get_existing_dedup_sets(service):
    """Читает таблицу и возвращает два набора для проверки дублей:
      1) set кодов авторизации;
      2) set композитных ключей (дата, контрагент, сумма) — для строк без кода.
    Столбцы ищутся по именам заголовков, чтобы порядок колонок не ломал логику.
    """
    auth_codes = set()
    composite = set()
    try:
        header_resp = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Транзакции!1:1"
        ).execute()
        headers = (header_resp.get("values") or [[]])[0]

        def idx_of(name):
            try:
                return headers.index(name)
            except ValueError:
                return -1

        i_auth  = idx_of("Код авторизации")
        i_date  = idx_of("Дата операции")
        i_cp    = idx_of("Контрагент")
        i_in    = idx_of("Приход")
        i_out   = idx_of("Расход")

        if i_auth == -1 and (i_date == -1 or i_cp == -1 or (i_in == -1 and i_out == -1)):
            print("get_existing_dedup_sets: нужные заголовки не найдены")
            return auth_codes, composite

        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Транзакции!A:Z"
        ).execute()
        rows = result.get("values", [])

        def cell(row, idx):
            return row[idx] if 0 <= idx < len(row) else ""

        for row in rows[1:]:
            # Для каждой строки строим ОБА ключа (если возможно)
            # — чтобы ловить дубли и когда одна выписка с кодом, а другая без
            code = str(cell(row, i_auth)).strip() if i_auth != -1 else ""
            if code:
                auth_codes.add(code)
            date = cell(row, i_date) if i_date != -1 else ""
            cp   = cell(row, i_cp) if i_cp != -1 else ""
            amt_in  = cell(row, i_in) if i_in != -1 else ""
            amt_out = cell(row, i_out) if i_out != -1 else ""
            amt = amt_in or amt_out
            if date and cp and str(amt).strip():
                composite.add(_composite_key(date, cp, amt))

        print(f"Дедуп: auth_codes={len(auth_codes)}, composite={len(composite)}")
        return auth_codes, composite
    except Exception as e:
        print(f"get_existing_dedup_sets error: {e}")
        return auth_codes, composite


def get_existing_auth_codes(service):
    """Совместимость со старым кодом — возвращает только auth_codes."""
    auth_codes, _ = get_existing_dedup_sets(service)
    return auth_codes


def write_to_sheets(transactions, uploader=""):
    """Записывает транзакции в Google Sheets."""
    service = get_sheets_service()
    from datetime import timezone, timedelta
    moscow_tz = timezone(timedelta(hours=3))
    upload_date = datetime.now(moscow_tz).strftime("%d.%m.%Y %H:%M")

    # Загружаем существующие правила из таблицы
    sheet_rules = get_existing_rules(service)

    # Загружаем существующие ключи дедупа: коды авторизации + (дата, контрагент, сумма)
    existing_auth_codes, existing_composite = get_existing_dedup_sets(service)

    rows = []
    clarify_list = []
    new_rules = {}  # новые правила которые нужно сохранить
    skipped = 0  # счётчик пропущенных дублей

    # Узнаём с какой строки начнём запись (для формул ВПР)
    try:
        existing = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Транзакции!A:A"
        ).execute()
        current_row = len(existing.get("values", [])) + 1
    except Exception:
        current_row = 2

    for t in transactions:
        amount = float(t.get("amount", 0) or 0)
        t_type = t.get("type", "out")
        counterparty = t.get("counterparty", "")
        description = t.get("description", "")
        counterparty_upper = counterparty.upper().strip()
        auth_code = str(t.get("auth_code", "") or "").strip()
        date_str_raw = str(t.get("date", "") or "").strip()

        # Строим оба ключа для проверки дублей
        composite_key = _composite_key(date_str_raw, counterparty, amount)
        composite_valid = bool(composite_key[0] and composite_key[1] and composite_key[2])

        # Дубль, если совпал хотя бы один ключ (код авторизации ИЛИ дата+контрагент+сумма)
        is_duplicate = (
            (auth_code and auth_code in existing_auth_codes)
            or (composite_valid and composite_key in existing_composite)
        )
        if is_duplicate:
            skipped += 1
            continue

        # Регистрируем оба ключа, чтобы ловить дубли внутри одной пачки
        if auth_code:
            existing_auth_codes.add(auth_code)
        if composite_valid:
            existing_composite.add(composite_key)

        # Сначала смотрим правила из таблицы (точное совпадение контрагента)
        if counterparty_upper in sheet_rules:
            category, biz_type = sheet_rules[counterparty_upper]
        else:
            # Потом встроенные правила
            category, biz_type = apply_rules(counterparty, description, amount, t_type)

        inc = amount if t_type == "in" else ""
        exp = amount if t_type == "out" else ""
        status = "❓ Уточнить" if category == "❓ Уточнить" else "✅"

        if status == "❓ Уточнить":
            clarify_list.append({
                "date": t.get("date", ""),
                "counterparty": counterparty,
                "amount": amount,
                "type": "Поступление" if t_type == "in" else "Списание",
                "description": description,
            })
            # Записываем в новые правила с пустой категорией — чтобы Алан заполнил
            if counterparty_upper not in sheet_rules and counterparty not in new_rules:
                new_rules[counterparty] = ("❓ Уточнить", "")
        else:
            # Если правило было применено из BUILTIN — сохраняем точный контрагент
            if counterparty_upper not in sheet_rules and counterparty not in new_rules:
                new_rules[counterparty] = (category, biz_type)

        # Извлекаем месяц из даты (формат ДД.ММ.ГГГГ → Апрель 2026)
        date_str = t.get("date", "")
        try:
            from datetime import datetime as dt
            d = dt.strptime(date_str, "%d.%m.%Y")
            months_ru = ["","Январь","Февраль","Март","Апрель","Май","Июнь",
                         "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
            month_label = f"{months_ru[d.month]} {d.year}"
        except Exception:
            month_label = ""

        rows.append([
            upload_date,
            uploader,
            date_str,
            t.get("time", ""),
            auth_code,
            month_label,
            counterparty,
            description,
            inc,
            exp,
            "=IFERROR(VLOOKUP(G" + str(current_row) + ";'Правила'!$A:$B;2;0);\"? Уточнить\")",
            "=IFERROR(VLOOKUP(G" + str(current_row) + ";'Правила'!$A:$C;3;0);\"\")",
            status,
        ])
        current_row += 1

    # Записываем транзакции
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="Транзакции!A:M",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    ).execute()

    # Сохраняем новые правила
    save_new_rules(service, new_rules)

    print(f"✅ Записано {len(rows)} строк, пропущено дублей: {skipped}, новых правил: {len(new_rules)}")
    return clarify_list, skipped


# ─────────────────────────────────────────────
# Bitrix API
# ─────────────────────────────────────────────

def bitrix_post(method_name, payload, timeout=20):
    url = f"{BITRIX_WEBHOOK_URL}/{method_name}.json"
    response = requests.post(url, json=payload, timeout=timeout)
    print(f"{method_name} POST status={response.status_code}")
    return response


def _combine_first_last(first, last):
    """Объединяет имя и фамилию, избегая дублирования.

    Битрикс иногда шлёт NAME="Алан Мурадянц" (уже с фамилией)
    И отдельно LAST_NAME="Мурадянц" — простое склеивание дало бы
    "Алан Мурадянц Мурадянц". Эта функция ловит такой случай.
    """
    first = (first or "").strip()
    last = (last or "").strip()
    if not first:
        return last
    if not last:
        return first
    # Если фамилия уже есть в first как отдельное слово — не дублируем
    first_words_lower = [w.lower() for w in first.split()]
    if last.lower() in first_words_lower:
        return first
    return f"{first} {last}"


def extract_uploader_name(data):
    """Определяет ФИО сотрудника, загрузившего файл.

    Битрикс непредсказуем в том, что кладёт в NAME:
      — иногда только имя ("Алан"),
      — иногда полное ФИО ("Алан Мурадянц").
    Поэтому объединяем через _combine_first_last, чтобы не получить
    "Алан Мурадянц Мурадянц", и при необходимости добираем ФИО из user.get.
    """
    first_name = str(data.get("data[USER][FIRST_NAME]") or "").strip()
    name_field = str(data.get("data[USER][NAME]") or "").strip()
    last_name  = str(data.get("data[USER][LAST_NAME]") or "").strip()

    # FIRST_NAME приоритетнее NAME (в NAME может быть уже ФИО целиком)
    first = first_name or name_field

    # Если в вебхуке есть и имя, и фамилия — склеиваем аккуратно и отдаём
    if first and last_name:
        return _combine_first_last(first, last_name)

    # Иначе идём в user.get за каноничным ФИО
    user_id = (
        data.get("data[USER][ID]")
        or data.get("data[PARAMS][FROM_USER_ID]")
        or data.get("auth[user_id]")
    )
    if user_id:
        try:
            resp = bitrix_post("user.get", {"ID": user_id}, timeout=10)
            if resp.status_code == 200:
                result = resp.json().get("result") or []
                if result:
                    u = result[0]
                    n = (u.get("NAME") or "").strip()
                    l = (u.get("LAST_NAME") or "").strip()
                    combined = _combine_first_last(n, l)
                    if combined:
                        return combined
        except Exception as e:
            print(f"extract_uploader_name error: {e}")

    # Фолбэк — то, что было в вебхуке
    fallback = _combine_first_last(first, last_name)
    return fallback if fallback else "Неизвестно"


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
    # Увеличен таймаут с 60 до 120 секунд
    resp = requests.get(url, headers=headers, timeout=120, allow_redirects=True)
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


def get_pdf_bytes(file_id, fallback_url=None, auth=None):
    """Скачивает PDF — каждый раз получаем свежий URL и сразу качаем.

    Стратегия (от самого надёжного к самому слабому):
      0. Если в auth есть access_token + client_endpoint, ходим в REST
         от имени пользователя, который отправил файл — у него точно
         есть доступ к собственному файлу. Это решает проблему 403
         для сотрудников, не являющихся владельцем вебхука.
      1. Основной вебхук портала (legacy fallback).
      2. Disk-вебхук, до 5 попыток с паузой (legacy fallback).
      3. Прямой fallback_url из payload + Bearer-токен вебхука.
    """

    auth = auth or {}
    user_token    = (auth.get("access_token") or "").strip()
    user_endpoint = (auth.get("client_endpoint") or "").strip()
    if user_token and not user_endpoint:
        # На всякий случай — если событие пришло без client_endpoint,
        # выводим его из основного вебхука (это всё ещё тот же портал).
        user_endpoint = derive_client_endpoint()

    def fetch_via_endpoint(endpoint, params, label):
        """Запрашивает disk.file.get у произвольного REST-эндпоинта,
        достаёт DOWNLOAD_URL и сразу качает файл."""
        try:
            resp = requests.get(
                f"{endpoint.rstrip('/')}/disk.file.get.json",
                params=params,
                timeout=60,
            )
            print(f"[{label}] disk.file.get status={resp.status_code}")
            if resp.status_code != 200:
                return None
            payload = resp.json()
            if not payload.get("result"):
                return None
            dl = extract_download_url(payload["result"])
            if not dl:
                print(f"[{label}] no download_url in result")
                return None
            print(f"[{label}] downloading immediately...")
            return try_download(dl)
        except Exception as e:
            print(f"[{label}] failed: {e}")
            return None

    # Попытка 0: контекст пользователя, отправившего файл (самое надёжное)
    if user_token and user_endpoint:
        result = fetch_via_endpoint(
            user_endpoint,
            {"id": file_id, "auth": user_token},
            "user-context",
        )
        if result:
            return result
        print("user-context failed, fallback to webhooks")
    else:
        print("no user access_token in event, skipping user-context attempt")

    # Попытка 1: основной вебхук
    result = fetch_via_endpoint(BITRIX_WEBHOOK_URL, {"id": file_id}, "main")
    if result:
        return result

    # Попытки 2-6: disk-вебхук (5 раз, каждый раз свежий URL, пауза между попытками)
    for attempt in range(5):
        print(f"disk webhook attempt {attempt + 1}/5")
        result = fetch_via_endpoint(
            BITRIX_DISK_WEBHOOK_URL, {"id": file_id}, f"disk-{attempt+1}"
        )
        if result:
            return result
        # Пауза перед следующей попыткой (кроме последней)
        if attempt < 4:
            print(f"Waiting 3s before next attempt...")
            time.sleep(3)

    # Fallback URL из payload
    if fallback_url:
        # Если есть user access_token — пробуем им же
        if user_token:
            print("Trying fallback_url with user access_token (Bearer)")
            result = try_download(fallback_url, {"Authorization": f"Bearer {user_token}"})
            if result:
                return result
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

    system_prompt = f"""Из банковской выписки Сбербанка извлеки ВСЕ транзакции.

Каждая транзакция в выписке содержит две строки:
- Первая: дата операции, время, категория, сумма
- Вторая: дата обработки, код авторизации, описание операции

Верни ТОЛЬКО JSON массив без markdown:
[{{
  "date": "ДД.ММ.ГГГГ",
  "time": "ЧЧ:ММ",
  "processing_date": "ДД.ММ.ГГГГ",
  "auth_code": "646991",
  "description": "текст описания",
  "amount": 100.0,
  "type": "in",
  "counterparty": "краткое название"
}}]

Правила:
- type: in=поступление/зачисление, out=списание. amount всегда положительное.
- counterparty — краткое понятное название ("Пятёрочка", "Яндекс GO", "М. Дария Руслановна")
- auth_code — код авторизации (6 цифр), если есть
- time — время операции в формате ЧЧ:ММ
- processing_date — дата обработки (вторая строка транзакции)
- НЕ добавляй поле category"""

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
    if start == -1:
        raise ValueError(f"Транзакции не найдены. Ответ: {safe_preview(text, 300)}")

    end = text.rfind("]")

    # Если ] не найден — JSON обрезан, восстанавливаем
    if end == -1 or end < start:
        print("JSON обрезан — восстанавливаем по последнему }")
        last_close = text.rfind("}")
        if last_close == -1:
            raise ValueError("Не удалось найти транзакции в ответе")
        json_str = text[start:last_close + 1] + "]"
    else:
        json_str = text[start:end + 1]

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # Ещё раз пробуем восстановить по последнему }
        last_close = json_str.rfind("}")
        if last_close == -1:
            raise ValueError("Не удалось распарсить ответ ИИ")
        try:
            return json.loads(json_str[:last_close + 1] + "]")
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка парсинга JSON: {e}")


# ─────────────────────────────────────────────
# Фоновая обработка
# ─────────────────────────────────────────────

def process_pdf_async(dialog_id, file_id, fallback_url, uploader="", auth=None):
    try:
        # Проверяем все сервисы перед обработкой
        problems = check_all_services()
        if problems:
            msg = "⚠️ Обнаружены проблемы с сервисами:\n\n" + "\n".join(problems)
            msg += "\n\nПожалуйста проверьте оплату и настройки."
            send_message(dialog_id, msg)
            return

        pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url, auth=auth)
        send_message(dialog_id, "🔍 Анализирую выписку через ИИ...")

        transactions = extract_transactions(pdf_bytes)

        total_in  = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "in")
        total_out = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "out")

        send_message(dialog_id, "📊 Записываю в таблицу...")
        clarify_list, skipped = write_to_sheets(transactions, uploader=uploader)

        skipped_text = f"\n⚠️ Пропущено дублей: {skipped}" if skipped > 0 else ""
        # Основное сообщение
        send_message(
            dialog_id,
            f"✅ Готово! Найдено {len(transactions)} транзакций.\n"
            f"📈 Поступления: {total_in:,.2f} ₽\n"
            f"📉 Списания: {total_out:,.2f} ₽"

            f"{skipped_text}\n\n"
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
        uploader = extract_uploader_name(data)
        # Достаём auth-токен пользователя из события — нужен для скачивания
        # файлов, загруженных НЕ владельцем вебхука (см. get_pdf_bytes).
        auth = parse_auth_from_event(data)
        send_message(dialog_id, "📄 Получил PDF, начинаю обработку...")
        thread = threading.Thread(
            target=process_pdf_async,
            args=(dialog_id, file_id, fallback_url, uploader, auth),
            daemon=True,
        )
        thread.start()

    elif is_help_query(message_text):
        # Полная инструкция: «инструкция / помощь / help / команды / что ты умеешь / возможности»
        send_message(dialog_id, build_help_text())
    elif message_text in ("привет", "start", "/start", ""):
        send_message(
            dialog_id,
            f"👋 Привет! Пришли PDF-выписку из банка — "
            f"я разнесу транзакции по категориям и запишу в таблицу [url={SHEET_URL}]Расходы Сбер[/url].\n\n"
            f"Напиши [B]инструкция[/B] — расскажу подробнее.",
        )
    else:
        send_message(
            dialog_id,
            "Пришли PDF-выписку из банка. "
            "Напиши [B]инструкция[/B] чтобы узнать что я умею.",
        )

    return jsonify({"result": "ok"})


@app.route("/check", methods=["GET"])
def check_services_route():
    try:
        problems = check_all_services()
        if problems:
            return jsonify({"ok": False, "problems": problems})
        return jsonify({"ok": True, "message": "Все сервисы работают ✅"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/help-text", methods=["GET"])
def help_text_route():
    """Возвращает текущий HELP_TEXT для дебага/превью."""
    return Response(build_help_text(), mimetype="text/plain; charset=utf-8")


@app.route("/init-sheets", methods=["GET"])
def init_sheets_route():
    try:
        init_sheets()
        return jsonify({"ok": True, "message": "Таблица инициализирована"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ─────────────────────────────────────────────
# OAuth Local App — установка приложения
# ─────────────────────────────────────────────

def _render_install_page(bot_id=None, error=None):
    """Простая HTML-страница, которую видит пользователь после установки."""
    if error:
        body = f"""
        <h2 style="color:#dc3545;">❌ Ошибка установки</h2>
        <p>{error}</p>
        <p style="color:#666;font-size:14px;">Если ошибка не очевидна — посмотри логи Railway за последнюю минуту.</p>
        """
    elif bot_id:
        body = f"""
        <h2 style="color:#28a745;">✅ ДДС-бот установлен!</h2>
        <p><b>BOT_ID:</b> <code>{bot_id}</code></p>
        <p>⚠️ <b>Важно:</b> обнови переменную окружения <code>BOT_CLIENT_ID</code> в Railway
        на это значение и перезапусти сервис, чтобы бот мог отправлять сообщения от имени нового профиля.</p>
        <p>После этого найди в списке чатов «ДДС Бот» и пришли ему PDF-выписку Сбербанка.</p>
        """
    else:
        body = """
        <h2>ДДС-бот · install endpoint</h2>
        <p>Этот URL принимает событие <code>ONAPPINSTALL</code> от Битрикса при первой установке приложения.</p>
        <p>GET-запрос ничего не делает — приходи через POST из Bitrix24.</p>
        """
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><title>ДДС-бот · установка</title></head>
<body style="font-family:system-ui,sans-serif;max-width:640px;margin:40px auto;padding:24px;line-height:1.5;">
{body}
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


def _register_chat_bot(client_endpoint, access_token):
    """Регистрирует чат-бота через imbot.register от имени установившего.

    Передаём auth (access_token), Битрикс создаёт бота под нашим Local App.
    После этого все ONIMBOTMESSAGEADD события будут приходить с auth[access_token]
    того пользователя, кто отправил сообщение — и get_pdf_bytes сможет
    скачивать файлы от его имени.

    Возвращает BOT_ID при успехе, кидает ValueError при ошибке.
    """
    bot_handler_url = f"{APP_PUBLIC_URL}/bot"
    payload = {
        "CODE": "dds_bot",
        "TYPE": "B",
        "EVENT_HANDLER": bot_handler_url,
        "EVENT_MESSAGE_ADD":     bot_handler_url,
        "EVENT_WELCOME_MESSAGE": bot_handler_url,
        "EVENT_BOT_DELETE":      bot_handler_url,
        "PROPERTIES": {
            "NAME": "ДДС Бот",
            "WORK_POSITION": "PDF-выписки Сбербанка → Google-таблица",
            "COLOR": "GREEN",
        },
    }
    url = f"{client_endpoint.rstrip('/')}/imbot.register.json"
    resp = requests.post(url, params={"auth": access_token}, json=payload, timeout=20)
    print(f"imbot.register status={resp.status_code} body={safe_preview(resp.text, 500)}")
    try:
        result = resp.json()
    except Exception as e:
        raise ValueError(f"imbot.register: невалидный JSON в ответе: {e}")
    if "error" in result:
        raise ValueError(
            f"imbot.register: {result.get('error_description') or result.get('error')}"
        )
    bot_id = result.get("result")
    if not bot_id:
        raise ValueError(f"imbot.register: пустой result, ответ: {safe_preview(resp.text, 300)}")
    return bot_id


@app.route("/install", methods=["GET", "POST"])
def install_handler():
    """Обработчик первоначальной установки Local App.

    Битрикс шлёт сюда POST с auth-данными (в одном из трёх форматов —
    parse_auth_from_event разберётся). От имени установившего регистрируем
    чат-бота через imbot.register — после этого все последующие чат-события
    на /bot будут содержать auth[access_token] отправителя сообщения, и
    patch в get_pdf_bytes (PR #2) наконец сможет скачивать файлы.

    Для install-событий Битрикс не присылает client_endpoint — выводим его
    из BITRIX_WEBHOOK_URL (тот же портал).
    """
    if request.method == "GET":
        return _render_install_page()

    data = parse_request_data()
    print("===== INSTALL EVENT =====")
    print(f"Method: {request.method}, Content-Type: {request.content_type}")
    top_keys = sorted(list(data.keys()))[:30]
    print(f"Top-level keys ({len(data)}): {top_keys}")
    auth_obj_type = type(data.get("auth")).__name__
    print(f"data['auth'] type: {auth_obj_type}")
    print(safe_preview(data, 5000))

    auth = parse_auth_from_event(data)
    access_token    = auth.get("access_token")
    client_endpoint = auth.get("client_endpoint") or derive_client_endpoint()
    print(f"access_token: {'set, len=' + str(len(access_token)) if access_token else 'EMPTY'}")
    print(f"client_endpoint: {client_endpoint or 'EMPTY'}")

    if not access_token:
        found_auth = {k: ("set" if v else "empty") for k, v in auth.items()}
        msg = (
            "Битрикс не прислал access_token ни в одном из известных форматов. "
            f"Что распарсилось: {found_auth}. "
            f"Top-level ключи: {top_keys}. "
            "Скопируй текст и пришли мне."
        )
        print(f"INSTALL ERROR: {msg}")
        return _render_install_page(error=msg), 400

    if not client_endpoint:
        msg = ("Не удалось определить client_endpoint. Проверь, что переменная "
               "BITRIX_WEBHOOK_URL в Railway указывает на твой портал Битрикса.")
        print(f"INSTALL ERROR: {msg}")
        return _render_install_page(error=msg), 400

    try:
        bot_id = _register_chat_bot(client_endpoint, access_token)
        print(f"✅ Бот зарегистрирован, BOT_ID={bot_id}")
        return _render_install_page(bot_id=bot_id)
    except Exception as e:
        print(f"INSTALL ERROR: {e}")
        return _render_install_page(error=str(e)), 500


if __name__ == "__main__":
    print("===== STARTING APP =====")
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
