import os
import io
import json
import time
import uuid
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

# DIALOG_ID чата «Платежи» — ВСЕ заявки на оплату падают сюда, независимо от
# плательщика (и Чермен, и Анастасия). Конкретный плательщик тегается внутри
# чата через [USER=id]. Формат: "chatNNN" или числовой ID пользователя.
# Узнать ID чата можно через /chats (im.recent.get) или из URL чата.
PAYMENT_CHAT_ID = os.getenv("PAYMENT_CHAT_ID", "chat242").strip()

# BOT_ID чат-бота (из конструктора чат-бота). Нужен для imbot.message.add
# при вызове через входящий вебхук — иначе Битрикс не знает, от кого слать.
BITRIX_BOT_ID = os.getenv("BITRIX_BOT_ID", "").strip()

# Сотрудники-плательщики для поля «Кто оплачивает».
# Каждое имя сопоставляется по подстроке в ФИО (ID знать не нужно). Несколько
# имён через запятую → переопределяется env PAYMENT_DEFAULT_PAYER_NAME.
# Если совпал ровно один — поле блокируется; если несколько — выпадающий список.
PAYMENT_PAYER_NAMES = [
    n.strip()
    for n in os.getenv("PAYMENT_DEFAULT_PAYER_NAME", "Кисиев, Фаткуллина").split(",")
    if n.strip()
]

# ─────────────────────────────────────────────
# Заказы на производство (форма /order)
# ─────────────────────────────────────────────
# Чат, куда падают заказы на согласование — ОТДЕЛЬНЫЙ от «Платежей».
# Задаётся своим env PRODUCTION_CHAT_ID (формат "chatNNN"). Если не задан —
# уведомления о заказах не отправляются (специально не подмешиваем чат платежей,
# чтобы заказы на производство не валились в чат «Платежи»). ID чата можно
# узнать на странице /chats (im.recent.get).
PRODUCTION_CHAT_ID = os.getenv("PRODUCTION_CHAT_ID", "").strip()

# Кто согласовывает заказы (по умолчанию — Алан). Сопоставляется по подстроке
# в ФИО, как плательщики. Можно перечислить несколько через запятую.
PRODUCTION_APPROVER_NAMES = [
    n.strip()
    for n in os.getenv("PRODUCTION_APPROVER_NAME", "Алан").split(",")
    if n.strip()
]

# Интеграция с CRM: после согласования создаём объект в CRM и ставим стадию.
# Всё best-effort — если CRM не настроена, CRM-шаг просто пропускается, а заказ
# всё равно фиксируется в Google Sheets и чате. Вебхуку нужен scope `crm`;
# по умолчанию берём общий BITRIX_WEBHOOK_URL.
BITRIX_CRM_WEBHOOK_URL = os.getenv("BITRIX_CRM_WEBHOOK_URL", BITRIX_WEBHOOK_URL).rstrip("/")

# Режим CRM: "deal" — создавать СДЕЛКУ в воронке (у нас настроена воронка
# «Заказы на производство»); "spa" — элемент смарт-процесса; "" / "off" — выкл.
PRODUCTION_CRM_MODE = os.getenv("PRODUCTION_CRM_MODE", "deal").strip().lower()

# Для режима "deal": воронка (направление) и стадия, на которую ставим заказ
# после согласования. ID можно подсмотреть на странице /order/crm-info.
PRODUCTION_DEAL_CATEGORY_ID = os.getenv("PRODUCTION_DEAL_CATEGORY_ID", "").strip()
PRODUCTION_DEAL_STAGE_ID    = os.getenv("PRODUCTION_DEAL_STAGE_ID", "").strip()

# Для режима "spa": смарт-процесс.
PRODUCTION_SPA_ENTITY_TYPE_ID = os.getenv("PRODUCTION_SPA_ENTITY_TYPE_ID", "").strip()
PRODUCTION_SPA_CATEGORY_ID    = os.getenv("PRODUCTION_SPA_CATEGORY_ID", "").strip()
PRODUCTION_SPA_STAGE_ID       = os.getenv("PRODUCTION_SPA_STAGE_ID", "").strip()

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

# Категории именно для ЗАЯВОК НА ОПЛАТУ (форма /pay). Отдельно от DDS_CATEGORIES,
# которые используются для разнесения PDF-выписок. Редактируются на /categories,
# хранятся в листе «Категории заявок». Это — список по умолчанию / для сброса.
PAYMENT_CATEGORIES_DEFAULT = [
    "Зарплата",
    "Коммунальные услуги",
    "Займы",
    "Фотосессия",
    "Автоматизация",
    "Логистика",
    "Выкуп товара",
    "Реклама у блогера",
    "Самовыкуп",
    "UGC-Блогеры",
    "Закуп ткани",
    "Закуп фурнитуры",
    "Оплата ОТК",
    "Оплата образцы",
    "Создание креатором контента для Joto",
    "Дизайнер",
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
    for sheet_name in ["Транзакции", "Правила", "Заявки", "Категории заявок", "Заказы"]:
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
    headers = [["Дата загрузки", "Кто загрузил", "Владелец счета", "Дата операции", "Время", "Код авторизации", "Месяц", "Контрагент",
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
    # Заголовок Заявки — журнал заявок на оплату
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="Заявки!A1:O1",
        valueInputOption="RAW",
        body={"values": [[
            "Дата создания", "Заявитель", "Категория", "Сумма",
            "Получатель", "Реквизиты", "Назначение платежа",
            "Срок оплаты", "Срочность", "Плательщик", "Файл счёта", "Статус",
            # Технические поля для отмены заявки из чата (можно скрыть колонки).
            "ID заявки", "ID заявителя", "ID сообщения",
        ]]},
    ).execute()

    # Заголовок Заказы — журнал заказов на производство.
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="Заказы!A1:U1",
        valueInputOption="RAW",
        body={"values": [[
            "Дата создания", "Заявитель", "Дата заказа", "Артикул / модель",
            "Размерный ряд и количество", "Цвет", "Ткань (состав)", "Фурнитура",
            "Общий тираж", "Подрядчик / цех", "Срок (дедлайн)", "Срочность",
            "Образец нужен", "Файлы", "Комментарий", "Статус",
            # Технические поля для согласования из чата (можно скрыть колонки).
            "ID заказа", "ID заявителя", "ID согласующего", "ID сообщения",
            "CRM",
        ]]},
    ).execute()

    # Лист «Категории заявок» — источник списка категорий для формы.
    # Заголовок ставим всегда. Стандартные категории из PAYMENT_CATEGORIES_DEFAULT
    # дописываем недеструктивно: добавляем только те, которых ещё нет в листе, —
    # так новые категории появляются после /init-sheets, а правки пользователя
    # (свои категории) не затираются.
    service.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range="Категории заявок!A1",
        valueInputOption="RAW",
        body={"values": [["Категория"]]},
    ).execute()
    existing = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range="Категории заявок!A2:A"
    ).execute().get("values", [])
    existing_cats = [r[0].strip() for r in existing if r and r[0].strip()]
    missing = [c for c in PAYMENT_CATEGORIES_DEFAULT if c not in existing_cats]
    if missing:
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Категории заявок!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [[c] for c in missing]},
        ).execute()
        print(f"ℹ️ Дописаны недостающие категории: {', '.join(missing)}")

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


def write_to_sheets(transactions, uploader="", account_owner=""):
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
            account_owner,
            date_str,
            t.get("time", ""),
            auth_code,
            month_label,
            counterparty,
            description,
            inc,
            exp,
            "=IFERROR(VLOOKUP(H" + str(current_row) + ";'Правила'!$A:$B;2;0);\"? Уточнить\")",
            "=IFERROR(VLOOKUP(H" + str(current_row) + ";'Правила'!$A:$C;3;0);\"\")",
            status,
        ])
        current_row += 1

    # Записываем транзакции
    service.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range="Транзакции!A:N",
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


def send_message(dialog_id, text, keyboard=None):
    """Отправляет сообщение ботом. Возвращает ID сообщения (int) или None.

    keyboard — необязательный список кнопок (формат Bitrix imbot KEYBOARD),
    например кнопка «Отменить заявку».
    """
    if not dialog_id:
        return None
    try:
        payload = {"DIALOG_ID": dialog_id, "MESSAGE": text, "CLIENT_ID": BOT_CLIENT_ID}
        # При вызове через входящий вебхук (вне контекста события) Битриксу
        # нужен ещё и BOT_ID, иначе он не знает, от чьего имени слать.
        if BITRIX_BOT_ID:
            payload["BOT_ID"] = BITRIX_BOT_ID
        if keyboard:
            payload["KEYBOARD"] = keyboard
        resp = bitrix_post("imbot.message.add", payload, timeout=15)
        try:
            return resp.json().get("result")
        except Exception:
            return None
    except Exception as e:
        print(f"send_message error: {e}")
        return None


def _is_payment_chat(dialog_id):
    """True, если диалог — это чат «Платежи» (PAYMENT_CHAT_ID).

    Сравниваем по номеру чата, т.к. Битрикс шлёт DIALOG_ID как "chat242",
    а TO_CHAT_ID — как "242".
    """
    if not dialog_id or not PAYMENT_CHAT_ID:
        return False
    norm = lambda x: str(x).lower().replace("chat", "").strip()
    return norm(dialog_id) == norm(PAYMENT_CHAT_ID)


def update_bot_message(message_id, text):
    """Редактирует ранее отправленное ботом сообщение и убирает у него кнопки."""
    if not message_id:
        return
    try:
        payload = {
            "MESSAGE_ID": message_id,
            "MESSAGE": text,
            "CLIENT_ID": BOT_CLIENT_ID,
            "KEYBOARD": "N",  # убрать кнопки
        }
        if BITRIX_BOT_ID:
            payload["BOT_ID"] = BITRIX_BOT_ID
        bitrix_post("imbot.message.update", payload, timeout=15)
    except Exception as e:
        print(f"update_bot_message error: {e}")


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


def fetch_via_attached_object(endpoint, file_id, label, access_token=None):
    """disk.attachedObject.get — для файлов, прикреплённых к чату.

    В Битриксе у chat-attached файлов есть отдельный «attached object»,
    к которому имеют доступ ВСЕ участники чата, даже если на сам файл
    в Диске прав нет. Это спасает ситуацию, когда сотрудник присылает
    PDF из своего личного Диска — обычный disk.file.get вернёт 403,
    а disk.attachedObject.get может отдать ссылку на скачивание.

    Bitrix принимает в качестве id как attached_object_id, так и
    в некоторых конфигурациях file_id (он внутри ищет привязанный
    объект). Пробуем file_id — если Bitrix умеет, найдёт сам.
    """
    try:
        params = {"id": file_id}
        if access_token:
            params["auth"] = access_token
        resp = requests.get(
            f"{endpoint.rstrip('/')}/disk.attachedObject.get.json",
            params=params,
            timeout=60,
        )
        body_preview = safe_preview(resp.text, 300)
        print(f"[{label}] disk.attachedObject.get status={resp.status_code} body={body_preview}")
        if resp.status_code != 200:
            return None
        payload = resp.json()
        if payload.get("error"):
            return None
        result_obj = payload.get("result") or {}
        dl = extract_download_url(result_obj)
        if not dl:
            print(f"[{label}] no download_url in attachedObject result")
            return None
        print(f"[{label}] downloading via attachedObject...")
        return try_download(dl)
    except Exception as e:
        print(f"[{label}] attachedObject failed: {e}")
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

    # Попытка 0.5: disk.attachedObject.get — для файлов в чатах.
    # Это спасает кейс, когда не-владелец webhook'а (сотрудник) загружает
    # PDF — disk.file.get вернёт 403, а attachedObject доступен участникам
    # чата (бот == участник чата, поэтому может скачать).
    print("[main] trying disk.attachedObject.get for chat-attached file")
    result = fetch_via_attached_object(BITRIX_WEBHOOK_URL, file_id, "main-attached", access_token=user_token or None)
    if result:
        return result
    result = fetch_via_attached_object(BITRIX_DISK_WEBHOOK_URL, file_id, "disk-attached")
    if result:
        return result

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

def is_bank_statement(pdf_bytes):
    """Спрашивает у ИИ, является ли PDF банковской выпиской.

    Нужно для чата «Платежи»: туда кидают чеки и платёжки, которые НЕ надо
    разносить как выписку. Возвращает True только если это похоже на
    банковскую выписку со списком операций. При ошибке — False (молчим).
    """
    if not ANTHROPIC_API_KEY:
        return False
    try:
        pdf_b64 = base64.b64encode(pdf_bytes).decode()
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=10,
            system=(
                "Определи тип документа в PDF. Ответь ОДНИМ словом без знаков:\n"
                "STATEMENT — если это банковская ВЫПИСКА по счёту/карте "
                "(перечень операций за период).\n"
                "OTHER — если это что-то другое: чек, квитанция, платёжное "
                "поручение, счёт на оплату, договор и т.п."
            ),
            messages=[{
                "role": "user",
                "content": [
                    {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": pdf_b64}},
                    {"type": "text", "text": "Это банковская выписка? STATEMENT или OTHER."},
                ],
            }],
        )
        answer = "".join(
            b.text for b in resp.content if getattr(b, "type", "") == "text"
        ).strip().upper()
        print(f"is_bank_statement → {answer!r}")
        return "STATEMENT" in answer
    except Exception as e:
        print(f"is_bank_statement error: {e}")
        return False


def extract_transactions(pdf_bytes):
    if not ANTHROPIC_API_KEY:
        raise ValueError("Не указан ANTHROPIC_API_KEY")

    pdf_b64 = base64.b64encode(pdf_bytes).decode()

    system_prompt = f"""Из банковской выписки Сбербанка извлеки:
1. ВСЕ транзакции
2. Владельца счета (ищи "Владелец счета" и полное имя после него)

Верни ТОЛЬКО JSON объект БЕЗ markdown и БЕЗ дополнительного текста:
{{
  "account_owner": "Фамилия Имя Отчество или как указано в выписке",
  "transactions": [
    {{
      "date": "ДД.ММ.ГГГГ",
      "time": "ЧЧ:ММ",
      "processing_date": "ДД.ММ.ГГГГ",
      "auth_code": "646991",
      "description": "текст описания",
      "amount": 100.0,
      "type": "in",
      "counterparty": "краткое название"
    }}
  ]
}}

Правила:
- type: in=поступление/зачисление, out=списание. amount всегда положительное.
- counterparty — краткое понятное название ("Пятёрочка", "Яндекс GO", "М. Дария Руслановна")
- auth_code — код авторизации (6 цифр), если есть
- time — время операции в формате ЧЧ:ММ
- processing_date — дата обработки (вторая строка транзакции)
- account_owner — полное имя владельца счета как написано в выписке (ФИО)
- НЕ добавляй поле category"""

    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=32000,
        system=system_prompt,
        messages=[{
            "role": "user",
            "content": [
                {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": pdf_b64}},
                {"type": "text", "text": "Извлеки владельца счета и все транзакции."},
            ],
        }],
    ) as stream:
        text = stream.get_final_text()

    print(f"Claude response: {safe_preview(text, 500)}")

    start = text.find("{")
    if start == -1:
        raise ValueError(f"JSON не найден. Ответ: {safe_preview(text, 300)}")

    end = text.rfind("}")

    # Если } не найден — JSON обрезан, восстанавливаем
    if end == -1 or end < start:
        print("JSON обрезан — восстанавливаем по последнему ]")
        last_close = text.rfind("]")
        if last_close == -1:
            raise ValueError("Не удалось найти данные в ответе")
        json_str = text[start:last_close + 1]
    else:
        json_str = text[start:end + 1]

    try:
        data = json.loads(json_str)
        account_owner = data.get("account_owner", "")
        transactions = data.get("transactions", [])
        print(f"✅ Извлечено: владелец='{account_owner}', транзакций={len(transactions)}")
        return account_owner, transactions
    except json.JSONDecodeError:
        # Ещё раз пробуем восстановить по последнему ]
        last_close = json_str.rfind("]")
        if last_close == -1:
            raise ValueError("Не удалось распарсить ответ ИИ")
        try:
            data = json.loads(json_str[:last_close + 1])
            account_owner = data.get("account_owner", "")
            transactions = data.get("transactions", [])
            print(f"✅ Извлечено (после восстановления): владелец='{account_owner}', транзакций={len(transactions)}")
            return account_owner, transactions
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка парсинга JSON: {e}")


# ─────────────────────────────────────────────
# Фоновая обработка
# ─────────────────────────────────────────────

def process_pdf_async(dialog_id, file_id, fallback_url, uploader="", auth=None,
                      require_statement_check=False):
    try:
        # В чате «Платежи» сначала качаем файл и проверяем, выписка ли это.
        # Если нет (чек/платёжка/счёт) — молча выходим, ничего не пишем.
        if require_statement_check:
            try:
                pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url, auth=auth)
            except Exception as e:
                print(f"process_pdf_async: download failed (payment chat), silent: {e}")
                return
            if not is_bank_statement(pdf_bytes):
                print("Платежи: PDF не похож на выписку — пропускаем молча")
                return
            send_message(dialog_id, "📄 Получил выписку, начинаю обработку...")
        else:
            pdf_bytes = None

        # Проверяем все сервисы перед обработкой
        problems = check_all_services()
        if problems:
            msg = "⚠️ Обнаружены проблемы с сервисами:\n\n" + "\n".join(problems)
            msg += "\n\nПожалуйста проверьте оплату и настройки."
            send_message(dialog_id, msg)
            return

        if pdf_bytes is None:
            pdf_bytes = get_pdf_bytes(file_id, fallback_url=fallback_url, auth=auth)
        send_message(dialog_id, "🔍 Анализирую выписку через ИИ...")

        account_owner, transactions = extract_transactions(pdf_bytes)

        total_in  = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "in")
        total_out = sum(float(t.get("amount", 0) or 0) for t in transactions if t.get("type") == "out")

        send_message(dialog_id, "📊 Записываю в таблицу...")
        clarify_list, skipped = write_to_sheets(transactions, uploader=uploader, account_owner=account_owner)

        skipped_text = f"\n⚠️ Пропущено дублей: {skipped}" if skipped > 0 else ""
        # Основное сообщение
        send_message(
            dialog_id,
            f"✅ Готово! Найдено {len(transactions)} транзакций.\n"
            f"📈 Поступления: {total_in:,.2f} ₽\n"
            f"📉 Списания: {total_out:,.2f} ₽"

            f"{skipped_text}\n"
            f"👤 Владелец счета: {account_owner}\n\n"
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

def find_recent_pdf_in_chat(dialog_id, access_token=None, limit=10):
    """Ищет самый свежий PDF среди последних N сообщений чата.

    Используется когда бот @упомянули, но файл к самому сообщению не
    прикреплён — например, человек прислал файл, потом отдельной строкой
    написал «@ДДС Бот» (или сделал reply с упоминанием на чужой файл).

    Делает im.dialog.messages.get и в каждом сообщении смотрит params.FILE_ID
    + params.ATTACH. Если нашли файл (с расширением .pdf) — возвращаем
    {file_id, filename, url_download}, иначе None.
    """
    try:
        url = f"{BITRIX_WEBHOOK_URL}/im.dialog.messages.get.json"
        params = {"DIALOG_ID": dialog_id, "LIMIT": limit}
        if access_token:
            params["auth"] = access_token
        resp = requests.get(url, params=params, timeout=15)
        print(f"im.dialog.messages.get status={resp.status_code}")
        if resp.status_code != 200:
            return None
        body = resp.json()
        if body.get("error"):
            print(f"im.dialog.messages.get error: {safe_preview(resp.text, 200)}")
            return None
        result = body.get("result") or {}
        messages = result.get("messages") or []
        files_raw = result.get("files")

        # Bitrix может вернуть files как dict {file_id: info}, как список
        # [info, info, ...] или None. Нормализуем в dict по id.
        files_dict = {}
        if isinstance(files_raw, dict):
            for k, v in files_raw.items():
                files_dict[str(k)] = v
                if isinstance(v, dict) and v.get("id") is not None:
                    files_dict[str(v.get("id"))] = v
        elif isinstance(files_raw, list):
            for item in files_raw:
                if isinstance(item, dict):
                    fid = item.get("id") or item.get("ID")
                    if fid is not None:
                        files_dict[str(fid)] = item

        def msg_ts(m):
            return str(m.get("date") or m.get("DATE") or "")

        sorted_msgs = sorted(messages, key=msg_ts, reverse=True)

        for msg in sorted_msgs:
            mparams = msg.get("params") or {}
            file_ids = mparams.get("FILE_ID") or []
            if not isinstance(file_ids, list):
                file_ids = [file_ids]
            for fid in file_ids:
                fid_str = str(fid)
                file_info = files_dict.get(fid_str) or {}
                name = ""
                url_dl = None
                if isinstance(file_info, dict):
                    name = file_info.get("name") or file_info.get("NAME") or ""
                    url_dl = file_info.get("urlDownload") or file_info.get("URL_DOWNLOAD")
                # Если в files нет инфы — можно попробовать достать имя из ATTACH
                if not name:
                    attach = mparams.get("ATTACH") or []
                    if isinstance(attach, list):
                        for a in attach:
                            if isinstance(a, dict):
                                blocks = a.get("BLOCKS") or a.get("blocks") or []
                                for b in blocks:
                                    if isinstance(b, dict):
                                        files_block = b.get("FILE") or b.get("file") or []
                                        if isinstance(files_block, list):
                                            for fb in files_block:
                                                if isinstance(fb, dict):
                                                    cand_name = fb.get("NAME") or fb.get("name") or ""
                                                    if cand_name.lower().endswith(".pdf"):
                                                        name = cand_name
                                                        url_dl = url_dl or fb.get("LINK") or fb.get("link")
                if name.lower().endswith(".pdf"):
                    print(f"[recent-files] found PDF in msg id={msg.get('id')}: {name} (file_id={fid_str})")
                    return {
                        "file_id":      fid_str,
                        "filename":     name or "document.pdf",
                        "url_download": url_dl,
                    }
        print("[recent-files] no PDF found in last messages")
        return None
    except Exception as e:
        print(f"find_recent_pdf_in_chat error: {e}")
        return None


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

    # В чате «Платежи» бот не болтает: на текст/чеки/платёжки молчит. Но если
    # туда прислали именно банковскую выписку — обрабатываем её как обычно.
    # Тип PDF (выписка или нет) определяется в process_pdf_async через ИИ.
    is_pay = _is_payment_chat(dialog_id)

    message_text = str(data.get("data[PARAMS][MESSAGE]", "")).strip().lower()

    file_info = find_pdf_in_payload(data)
    file_id      = file_info.get("file_id")
    filename     = file_info.get("filename") or ""
    fallback_url = file_info.get("url_download")

    # Если в текущем сообщении PDF не найден, но бот упомянут или это reply —
    # ищем PDF в последних 10 сообщениях чата. Это покрывает кейсы:
    #   1) Файл прислан + потом отдельно «@ДДС Бот» (как в Telegram)
    #   2) Reply на чьё-то сообщение с PDF + @упоминание бота
    #   3) Любое @упоминание бота, когда в чате недавно был PDF
    if not (filename.lower().endswith(".pdf") and file_id):
        bot_mentioned = False
        # Битрикс кладёт упоминания как data[PARAMS][MENTIONED_LIST][BOT_ID]
        for k in data.keys():
            if k.startswith("data[PARAMS][MENTIONED_LIST]"):
                bot_mentioned = True
                break
        reply_id = data.get("data[PARAMS][REPLY_ID]") or ""
        if bot_mentioned or reply_id:
            print(f"[recent-files] bot mentioned (or reply: {reply_id}), "
                  f"searching for PDF in recent chat messages")
            auth_for_search = parse_auth_from_event(data)
            recent_pdf = find_recent_pdf_in_chat(
                dialog_id,
                access_token=(auth_for_search.get("access_token") or None),
                limit=10,
            )
            if recent_pdf:
                file_id = recent_pdf["file_id"]
                filename = recent_pdf["filename"]
                fallback_url = recent_pdf["url_download"]

    if filename.lower().endswith(".pdf") and file_id:
        uploader = extract_uploader_name(data)
        # Достаём auth-токен пользователя из события — нужен для скачивания
        # файлов, загруженных НЕ владельцем вебхука (см. get_pdf_bytes).
        auth = parse_auth_from_event(data)
        # В «Платежах» не анонсируем приём сразу — сначала ИИ проверит, выписка
        # ли это (иначе на чек бот бы написал «Получил PDF…»).
        if not is_pay:
            send_message(dialog_id, "📄 Получил PDF, начинаю обработку...")
        thread = threading.Thread(
            target=process_pdf_async,
            args=(dialog_id, file_id, fallback_url, uploader, auth),
            kwargs={"require_statement_check": is_pay},
            daemon=True,
        )
        thread.start()

    elif is_pay:
        # В чате «Платежи» на текст/чеки/прочее без выписки — молчим.
        return jsonify({"result": "ok", "skipped": "payment_chat_no_statement"})

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


@app.route("/chats", methods=["GET"])
def chats_route():
    """Список диалогов/чатов с их DIALOG_ID — чтобы найти PAYMENT_CHAT_ID.

    Тянет im.recent.get через вебхук и выводит таблицу: название → ID.
    Групповые чаты имеют ID вида chatXXX — его и нужно вписать в Railway.
    """
    try:
        resp = bitrix_post("im.recent.get", {}, timeout=20)
        items = (resp.json().get("result") or {}) if resp.status_code == 200 else {}
        if isinstance(items, dict):
            items = items.get("items", items)
        rows = ""
        for it in (items or []):
            chat = it.get("chat") or {}
            dialog_id = it.get("id") or chat.get("dialog_id") or ""
            title = it.get("title") or chat.get("name") or "—"
            kind = "👥 чат" if str(dialog_id).startswith("chat") else "👤 ЛС"
            rows += (
                f'<tr><td>{kind}</td><td>{title}</td>'
                f'<td><code>{dialog_id}</code></td></tr>'
            )
        if not rows:
            rows = '<tr><td colspan="3">Чатов не найдено (или нет прав im).</td></tr>'
        html = f"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<title>Чаты · поиск PAYMENT_CHAT_ID</title>
<style>body{{font-family:system-ui,sans-serif;max-width:760px;margin:30px auto;padding:16px;}}
table{{border-collapse:collapse;width:100%;}}td,th{{border:1px solid #ddd;padding:8px 10px;text-align:left;font-size:14px;}}
th{{background:#f4f6f8;}}code{{background:#eef2f4;padding:2px 6px;border-radius:5px;}}</style></head>
<body><h2>Чаты портала</h2>
<p>Найди нужный <b>общий чат</b> и впиши его <code>ID</code> (вида <code>chat123</code>)
в переменную <b>PAYMENT_CHAT_ID</b> в Railway.</p>
<table><tr><th>Тип</th><th>Название</th><th>DIALOG_ID</th></tr>{rows}</table></body></html>"""
        return Response(html, mimetype="text/html; charset=utf-8")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/init-sheets", methods=["GET"])
def init_sheets_route():
    try:
        init_sheets()
        return jsonify({"ok": True, "message": "Таблица инициализирована"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ─────────────────────────────────────────────
# Заявки на оплату (Local Application)
# ─────────────────────────────────────────────

def bitrix_disk_post(method_name, payload, timeout=30):
    url = f"{BITRIX_DISK_WEBHOOK_URL}/{method_name}.json"
    return requests.post(url, json=payload, timeout=timeout)


def fetch_active_users():
    """Список активных сотрудников для выпадающего списка «Плательщик».

    Возвращает список dict: {"id": int, "name": "Имя Фамилия"}.
    Постранично тянет user.get (Битрикс отдаёт по 50 за раз).
    """
    users = []
    start = 0
    try:
        while True:
            resp = bitrix_post(
                "user.get",
                {"FILTER": {"ACTIVE": True}, "start": start},
                timeout=20,
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            for u in data.get("result", []) or []:
                name = _combine_first_last(u.get("NAME"), u.get("LAST_NAME"))
                if not name:
                    name = (u.get("EMAIL") or f"ID {u.get('ID')}").strip()
                users.append({"id": int(u["ID"]), "name": name})
            nxt = data.get("next")
            if nxt is None:
                break
            start = nxt
    except Exception as e:
        print(f"fetch_active_users error: {e}")
    users.sort(key=lambda x: x["name"].lower())
    return users


def fetch_requester(access_token, client_endpoint):
    """ФИО + ID сотрудника, открывшего приложение (по токену Local App)."""
    if not access_token or not client_endpoint:
        return {"id": None, "name": ""}
    try:
        resp = requests.post(
            f"{client_endpoint.rstrip('/')}/user.current.json",
            data={"auth": access_token},
            timeout=15,
        )
        if resp.status_code == 200:
            u = resp.json().get("result") or {}
            if u:
                name = _combine_first_last(u.get("NAME"), u.get("LAST_NAME"))
                return {"id": u.get("ID"), "name": name or "Сотрудник"}
    except Exception as e:
        print(f"fetch_requester error: {e}")
    return {"id": None, "name": ""}


def resolve_user_name(user_id):
    """ФИО сотрудника по его ID (через user.get). Фолбэк — 'ID N'."""
    if not user_id:
        return ""
    try:
        r = bitrix_post("user.get", {"ID": user_id}, timeout=10)
        res = (r.json().get("result") or []) if r.status_code == 200 else []
        if res:
            name = _combine_first_last(res[0].get("NAME"), res[0].get("LAST_NAME"))
            if name:
                return name
    except Exception as e:
        print(f"resolve_user_name error: {e}")
    return f"ID {user_id}"


def get_payment_categories():
    """Список категорий для формы из листа «Категории заявок».

    Фолбэк на DDS_CATEGORIES, если лист пуст или недоступен.
    """
    try:
        service = get_sheets_service()
        rows = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Категории заявок!A2:A"
        ).execute().get("values", [])
        cats = [r[0].strip() for r in rows if r and r[0].strip()]
        if cats:
            return cats
    except Exception as e:
        print(f"get_payment_categories error: {e}")
    return list(PAYMENT_CATEGORIES_DEFAULT)


def reset_payment_categories():
    """Перезаписывает лист категорий стандартным списком PAYMENT_CATEGORIES_DEFAULT."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID, range="Категории заявок!A2:A",
        ).execute()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range="Категории заявок!A2",
            valueInputOption="RAW",
            body={"values": [[c] for c in PAYMENT_CATEGORIES_DEFAULT]},
        ).execute()
    except Exception as e:
        print(f"reset_payment_categories error: {e}")


def add_payment_category(name):
    name = (name or "").strip()
    if not name:
        return
    try:
        if name in get_payment_categories():
            return
        service = get_sheets_service()
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Категории заявок!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [[name]]},
        ).execute()
    except Exception as e:
        print(f"add_payment_category error: {e}")


def delete_payment_category(name):
    """Удаляет категорию: перезаписывает столбец A оставшимися значениями."""
    name = (name or "").strip()
    if not name:
        return
    try:
        remaining = [c for c in get_payment_categories() if c != name]
        service = get_sheets_service()
        # Чистим всё под заголовком и пишем заново
        service.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID, range="Категории заявок!A2:A",
        ).execute()
        if remaining:
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range="Категории заявок!A2",
                valueInputOption="RAW",
                body={"values": [[c] for c in remaining]},
            ).execute()
    except Exception as e:
        print(f"delete_payment_category error: {e}")


def upload_invoice_to_disk(filename, content_bytes):
    """Загружает файл счёта на Bitrix-диск, возвращает ссылку для просмотра.

    Использует первое доступное хранилище (disk.storage.getlist) и
    кладёт файл в его корень через disk.storage.uploadfile.
    При любой ошибке возвращает "" — заявка всё равно создастся.
    """
    if not content_bytes:
        return ""
    try:
        resp = bitrix_disk_post("disk.storage.getlist", {})
        storages = (resp.json().get("result") or []) if resp.status_code == 200 else []
        if not storages:
            print("upload_invoice_to_disk: нет доступных хранилищ диска")
            return ""
        storage_id = storages[0]["ID"]

        b64 = base64.b64encode(content_bytes).decode("ascii")
        up = bitrix_disk_post(
            "disk.storage.uploadfile",
            {
                "id": storage_id,
                "data": {"NAME": filename},
                "fileContent": [filename, b64],
                "generateUniqueName": True,
            },
            timeout=60,
        )
        if up.status_code != 200:
            print(f"upload_invoice_to_disk status={up.status_code} body={safe_preview(up.text,300)}")
            return ""
        f = up.json().get("result") or {}
        return f.get("DETAIL_URL") or f.get("DOWNLOAD_URL") or ""
    except Exception as e:
        print(f"upload_invoice_to_disk error: {e}")
        return ""


def append_payment_request_row(row):
    """Добавляет строку заявки на оплату в лист «Заявки»."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Заявки!A:O",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as e:
        print(f"append_payment_request_row error: {e}")


# Колонки листа «Заявки» (0-based): L=Статус(11), M=ID заявки(12),
# N=ID заявителя(13), O=ID сообщения(14).
def find_payment_row_by_rid(rid):
    """Ищет заявку по ID (колонка M). Возвращает (row_number, values) или (None, None).

    row_number — 1-based номер строки в листе (для адресации диапазонов).
    """
    rid = (rid or "").strip()
    if not rid:
        return None, None
    try:
        service = get_sheets_service()
        rows = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Заявки!A2:O"
        ).execute().get("values", [])
        for i, r in enumerate(rows):
            if len(r) > 12 and (r[12] or "").strip() == rid:
                return i + 2, r  # +2: строки начинаются с 1, данные — со 2-й
    except Exception as e:
        print(f"find_payment_row_by_rid error: {e}")
    return None, None


def set_payment_status(row_number, status):
    """Проставляет статус (колонка L) для заявки в указанной строке."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"Заявки!L{row_number}",
            valueInputOption="RAW",
            body={"values": [[status]]},
        ).execute()
    except Exception as e:
        print(f"set_payment_status error: {e}")


def _render_payment_form(users, categories, error=None):
    """HTML-форма создания заявки на оплату (открывается как Local App)."""
    cat_options = "\n".join(
        f'<option value="{c}">{c}</option>' for c in categories
    )
    user_options = "\n".join(
        f'<option value="{u["id"]}">{u["name"]}</option>' for u in users
    )
    # Плательщики — только из разрешённого списка имён (по подстроке в ФИО).
    allowed_payers = []
    seen_payer_ids = set()
    for name_part in PAYMENT_PAYER_NAMES:
        np = name_part.lower()
        for u in users:
            if np in u["name"].lower() and u["id"] not in seen_payer_ids:
                allowed_payers.append(u)
                seen_payer_ids.add(u["id"])
    if len(allowed_payers) == 1:
        # Один плательщик — заблокированное поле + скрытый payer_id.
        p = allowed_payers[0]
        payer_field = (
            f'<input type="text" value="{p["name"]}" readonly '
            f'style="background:#f4f6f8;cursor:not-allowed;">'
            f'<input type="hidden" name="payer_id" value="{p["id"]}">'
        )
    elif len(allowed_payers) >= 2:
        # Несколько разрешённых плательщиков — выбор из них.
        opts = "\n".join(
            f'<option value="{u["id"]}">{u["name"]}</option>' for u in allowed_payers
        )
        payer_field = (
            '<select name="payer_id" required>'
            '<option value="" disabled selected>— выберите плательщика —</option>'
            f'{opts}</select>'
        )
    else:
        # Никто из списка не найден — фолбэк на полный список сотрудников.
        opts = "\n".join(
            f'<option value="{u["id"]}">{u["name"]}</option>' for u in users
        )
        payer_field = (
            '<select name="payer_id" required>'
            '<option value="" disabled selected>— выберите сотрудника —</option>'
            f'{opts}</select>'
        )
    err_html = (
        f'<div class="err">⚠️ {error}</div>' if error else ""
    )
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Платежи · заявка на оплату</title>
<script src="//api.bitrix24.com/api/v1/"></script>
<style>
  :root {{
    --bx-blue:#2066b0; --bx-blue-dark:#17518f; --bx-bg:#eef2f4;
    --bx-border:#dfe5ec; --bx-text:#1e2734; --bx-muted:#7d8a99;
  }}
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Helvetica Neue',Arial,system-ui,sans-serif; margin:0;
         padding:18px; background:var(--bx-bg); color:var(--bx-text); }}
  .card {{ max-width:600px; margin:0 auto; background:#fff; border:1px solid var(--bx-border);
           border-radius:14px; overflow:hidden; box-shadow:0 2px 10px rgba(31,49,71,.06); }}
  .head {{ display:flex; align-items:center; gap:12px; padding:20px 24px;
           background:linear-gradient(135deg,var(--bx-blue),var(--bx-blue-dark)); color:#fff; }}
  .head .ic {{ font-size:26px; line-height:1; }}
  .head h1 {{ font-size:19px; margin:0; font-weight:600; }}
  .head .tag {{ font-size:12px; opacity:.85; margin-top:2px; }}
  .body {{ padding:22px 24px 26px; }}
  .who {{ font-size:13px; color:var(--bx-muted); margin-bottom:14px;
          background:var(--bx-bg); padding:8px 12px; border-radius:8px; }}
  label {{ display:block; font-size:13px; font-weight:600; margin:16px 0 6px; }}
  .hint {{ font-weight:400; color:var(--bx-muted); }}
  input, select, textarea {{ width:100%; padding:11px 13px; font-size:15px; color:var(--bx-text);
           border:1px solid var(--bx-border); border-radius:9px; background:#fff;
           transition:border-color .15s, box-shadow .15s; }}
  input:focus, select:focus, textarea:focus {{ outline:none; border-color:var(--bx-blue);
           box-shadow:0 0 0 3px rgba(32,102,176,.12); }}
  textarea {{ resize:vertical; min-height:62px; }}
  .row {{ display:flex; gap:14px; }}
  .row > div {{ flex:1; }}
  .file {{ border:1px dashed var(--bx-border); border-radius:9px; padding:11px 13px;
           background:var(--bx-bg); }}
  button {{ width:100%; margin-top:24px; padding:14px; font-size:16px; font-weight:600;
           color:#fff; background:var(--bx-blue); border:0; border-radius:10px; cursor:pointer;
           transition:background .15s; }}
  button:hover {{ background:var(--bx-blue-dark); }}
  button:disabled {{ background:#9cb6d4; cursor:default; }}
  .err {{ background:#fdecec; color:#c0392b; padding:11px 13px; border-radius:9px;
          font-size:14px; margin-bottom:14px; }}
  .req {{ color:#c0392b; }}
  /* Срочный платёж — «горит» красным */
  select.urgent {{ border-color:#e0392b; color:#c0392b; background:#fdecec;
           font-weight:700; box-shadow:0 0 0 3px rgba(224,57,43,.12); }}
  select.urgent:focus {{ border-color:#e0392b; box-shadow:0 0 0 3px rgba(224,57,43,.2); }}
  @keyframes urgPulse {{ 0%,100%{{box-shadow:0 0 0 3px rgba(224,57,43,.12);}}
           50%{{box-shadow:0 0 0 5px rgba(224,57,43,.28);}} }}
  select.urgent {{ animation:urgPulse 1.2s ease-in-out infinite; }}
</style>
</head>
<body>
<div class="card">
  <div class="head">
    <div class="ic">💳</div>
    <div>
      <h1>Платежи</h1>
      <div class="tag">Заявка на оплату</div>
    </div>
  </div>
  <div class="body">
    {err_html}
    <form method="POST" action="/pay/submit" enctype="multipart/form-data"
          onsubmit="var b=this.querySelector('button');b.disabled=true;b.textContent='Отправляем…';">

      <label>Заявитель <span class="req">*</span>
        <span class="hint" id="reqAutoNote" style="display:none">— определён автоматически</span>
      </label>
      <!-- Видимый select заблокирован: значение определяется автоматически (BX24).
           Реально на сервер уходит скрытое поле requester_id. -->
      <select id="requesterSelect" disabled style="background:#f4f6f8;cursor:not-allowed;">
        <option value="" selected>— определяется автоматически —</option>
        {user_options}
      </select>
      <input type="hidden" name="requester_id" id="requesterIdHidden">
      <div class="hint" id="reqManualNote" style="display:none;margin-top:6px;">
        Не удалось определить автоматически — выберите себя из списка.
      </div>

      <div class="row">
        <div>
          <label>Сумма, ₽ <span class="req">*</span></label>
          <input type="text" name="amount" inputmode="decimal" placeholder="15 000" required>
        </div>
        <div>
          <label>Срок оплаты</label>
          <input type="date" name="due_date">
        </div>
      </div>

      <label>Срочность платежа <span class="req">*</span></label>
      <select name="urgency" id="urgencySelect" required onchange="syncUrgency()">
        <option value="Не срочный" selected>🟢 Не срочный</option>
        <option value="Срочный">🔴 СРОЧНЫЙ — оплатить как можно скорее</option>
      </select>

      <label>Категория <span class="req">*</span></label>
      <select name="category" required>{cat_options}</select>

      <label>Получатель <span class="req">*</span></label>
      <input type="text" name="recipient" placeholder="Кому платим: название / ФИО / ИП" required>

      <label>Реквизиты</label>
      <textarea name="requisites" placeholder="Счёт / карта / ИНН / БИК"></textarea>

      <label>Назначение платежа <span class="req">*</span>
        <span class="hint">— (номер счёта, назначение, дата)</span>
      </label>
      <textarea name="purpose" placeholder="Напр.: счёт №125, закуп ткани, 05.06.2026" required></textarea>

      <label>Кто оплачивает <span class="req">*</span> <span class="hint">— получит уведомление в чат</span></label>
      {payer_field}

      <label>Файл счёта <span class="hint">(PDF или фото)</span></label>
      <div class="file"><input type="file" name="invoice" accept=".pdf,.jpg,.jpeg,.png" style="border:0;padding:0;background:transparent;"></div>

      <button type="submit">Создать заявку</button>
    </form>
  </div>
</div>
<script>
  // Срочность: красная подсветка «горит», когда выбран срочный платёж.
  function syncUrgency() {{
    var u = document.getElementById('urgencySelect');
    if (u.value === 'Срочный') {{ u.classList.add('urgent'); }}
    else {{ u.classList.remove('urgent'); }}
  }}
  syncUrgency();

  var sel    = document.getElementById('requesterSelect');
  var hidden = document.getElementById('requesterIdHidden');

  // Фолбэк: форма открыта вне Битрикса — разблокируем выбор заявителя вручную.
  function enableManual() {{
    sel.disabled = false;
    sel.style.background = '#fff';
    sel.style.cursor = 'pointer';
    sel.setAttribute('required', 'required');
    document.getElementById('reqManualNote').style.display = 'block';
    sel.addEventListener('change', function() {{ hidden.value = sel.value; }});
  }}

  // Автоопределение через BX24 (когда форма открыта внутри Битрикса).
  try {{
    if (window.BX24) {{
      var done = false;
      BX24.init(function() {{
        try {{ BX24.fitWindow(); }} catch(e) {{}}
        try {{
          BX24.callMethod('user.current', {{}}, function(res) {{
            if (res.error()) {{ if (!done) enableManual(); return; }}
            var u = res.data();
            if (u && u.ID) {{
              if (!sel.querySelector('option[value="' + u.ID + '"]')) {{
                var o = document.createElement('option');
                o.value = u.ID;
                o.textContent = ((u.NAME||'') + ' ' + (u.LAST_NAME||'')).trim() || ('ID ' + u.ID);
                sel.appendChild(o);
              }}
              sel.value = u.ID;          // показываем имя (поле остаётся заблокированным)
              hidden.value = u.ID;        // именно это уходит на сервер
              document.getElementById('reqAutoNote').style.display = 'inline';
              done = true;
            }} else if (!done) {{ enableManual(); }}
          }});
        }} catch(e) {{ enableManual(); }}
      }});
      // страховка: если BX24 не ответил за 4 сек — даём выбрать вручную
      setTimeout(function() {{ if (!done && !hidden.value) enableManual(); }}, 4000);
    }} else {{
      enableManual();
    }}
  }} catch(e) {{ enableManual(); }}
</script>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


def _render_payment_result(ok, message):
    color = "#16a34a" if ok else "#b42318"
    icon = "✅" if ok else "❌"
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Заявка на оплату</title></head>
<body style="font-family:system-ui,sans-serif;background:#f4f6f8;margin:0;padding:24px;">
<div style="max-width:480px;margin:40px auto;background:#fff;border-radius:12px;padding:32px;text-align:center;box-shadow:0 1px 4px rgba(0,0,0,.08);">
  <div style="font-size:48px;">{icon}</div>
  <h2 style="color:{color};margin:12px 0;">{message}</h2>
  <a href="/pay" style="display:inline-block;margin-top:8px;color:#2563eb;text-decoration:none;font-weight:600;">← Создать ещё одну</a>
</div>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


@app.route("/pay", methods=["GET", "POST"])
def payment_form_route():
    """Local Application: форма создания заявки на оплату.

    Заявитель определяется автоматически на клиенте через BX24
    (user.current), поэтому серверу auth-токен не нужен — достаточно
    отдать список сотрудников (фолбэк-выбор) и актуальные категории.
    """
    users = fetch_active_users()
    categories = get_payment_categories()
    return _render_payment_form(users, categories)


@app.route("/pay/submit", methods=["POST"])
def payment_submit_route():
    """Приём заявки: загрузка счёта, запись в Sheets, уведомление в чат."""
    try:
        form = request.form
        amount       = (form.get("amount") or "").strip()
        category     = (form.get("category") or "").strip()
        recipient    = (form.get("recipient") or "").strip()
        requisites   = (form.get("requisites") or "").strip()
        purpose      = (form.get("purpose") or "").strip()
        due_date     = (form.get("due_date") or "").strip()
        urgency      = (form.get("urgency") or "Не срочный").strip()
        payer_id     = (form.get("payer_id") or "").strip()
        requester_id = (form.get("requester_id") or "").strip()
        is_urgent    = urgency == "Срочный"

        if not (amount and category and recipient and purpose
                and payer_id and requester_id):
            return _render_payment_result(False, "Заполнены не все обязательные поля")

        payer_name     = resolve_user_name(payer_id)
        requester_name = resolve_user_name(requester_id)

        # Файл счёта → Bitrix Drive
        file_link = ""
        f = request.files.get("invoice")
        if f and f.filename:
            content = f.read()
            file_link = upload_invoice_to_disk(f.filename, content)

        # Уникальный ID заявки — связывает строку таблицы, сообщение в чате
        # и кнопку «Отменить».
        rid = uuid.uuid4().hex[:12]

        # Уведомление в чат «Платежи» с упоминанием плательщика.
        # Все заявки (и Чермен, и Анастасия) идут в один общий PAYMENT_CHAT_ID.
        message_id = None
        if PAYMENT_CHAT_ID:
            header = ("🔴 [B]СРОЧНАЯ заявка на оплату[/B] 🔴" if is_urgent
                      else "🧾 [B]Новая заявка на оплату[/B]")
            lines = [
                header,
                f"👤 Заявитель: {requester_name}",
                f"💰 Сумма: {amount}",
                f"📂 Категория: {category}",
                f"🚦 Срочность: {'🔴 СРОЧНЫЙ' if is_urgent else '🟢 Не срочный'}",
                f"🏦 Получатель: {recipient}",
                f"💳 Реквизиты: {requisites or '—'}",
                f"📝 Назначение: {purpose}",
                f"📅 Срок оплаты: {due_date or '—'}",
            ]
            if file_link:
                lines.append(f"📎 Счёт: {file_link}")
            lines.append("")
            lines.append(f"[USER={payer_id}]{payer_name}[/USER], нужно оплатить 🙏")
            # Кнопка отмены: открывает страницу /pay/cancel, где заявитель
            # подтверждает отмену (проверка автора через BX24).
            keyboard = [{
                "TEXT": "❌ Отменить заявку",
                "LINK": f"{APP_PUBLIC_URL}/pay/cancel?rid={rid}",
                "BG_COLOR": "#eb5757",
                "TEXT_COLOR": "#ffffff",
                "DISPLAY": "LINE",
            }]
            message_id = send_message(PAYMENT_CHAT_ID, "\n".join(lines), keyboard=keyboard)
        else:
            print("PAYMENT_CHAT_ID не задан — уведомление в чат не отправлено")

        # Запись в Google Sheets (вместе с техн. полями для отмены).
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        append_payment_request_row([
            now, requester_name, category, amount, recipient, requisites or "—",
            purpose, due_date or "—", urgency, payer_name, file_link or "—", "Новая",
            rid, requester_id, str(message_id or ""),
        ])

        return _render_payment_result(True, "Заявка создана и отправлена на оплату")
    except Exception as e:
        print(f"payment_submit_route error: {e}")
        return _render_payment_result(False, "Не удалось создать заявку. Попробуйте ещё раз.")


@app.route("/pay/cancel", methods=["GET"])
def payment_cancel_route():
    """Страница отмены заявки (открывается по кнопке в чате).

    Определяет текущего пользователя через BX24 (user.current) и отправляет
    подтверждение на /pay/cancel/confirm. Отменить может только заявитель.
    """
    rid = (request.args.get("rid") or "").strip()
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Отмена заявки</title>
<script src="//api.bitrix24.com/api/v1/"></script>
<style>
  body {{ font-family:system-ui,Arial,sans-serif; background:#eef2f4; margin:0; padding:24px; color:#1e2734; }}
  .card {{ max-width:440px; margin:32px auto; background:#fff; border-radius:14px;
           padding:28px 24px; text-align:center; box-shadow:0 2px 10px rgba(31,49,71,.08); }}
  .ic {{ font-size:46px; }}
  h2 {{ margin:14px 0 6px; font-size:19px; }}
  p {{ color:#5b6b7d; font-size:14px; margin:8px 0; }}
  .spin {{ color:#7d8a99; }}
</style></head>
<body>
<div class="card">
  <div class="ic" id="ic">⏳</div>
  <h2 id="title">Отмена заявки…</h2>
  <p id="msg" class="spin">Определяем пользователя…</p>
</div>
<script>
  var RID = {json.dumps(rid)};
  function show(ic, title, msg) {{
    document.getElementById('ic').textContent = ic;
    document.getElementById('title').textContent = title;
    document.getElementById('msg').textContent = msg;
  }}
  function doConfirm(uid) {{
    var fd = new FormData();
    fd.append('rid', RID);
    fd.append('user_id', uid || '');
    fetch('/pay/cancel/confirm', {{ method:'POST', body:fd }})
      .then(function(r) {{ return r.json(); }})
      .then(function(d) {{
        if (d.ok) {{ show('✅', 'Заявка отменена', d.message || ''); }}
        else {{ show('⚠️', 'Не отменено', d.message || 'Ошибка'); }}
        try {{ if (window.BX24) BX24.fitWindow(); }} catch(e) {{}}
      }})
      .catch(function() {{ show('⚠️', 'Ошибка', 'Не удалось связаться с сервером'); }});
  }}
  try {{
    if (window.BX24) {{
      BX24.init(function() {{
        try {{ BX24.fitWindow(); }} catch(e) {{}}
        BX24.callMethod('user.current', {{}}, function(res) {{
          if (res.error()) {{ show('⚠️','Ошибка','Не удалось определить пользователя'); return; }}
          var u = res.data();
          doConfirm(u && u.ID ? u.ID : '');
        }});
      }});
    }} else {{
      show('⚠️','Откройте из Битрикса','Кнопку отмены нужно нажимать внутри Битрикс24.');
    }}
  }} catch(e) {{ show('⚠️','Ошибка', String(e)); }}
</script>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


@app.route("/pay/cancel/confirm", methods=["POST"])
def payment_cancel_confirm_route():
    """Выполняет отмену заявки: проверяет автора, ставит статус, правит сообщение."""
    try:
        rid = (request.form.get("rid") or "").strip()
        user_id = (request.form.get("user_id") or "").strip()
        if not rid:
            return jsonify({"ok": False, "message": "Не указан ID заявки"})

        row_number, values = find_payment_row_by_rid(rid)
        if not row_number:
            return jsonify({"ok": False, "message": "Заявка не найдена"})

        status        = (values[11] if len(values) > 11 else "").strip()
        requester_id  = (values[13] if len(values) > 13 else "").strip()
        message_id    = (values[14] if len(values) > 14 else "").strip()
        requester_nm  = (values[1] if len(values) > 1 else "").strip()
        amount        = (values[3] if len(values) > 3 else "").strip()
        recipient     = (values[4] if len(values) > 4 else "").strip()
        category      = (values[2] if len(values) > 2 else "").strip()

        if status == "Отменена":
            return jsonify({"ok": False, "message": "Заявка уже отменена"})

        # Отменить может только автор заявки.
        if not user_id or str(user_id) != str(requester_id):
            return jsonify({
                "ok": False,
                "message": f"Отменить может только заявитель ({requester_nm})",
            })

        set_payment_status(row_number, "Отменена")

        # Правим сообщение в чате — помечаем отменённым и убираем кнопку.
        if message_id:
            cancelled = "\n".join([
                "❌ [B]ЗАЯВКА ОТМЕНЕНА[/B]",
                f"👤 Заявитель: {requester_nm}",
                f"💰 Сумма: {amount}",
                f"📂 Категория: {category}",
                f"🏦 Получатель: {recipient}",
                "",
                "[I]Отменена заявителем[/I]",
            ])
            update_bot_message(message_id, cancelled)

        # Отдельное уведомление в чат «Платежи» об отмене.
        if PAYMENT_CHAT_ID:
            send_message(PAYMENT_CHAT_ID, "\n".join([
                "❌ [B]Заявка отменена[/B]",
                f"👤 Заявитель: {requester_nm}",
                f"💰 Сумма: {amount}",
                f"🏦 Получатель: {recipient}",
                f"📂 Категория: {category}",
            ]))

        return jsonify({"ok": True, "message": "Заявка помечена как отменённая"})
    except Exception as e:
        print(f"payment_cancel_confirm_route error: {e}")
        return jsonify({"ok": False, "message": "Внутренняя ошибка"})


# ─────────────────────────────────────────────
# Заказы на производство (/order) — форма, согласование, CRM
# ─────────────────────────────────────────────

def bitrix_crm_post(method_name, payload, timeout=20):
    """POST на CRM-вебхук Битрикса (scope `crm`)."""
    url = f"{BITRIX_CRM_WEBHOOK_URL}/{method_name}.json"
    return requests.post(url, json=payload, timeout=timeout)


def resolve_approver(users=None):
    """Согласующий (Алан) среди активных сотрудников — по подстроке в ФИО.

    Возвращает {"id": int|None, "name": str}. Если не нашли — id=None,
    name = первое из PRODUCTION_APPROVER_NAMES (для отображения).
    """
    if users is None:
        users = fetch_active_users()
    for name_part in PRODUCTION_APPROVER_NAMES:
        np = name_part.lower()
        for u in users:
            if np in u["name"].lower():
                return {"id": u["id"], "name": u["name"]}
    return {"id": None,
            "name": PRODUCTION_APPROVER_NAMES[0] if PRODUCTION_APPROVER_NAMES else "Согласующий"}


def append_order_row(row):
    """Добавляет строку заказа на производство в лист «Заказы»."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range="Заказы!A:U",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as e:
        print(f"append_order_row error: {e}")


# Колонки листа «Заказы» (0-based): P=Статус(15), Q=ID заказа(16),
# R=ID заявителя(17), S=ID согласующего(18), T=ID сообщения(19), U=CRM(20).
def find_order_row_by_rid(rid):
    """Ищет заказ по ID (колонка Q). Возвращает (row_number, values) или (None, None)."""
    rid = (rid or "").strip()
    if not rid:
        return None, None
    try:
        service = get_sheets_service()
        rows = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range="Заказы!A2:U"
        ).execute().get("values", [])
        for i, r in enumerate(rows):
            if len(r) > 16 and (r[16] or "").strip() == rid:
                return i + 2, r  # +2: строки с 1, данные — со 2-й
    except Exception as e:
        print(f"find_order_row_by_rid error: {e}")
    return None, None


def set_order_status(row_number, status):
    """Проставляет статус (колонка P) для заказа в указанной строке."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"Заказы!P{row_number}",
            valueInputOption="RAW",
            body={"values": [[status]]},
        ).execute()
    except Exception as e:
        print(f"set_order_status error: {e}")


def set_order_crm_link(row_number, link):
    """Записывает ссылку/ID элемента CRM (колонка U)."""
    try:
        service = get_sheets_service()
        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"Заказы!U{row_number}",
            valueInputOption="RAW",
            body={"values": [[link]]},
        ).execute()
    except Exception as e:
        print(f"set_order_crm_link error: {e}")


def _order_title(order):
    title = f"Заказ: {order.get('model') or '—'}"
    if order.get("quantity"):
        title += f" · {order['quantity']} шт"
    return title


def _order_details_text(order):
    """Полные детали заказа одним текстом — для COMMENTS сделки / таймлайна."""
    return "\n".join([
        "Заказ на производство (согласован):",
        f"Заявитель: {order.get('requester') or '—'}",
        f"Дата заказа: {order.get('order_date') or '—'}",
        f"Артикул / модель: {order.get('model') or '—'}",
        f"Размерный ряд и количество: {order.get('sizes') or '—'}",
        f"Цвет: {order.get('color') or '—'}",
        f"Ткань (состав): {order.get('fabric') or '—'}",
        f"Фурнитура: {order.get('accessories') or '—'}",
        f"Общий тираж: {order.get('quantity') or '—'}",
        f"Подрядчик / цех: {order.get('contractor') or '—'}",
        f"Срок (дедлайн): {order.get('deadline') or '—'}",
        f"Срочность: {order.get('urgency') or '—'}",
        f"Образец нужен: {order.get('sample') or '—'}",
        f"Файлы: {order.get('files') or '—'}",
        f"Комментарий: {order.get('comment') or '—'}",
    ])


def _crm_create_deal(order):
    """Создаёт СДЕЛКУ в воронке «Заказы на производство» на нужной стадии.

    Полные детали кладём в поле COMMENTS сделки. Возвращает ссылку на сделку.
    """
    if not (PRODUCTION_DEAL_CATEGORY_ID or PRODUCTION_DEAL_STAGE_ID):
        print("_crm_create_deal: не задан PRODUCTION_DEAL_CATEGORY_ID/STAGE_ID — CRM-шаг пропущен")
        return ""
    try:
        fields = {
            "TITLE":    _order_title(order),
            "COMMENTS": _order_details_text(order),
            "OPENED":   "Y",
        }
        if PRODUCTION_DEAL_CATEGORY_ID:
            fields["CATEGORY_ID"] = PRODUCTION_DEAL_CATEGORY_ID
        if PRODUCTION_DEAL_STAGE_ID:
            fields["STAGE_ID"] = PRODUCTION_DEAL_STAGE_ID

        resp = bitrix_crm_post("crm.deal.add", {"fields": fields}, timeout=20)
        if resp.status_code != 200:
            print(f"crm.deal.add status={resp.status_code} body={safe_preview(resp.text,300)}")
            return ""
        data = resp.json()
        if data.get("error"):
            print(f"crm.deal.add error: {safe_preview(data,300)}")
            return ""
        deal_id = data.get("result")
        if not deal_id:
            return ""
        portal = BITRIX_WEBHOOK_URL.split("/rest/")[0]
        return f"{portal}/crm/deal/details/{deal_id}/"
    except Exception as e:
        print(f"_crm_create_deal error: {e}")
        return ""


def _crm_create_spa(order):
    """Создаёт элемент смарт-процесса CRM. Детали — комментарием в таймлайн."""
    if not PRODUCTION_SPA_ENTITY_TYPE_ID:
        print("_crm_create_spa: не задан PRODUCTION_SPA_ENTITY_TYPE_ID — CRM-шаг пропущен")
        return ""
    try:
        fields = {"title": _order_title(order)}
        if PRODUCTION_SPA_CATEGORY_ID:
            fields["categoryId"] = PRODUCTION_SPA_CATEGORY_ID
        if PRODUCTION_SPA_STAGE_ID:
            fields["stageId"] = PRODUCTION_SPA_STAGE_ID

        resp = bitrix_crm_post(
            "crm.item.add",
            {"entityTypeId": PRODUCTION_SPA_ENTITY_TYPE_ID, "fields": fields},
            timeout=20,
        )
        if resp.status_code != 200:
            print(f"crm.item.add status={resp.status_code} body={safe_preview(resp.text,300)}")
            return ""
        data = resp.json()
        if data.get("error"):
            print(f"crm.item.add error: {safe_preview(data,300)}")
            return ""
        item = (data.get("result") or {}).get("item") or {}
        item_id = item.get("id")
        if not item_id:
            return ""
        try:
            bitrix_crm_post(
                "crm.timeline.comment.add",
                {"fields": {
                    "ENTITY_ID": item_id,
                    "ENTITY_TYPE": f"DYNAMIC_{PRODUCTION_SPA_ENTITY_TYPE_ID}",
                    "COMMENT": _order_details_text(order),
                }},
                timeout=20,
            )
        except Exception as e:
            print(f"crm.timeline.comment.add error (ignored): {e}")
        portal = BITRIX_WEBHOOK_URL.split("/rest/")[0]
        return f"{portal}/crm/type/{PRODUCTION_SPA_ENTITY_TYPE_ID}/details/{item_id}/"
    except Exception as e:
        print(f"_crm_create_spa error: {e}")
        return ""


def create_crm_order_item(order):
    """Создаёт объект в CRM по согласованному заказу (сделка или смарт-процесс).

    Режим — PRODUCTION_CRM_MODE. Best-effort: при любой ошибке/выкл. CRM возвращает
    "" (заказ всё равно зафиксирован в Sheets/чате). Возвращает ссылку на карточку.
    """
    if PRODUCTION_CRM_MODE == "deal":
        return _crm_create_deal(order)
    if PRODUCTION_CRM_MODE in ("spa", "smart", "item"):
        return _crm_create_spa(order)
    print(f"create_crm_order_item: PRODUCTION_CRM_MODE='{PRODUCTION_CRM_MODE}' — CRM-шаг пропущен")
    return ""


def _render_order_result(ok, message):
    color = "#16a34a" if ok else "#b42318"
    icon = "✅" if ok else "❌"
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Заказ на производство</title></head>
<body style="font-family:system-ui,sans-serif;background:#f4f6f8;margin:0;padding:24px;">
<div style="max-width:480px;margin:40px auto;background:#fff;border-radius:12px;padding:32px;text-align:center;box-shadow:0 1px 4px rgba(0,0,0,.08);">
  <div style="font-size:48px;">{icon}</div>
  <h2 style="color:{color};margin:12px 0;">{message}</h2>
  <a href="/order" style="display:inline-block;margin-top:8px;color:#2563eb;text-decoration:none;font-weight:600;">← Создать ещё один</a>
</div>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


def _render_order_form(users, error=None):
    """HTML-форма создания заказа на производство (Local App)."""
    user_options = "\n".join(
        f'<option value="{u["id"]}">{u["name"]}</option>' for u in users
    )
    err_html = f'<div class="err">⚠️ {error}</div>' if error else ""
    today = datetime.now().strftime("%Y-%m-%d")
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Заказы на производство</title>
<script src="//api.bitrix24.com/api/v1/"></script>
<style>
  :root {{
    --bx-blue:#7b2db0; --bx-blue-dark:#5f1f8f; --bx-bg:#eef2f4;
    --bx-border:#dfe5ec; --bx-text:#1e2734; --bx-muted:#7d8a99;
  }}
  * {{ box-sizing:border-box; }}
  body {{ font-family:'Helvetica Neue',Arial,system-ui,sans-serif; margin:0;
         padding:18px; background:var(--bx-bg); color:var(--bx-text); }}
  .card {{ max-width:600px; margin:0 auto; background:#fff; border:1px solid var(--bx-border);
           border-radius:14px; overflow:hidden; box-shadow:0 2px 10px rgba(31,49,71,.06); }}
  .head {{ display:flex; align-items:center; gap:12px; padding:20px 24px;
           background:linear-gradient(135deg,var(--bx-blue),var(--bx-blue-dark)); color:#fff; }}
  .head .ic {{ font-size:26px; line-height:1; }}
  .head h1 {{ font-size:19px; margin:0; font-weight:600; }}
  .head .tag {{ font-size:12px; opacity:.85; margin-top:2px; }}
  .body {{ padding:22px 24px 26px; }}
  label {{ display:block; font-size:13px; font-weight:600; margin:16px 0 6px; }}
  .hint {{ font-weight:400; color:var(--bx-muted); }}
  input, select, textarea {{ width:100%; padding:11px 13px; font-size:15px; color:var(--bx-text);
           border:1px solid var(--bx-border); border-radius:9px; background:#fff;
           transition:border-color .15s, box-shadow .15s; }}
  input:focus, select:focus, textarea:focus {{ outline:none; border-color:var(--bx-blue);
           box-shadow:0 0 0 3px rgba(123,45,176,.12); }}
  textarea {{ resize:vertical; min-height:62px; }}
  .row {{ display:flex; gap:14px; }}
  .row > div {{ flex:1; }}
  .file {{ border:1px dashed var(--bx-border); border-radius:9px; padding:11px 13px;
           background:var(--bx-bg); }}
  button {{ width:100%; margin-top:24px; padding:14px; font-size:16px; font-weight:600;
           color:#fff; background:var(--bx-blue); border:0; border-radius:10px; cursor:pointer;
           transition:background .15s; }}
  button:hover {{ background:var(--bx-blue-dark); }}
  button:disabled {{ background:#b89cd4; cursor:default; }}
  .err {{ background:#fdecec; color:#c0392b; padding:11px 13px; border-radius:9px;
          font-size:14px; margin-bottom:14px; }}
  .req {{ color:#c0392b; }}
  select.urgent {{ border-color:#e0392b; color:#c0392b; background:#fdecec;
           font-weight:700; box-shadow:0 0 0 3px rgba(224,57,43,.12); }}
  select.urgent:focus {{ border-color:#e0392b; box-shadow:0 0 0 3px rgba(224,57,43,.2); }}
  @keyframes urgPulse {{ 0%,100%{{box-shadow:0 0 0 3px rgba(224,57,43,.12);}}
           50%{{box-shadow:0 0 0 5px rgba(224,57,43,.28);}} }}
  select.urgent {{ animation:urgPulse 1.2s ease-in-out infinite; }}
</style>
</head>
<body>
<div class="card">
  <div class="head">
    <div class="ic">🏭</div>
    <div>
      <h1>Заказы на производство</h1>
      <div class="tag">Новый заказ на согласование</div>
    </div>
  </div>
  <div class="body">
    {err_html}
    <form method="POST" action="/order/submit" enctype="multipart/form-data"
          onsubmit="var b=this.querySelector('button');b.disabled=true;b.textContent='Отправляем…';">

      <label>Заявитель <span class="req">*</span>
        <span class="hint" id="reqAutoNote" style="display:none">— определён автоматически</span>
      </label>
      <select id="requesterSelect" disabled style="background:#f4f6f8;cursor:not-allowed;">
        <option value="" selected>— определяется автоматически —</option>
        {user_options}
      </select>
      <input type="hidden" name="requester_id" id="requesterIdHidden">
      <div class="hint" id="reqManualNote" style="display:none;margin-top:6px;">
        Не удалось определить автоматически — выберите себя из списка.
      </div>

      <div class="row">
        <div>
          <label>Дата заказа <span class="req">*</span></label>
          <input type="date" name="order_date" value="{today}" required>
        </div>
        <div>
          <label>Срок (дедлайн) <span class="req">*</span></label>
          <input type="date" name="deadline" required>
        </div>
      </div>

      <label>Артикул / модель <span class="req">*</span></label>
      <input type="text" name="model" placeholder="Напр.: худи Joto Classic, арт. JT-001" required>

      <label>Размерный ряд и количество <span class="req">*</span>
        <span class="hint">— размер и сколько штук</span>
      </label>
      <textarea name="sizes" placeholder="Напр.: S — 10, M — 20, L — 20, XL — 10" required></textarea>

      <div class="row">
        <div>
          <label>Цвет(а) <span class="req">*</span></label>
          <input type="text" name="color" placeholder="Чёрный / молочный" required>
        </div>
        <div>
          <label>Общий тираж <span class="req">*</span></label>
          <input type="text" name="quantity" inputmode="numeric" placeholder="60" required>
        </div>
      </div>

      <label>Ткань (материал, состав) <span class="req">*</span></label>
      <input type="text" name="fabric" placeholder="Напр.: футер 3-нитка, 80% хлопок / 20% ПЭ, 330 г/м²" required>

      <label>Фурнитура <span class="req">*</span></label>
      <input type="text" name="accessories" placeholder="Напр.: люверсы, шнур, бирки, размерники" required>

      <label>Подрядчик / цех <span class="req">*</span></label>
      <input type="text" name="contractor" placeholder="Кто шьёт" required>

      <label>Срочность <span class="req">*</span></label>
      <select name="urgency" id="urgencySelect" required onchange="syncUrgency()">
        <option value="Не срочный" selected>🟢 Не срочный</option>
        <option value="Срочный">🔴 СРОЧНЫЙ — нужно как можно скорее</option>
      </select>

      <label>Образец нужен? <span class="req">*</span></label>
      <select name="sample" required>
        <option value="Да">Да — сначала образец</option>
        <option value="Нет" selected>Нет — сразу в тираж</option>
      </select>

      <label>Файлы <span class="hint">— лекало / макет / тех.задание (можно несколько)</span></label>
      <div class="file"><input type="file" name="files" multiple
           accept=".pdf,.jpg,.jpeg,.png,.ai,.cdr,.zip,.rar,.xlsx,.docx"
           style="border:0;padding:0;background:transparent;"></div>

      <label>Комментарий</label>
      <textarea name="comment" placeholder="Доп. пожелания, детали отшива и т.п."></textarea>

      <button type="submit">Отправить на согласование</button>
    </form>
  </div>
</div>
<script>
  function syncUrgency() {{
    var u = document.getElementById('urgencySelect');
    if (u.value === 'Срочный') {{ u.classList.add('urgent'); }}
    else {{ u.classList.remove('urgent'); }}
  }}
  syncUrgency();

  var sel    = document.getElementById('requesterSelect');
  var hidden = document.getElementById('requesterIdHidden');

  function enableManual() {{
    sel.disabled = false;
    sel.style.background = '#fff';
    sel.style.cursor = 'pointer';
    sel.setAttribute('required', 'required');
    document.getElementById('reqManualNote').style.display = 'block';
    sel.addEventListener('change', function() {{ hidden.value = sel.value; }});
  }}

  try {{
    if (window.BX24) {{
      var done = false;
      BX24.init(function() {{
        try {{ BX24.fitWindow(); }} catch(e) {{}}
        try {{
          BX24.callMethod('user.current', {{}}, function(res) {{
            if (res.error()) {{ if (!done) enableManual(); return; }}
            var u = res.data();
            if (u && u.ID) {{
              if (!sel.querySelector('option[value="' + u.ID + '"]')) {{
                var o = document.createElement('option');
                o.value = u.ID;
                o.textContent = ((u.NAME||'') + ' ' + (u.LAST_NAME||'')).trim() || ('ID ' + u.ID);
                sel.appendChild(o);
              }}
              sel.value = u.ID;
              hidden.value = u.ID;
              document.getElementById('reqAutoNote').style.display = 'inline';
              done = true;
            }} else if (!done) {{ enableManual(); }}
          }});
        }} catch(e) {{ enableManual(); }}
      }});
      setTimeout(function() {{ if (!done && !hidden.value) enableManual(); }}, 4000);
    }} else {{
      enableManual();
    }}
  }} catch(e) {{ enableManual(); }}
</script>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


@app.route("/order", methods=["GET", "POST"])
def order_form_route():
    """Local Application: форма создания заказа на производство."""
    users = fetch_active_users()
    return _render_order_form(users)


@app.route("/order/submit", methods=["POST"])
def order_submit_route():
    """Приём заказа: загрузка файлов, запись в Sheets, уведомление Алану в чат."""
    try:
        form = request.form
        requester_id = (form.get("requester_id") or "").strip()
        order_date   = (form.get("order_date") or "").strip()
        deadline     = (form.get("deadline") or "").strip()
        model        = (form.get("model") or "").strip()
        sizes        = (form.get("sizes") or "").strip()
        color        = (form.get("color") or "").strip()
        quantity     = (form.get("quantity") or "").strip()
        fabric       = (form.get("fabric") or "").strip()
        accessories  = (form.get("accessories") or "").strip()
        contractor   = (form.get("contractor") or "").strip()
        urgency      = (form.get("urgency") or "Не срочный").strip()
        sample       = (form.get("sample") or "").strip()
        comment      = (form.get("comment") or "").strip()
        is_urgent    = urgency == "Срочный"

        required = [requester_id, order_date, deadline, model, sizes, color,
                    quantity, fabric, accessories, contractor, sample]
        if not all(required):
            return _render_order_result(False, "Заполнены не все обязательные поля")

        requester_name = resolve_user_name(requester_id)
        approver = resolve_approver()

        # Файлы (лекало / макет / тех.задание) → Bitrix Drive.
        file_links = []
        for f in request.files.getlist("files"):
            if f and f.filename:
                link = upload_invoice_to_disk(f.filename, f.read())
                if link:
                    file_links.append(link)
        files_str = "\n".join(file_links)

        # Уникальный ID заказа — связывает строку таблицы, сообщение и кнопки.
        rid = uuid.uuid4().hex[:12]

        message_id = None
        if PRODUCTION_CHAT_ID:
            header = ("🔴 [B]СРОЧНЫЙ заказ на производство[/B] 🔴" if is_urgent
                      else "🏭 [B]Новый заказ на производство[/B]")
            lines = [
                header,
                f"👤 Заявитель: {requester_name}",
                f"🏷 Артикул / модель: {model}",
                f"📐 Размеры и кол-во: {sizes}",
                f"🎨 Цвет: {color}",
                f"🧵 Ткань: {fabric}",
                f"🔘 Фурнитура: {accessories}",
                f"🔢 Тираж: {quantity}",
                f"🏭 Подрядчик / цех: {contractor}",
                f"📅 Дата заказа: {order_date}",
                f"⏰ Дедлайн: {deadline}",
                f"🚦 Срочность: {'🔴 СРОЧНЫЙ' if is_urgent else '🟢 Не срочный'}",
                f"🧪 Образец: {sample}",
                f"💬 Комментарий: {comment or '—'}",
            ]
            for i, link in enumerate(file_links, 1):
                lines.append(f"📎 Файл {i}: {link}")
            lines.append("")
            approver_tag = (f"[USER={approver['id']}]{approver['name']}[/USER]"
                            if approver["id"] else approver["name"])
            lines.append(f"{approver_tag}, нужно согласовать заказ 🙏")
            keyboard = [
                {
                    "TEXT": "✅ Согласовать",
                    "LINK": f"{APP_PUBLIC_URL}/order/approve?rid={rid}&action=approve",
                    "BG_COLOR": "#27ae60",
                    "TEXT_COLOR": "#ffffff",
                    "DISPLAY": "LINE",
                },
                {
                    "TEXT": "❌ Отклонить",
                    "LINK": f"{APP_PUBLIC_URL}/order/approve?rid={rid}&action=reject",
                    "BG_COLOR": "#eb5757",
                    "TEXT_COLOR": "#ffffff",
                    "DISPLAY": "LINE",
                },
            ]
            message_id = send_message(PRODUCTION_CHAT_ID, "\n".join(lines), keyboard=keyboard)
        else:
            print("PRODUCTION_CHAT_ID не задан — уведомление в чат не отправлено")

        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        append_order_row([
            now, requester_name, order_date, model, sizes, color, fabric,
            accessories, quantity, contractor, deadline, urgency, sample,
            files_str or "—", comment or "—", "На согласовании",
            rid, requester_id, str(approver["id"] or ""), str(message_id or ""), "—",
        ])

        return _render_order_result(True, "Заказ отправлен на согласование")
    except Exception as e:
        print(f"order_submit_route error: {e}")
        return _render_order_result(False, "Не удалось создать заказ. Попробуйте ещё раз.")


@app.route("/order/approve", methods=["GET"])
def order_approve_route():
    """Страница согласования/отклонения заказа (по кнопке в чате).

    Определяет нажавшего через BX24 (user.current) и отправляет решение на
    /order/approve/confirm. Согласовать/отклонить может только согласующий (Алан).
    """
    rid = (request.args.get("rid") or "").strip()
    action = (request.args.get("action") or "approve").strip()
    is_reject = action == "reject"
    title = "Отклонение заказа…" if is_reject else "Согласование заказа…"
    return Response(
        f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Согласование заказа</title>
<script src="//api.bitrix24.com/api/v1/"></script>
<style>
  body {{ font-family:system-ui,Arial,sans-serif; background:#eef2f4; margin:0; padding:24px; color:#1e2734; }}
  .card {{ max-width:440px; margin:32px auto; background:#fff; border-radius:14px;
           padding:28px 24px; text-align:center; box-shadow:0 2px 10px rgba(31,49,71,.08); }}
  .ic {{ font-size:46px; }}
  h2 {{ margin:14px 0 6px; font-size:19px; }}
  p {{ color:#5b6b7d; font-size:14px; margin:8px 0; }}
</style></head>
<body>
<div class="card">
  <div class="ic" id="ic">⏳</div>
  <h2 id="title">{title}</h2>
  <p id="msg">Определяем пользователя…</p>
</div>
<script>
  var RID = {json.dumps(rid)};
  var ACTION = {json.dumps('reject' if is_reject else 'approve')};
  function show(ic, title, msg) {{
    document.getElementById('ic').textContent = ic;
    document.getElementById('title').textContent = title;
    document.getElementById('msg').textContent = msg;
  }}
  function doConfirm(uid) {{
    var fd = new FormData();
    fd.append('rid', RID);
    fd.append('action', ACTION);
    fd.append('user_id', uid || '');
    fetch('/order/approve/confirm', {{ method:'POST', body:fd }})
      .then(function(r) {{ return r.json(); }})
      .then(function(d) {{
        if (d.ok) {{ show('✅', d.title || 'Готово', d.message || ''); }}
        else {{ show('⚠️', 'Не выполнено', d.message || 'Ошибка'); }}
        try {{ if (window.BX24) BX24.fitWindow(); }} catch(e) {{}}
      }})
      .catch(function() {{ show('⚠️', 'Ошибка', 'Не удалось связаться с сервером'); }});
  }}
  try {{
    if (window.BX24) {{
      BX24.init(function() {{
        try {{ BX24.fitWindow(); }} catch(e) {{}}
        BX24.callMethod('user.current', {{}}, function(res) {{
          if (res.error()) {{ show('⚠️','Ошибка','Не удалось определить пользователя'); return; }}
          var u = res.data();
          doConfirm(u && u.ID ? u.ID : '');
        }});
      }});
    }} else {{
      show('⚠️','Откройте из Битрикса','Кнопку нужно нажимать внутри Битрикс24.');
    }}
  }} catch(e) {{ show('⚠️','Ошибка', String(e)); }}
</script>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


@app.route("/order/approve/confirm", methods=["POST"])
def order_approve_confirm_route():
    """Выполняет согласование/отклонение: проверяет, что нажал Алан, ставит
    статус, правит сообщение, при согласовании — создаёт элемент в CRM."""
    try:
        rid     = (request.form.get("rid") or "").strip()
        action  = (request.form.get("action") or "approve").strip()
        user_id = (request.form.get("user_id") or "").strip()
        is_reject = action == "reject"
        if not rid:
            return jsonify({"ok": False, "message": "Не указан ID заказа"})

        row_number, values = find_order_row_by_rid(rid)
        if not row_number:
            return jsonify({"ok": False, "message": "Заказ не найден"})

        def cell(i):
            return (values[i] if len(values) > i else "").strip()

        status       = cell(15)
        approver_id  = cell(18)
        message_id   = cell(19)
        requester_nm = cell(1)
        model        = cell(3)
        quantity     = cell(8)

        if status in ("Согласован", "Отклонён"):
            return jsonify({"ok": False, "message": f"Заказ уже обработан (статус: {status})"})

        # Согласовать/отклонить может только согласующий (Алан).
        approver_nm = resolve_user_name(approver_id) if approver_id else (
            PRODUCTION_APPROVER_NAMES[0] if PRODUCTION_APPROVER_NAMES else "согласующий")
        if not user_id or (approver_id and str(user_id) != str(approver_id)):
            return jsonify({
                "ok": False,
                "message": f"Согласовать может только {approver_nm}",
            })

        if is_reject:
            set_order_status(row_number, "Отклонён")
            if message_id:
                update_bot_message(message_id, "\n".join([
                    "❌ [B]ЗАКАЗ ОТКЛОНЁН[/B]",
                    f"👤 Заявитель: {requester_nm}",
                    f"🏷 Модель: {model}",
                    f"🔢 Тираж: {quantity}",
                    "",
                    f"[I]Отклонил: {approver_nm}[/I]",
                ]))
            if PRODUCTION_CHAT_ID:
                send_message(PRODUCTION_CHAT_ID, "\n".join([
                    "❌ [B]Заказ отклонён[/B]",
                    f"👤 Заявитель: {requester_nm}",
                    f"🏷 Модель: {model}",
                    f"🔢 Тираж: {quantity}",
                ]))
            return jsonify({"ok": True, "title": "Заказ отклонён",
                            "message": "Статус обновлён"})

        # Согласование: статус → «Согласован», создаём элемент в CRM.
        set_order_status(row_number, "Согласован")
        order = {
            "requester":   requester_nm,
            "order_date":  cell(2),
            "model":       model,
            "sizes":       cell(4),
            "color":       cell(5),
            "fabric":      cell(6),
            "accessories": cell(7),
            "quantity":    quantity,
            "contractor":  cell(9),
            "deadline":    cell(10),
            "urgency":     cell(11),
            "sample":      cell(12),
            "files":       cell(13),
            "comment":     cell(14),
        }
        crm_link = create_crm_order_item(order)
        if crm_link:
            set_order_crm_link(row_number, crm_link)

        if message_id:
            done_lines = [
                "✅ [B]ЗАКАЗ СОГЛАСОВАН[/B]",
                f"👤 Заявитель: {requester_nm}",
                f"🏷 Модель: {model}",
                f"🔢 Тираж: {quantity}",
                "",
                f"[I]Согласовал: {approver_nm}[/I]",
            ]
            if crm_link:
                done_lines.append(f"📋 Карточка в CRM: {crm_link}")
            update_bot_message(message_id, "\n".join(done_lines))

        if PRODUCTION_CHAT_ID:
            notify = [
                "✅ [B]Заказ согласован — в производство[/B]",
                f"👤 Заявитель: {requester_nm}",
                f"🏷 Модель: {model}",
                f"🔢 Тираж: {quantity}",
            ]
            if crm_link:
                notify.append(f"📋 CRM: {crm_link}")
            send_message(PRODUCTION_CHAT_ID, "\n".join(notify))

        msg = "Заказ согласован" + (" и заведён в CRM" if crm_link else "")
        return jsonify({"ok": True, "title": "Заказ согласован", "message": msg})
    except Exception as e:
        print(f"order_approve_confirm_route error: {e}")
        return jsonify({"ok": False, "message": "Внутренняя ошибка"})


@app.route("/order/crm-info", methods=["GET"])
def order_crm_info_route():
    """Диагностика CRM: показывает воронки (направления) сделок с их ID и стадиями.

    Нужна, чтобы узнать точные PRODUCTION_DEAL_CATEGORY_ID и PRODUCTION_DEAL_STAGE_ID
    для прописывания в Railway. Также перечисляет смарт-процессы (на случай режима spa).
    Требует у CRM-вебхука scope `crm`.
    """
    import html as _html
    blocks = []

    # Воронки сделок: дефолтная (ID 0) + пользовательские (crm.dealcategory.list).
    categories = [{"ID": "0", "NAME": "Общее (воронка по умолчанию)"}]
    try:
        r = bitrix_crm_post("crm.dealcategory.list", {}, timeout=20)
        body = r.json()
        if body.get("error"):
            blocks.append(f'<p class="err">crm.dealcategory.list: {_html.escape(str(body))}</p>')
        for c in body.get("result", []) or []:
            categories.append({"ID": str(c.get("ID")), "NAME": c.get("NAME") or f"ID {c.get('ID')}"})
    except Exception as e:
        blocks.append(f'<p class="err">crm.dealcategory.list error: {_html.escape(str(e))}</p>')

    for c in categories:
        cid, cname = c["ID"], c["NAME"]
        try:
            sr = bitrix_crm_post("crm.dealcategory.stage.list", {"id": cid}, timeout=20)
            stages = sr.json().get("result", []) or []
        except Exception as e:
            stages = []
            blocks.append(f'<p class="err">stage.list(id={cid}) error: {_html.escape(str(e))}</p>')
        rows = "".join(
            f"<tr><td>{_html.escape(str(s.get('NAME','')))}</td>"
            f"<td><code>{_html.escape(str(s.get('STATUS_ID','')))}</code></td></tr>"
            for s in stages
        )
        blocks.append(
            f'<h3>🛒 Воронка: {_html.escape(cname)}</h3>'
            f'<p>PRODUCTION_DEAL_CATEGORY_ID = <code>{_html.escape(cid)}</code></p>'
            f'<table><tr><th>Стадия</th><th>PRODUCTION_DEAL_STAGE_ID</th></tr>{rows}</table>'
        )

    # Смарт-процессы (для режима spa) — просто перечислим с entityTypeId.
    try:
        tr = bitrix_crm_post("crm.type.list", {}, timeout=20)
        types = (tr.json().get("result") or {}).get("types") or []
        if types:
            trows = "".join(
                f"<tr><td>{_html.escape(str(t.get('title','')))}</td>"
                f"<td><code>{_html.escape(str(t.get('entityTypeId','')))}</code></td></tr>"
                for t in types
            )
            blocks.append(
                '<h3>⚙️ Смарт-процессы (режим spa)</h3>'
                '<table><tr><th>Название</th><th>entityTypeId</th></tr>'
                f'{trows}</table>'
            )
    except Exception as e:
        blocks.append(f'<p class="err">crm.type.list error: {_html.escape(str(e))}</p>')

    return Response(
        f"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CRM info — заказы на производство</title>
<style>
  body {{ font-family:system-ui,Arial,sans-serif; max-width:760px; margin:24px auto;
         padding:0 18px; color:#1e2734; }}
  h2 {{ color:#7b2db0; }}
  h3 {{ margin-top:26px; }}
  table {{ border-collapse:collapse; width:100%; margin:8px 0 18px; }}
  th, td {{ border:1px solid #dfe5ec; padding:7px 10px; text-align:left; font-size:14px; }}
  th {{ background:#f4f6f8; }}
  code {{ background:#f4f6f8; padding:2px 6px; border-radius:5px; }}
  .err {{ color:#c0392b; }}
  .note {{ background:#fff7e6; border:1px solid #ffe0a3; padding:12px 14px; border-radius:8px; font-size:14px; }}
</style></head>
<body>
<h2>🏭 CRM: воронки и стадии для заказов</h2>
<p class="note">Найди воронку «Заказы на производство», скопируй её
<b>PRODUCTION_DEAL_CATEGORY_ID</b> и <b>PRODUCTION_DEAL_STAGE_ID</b> нужной стадии,
пропиши их в переменные Railway. Если таблицы пустые или красные ошибки —
у CRM-вебхука нет scope <code>crm</code> (заведи BITRIX_CRM_WEBHOOK_URL со scope crm).</p>
{''.join(blocks)}
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


@app.route("/categories", methods=["GET"])
def categories_route():
    """Страница управления категориями заявок: список + добавить/удалить."""
    cats = get_payment_categories()
    rows = ""
    for c in cats:
        rows += f"""
        <li>
          <span>{c}</span>
          <form method="POST" action="/categories/delete" onsubmit="return confirm('Удалить категорию «{c}»?');">
            <input type="hidden" name="name" value="{c}">
            <button class="del" title="Удалить">✕</button>
          </form>
        </li>"""
    html = f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Платежи · категории</title>
<style>
  body {{ font-family:'Helvetica Neue',Arial,system-ui,sans-serif; background:#eef2f4;
         margin:0; padding:18px; color:#1e2734; }}
  .card {{ max-width:560px; margin:0 auto; background:#fff; border:1px solid #dfe5ec;
           border-radius:14px; overflow:hidden; box-shadow:0 2px 10px rgba(31,49,71,.06); }}
  .head {{ padding:18px 24px; background:linear-gradient(135deg,#2066b0,#17518f); color:#fff; }}
  .head h1 {{ margin:0; font-size:18px; }}
  .body {{ padding:18px 24px 24px; }}
  ul {{ list-style:none; margin:0 0 18px; padding:0; }}
  li {{ display:flex; align-items:center; justify-content:space-between; gap:10px;
        padding:10px 12px; border:1px solid #eef2f4; border-radius:9px; margin-bottom:8px; }}
  li span {{ font-size:15px; }}
  li form {{ margin:0; }}
  .del {{ border:0; background:#fdecec; color:#c0392b; width:28px; height:28px;
          border-radius:7px; cursor:pointer; font-size:14px; }}
  .del:hover {{ background:#f8d4d4; }}
  .add {{ display:flex; gap:10px; }}
  .add input {{ flex:1; padding:11px 13px; font-size:15px; border:1px solid #dfe5ec; border-radius:9px; }}
  .add button {{ padding:11px 18px; font-size:15px; font-weight:600; color:#fff;
                 background:#2066b0; border:0; border-radius:9px; cursor:pointer; }}
  .add button:hover {{ background:#17518f; }}
</style></head>
<body><div class="card">
  <div class="head"><h1>📂 Категории заявок</h1></div>
  <div class="body">
    <ul>{rows or '<li><span>Список пуст</span></li>'}</ul>
    <form class="add" method="POST" action="/categories/add">
      <input type="text" name="name" placeholder="Новая категория" required>
      <button>Добавить</button>
    </form>
    <form method="POST" action="/categories/reset" style="margin-top:14px;"
          onsubmit="return confirm('Заменить весь список стандартными категориями?');">
      <button style="width:100%;padding:11px;border:0;border-radius:9px;cursor:pointer;
                     background:#eef2f4;color:#7d8a99;font-size:14px;font-weight:600;">
        ↻ Сбросить к стандартным
      </button>
    </form>
  </div>
</div></body></html>"""
    return Response(html, mimetype="text/html; charset=utf-8")


@app.route("/categories/add", methods=["POST"])
def categories_add_route():
    add_payment_category(request.form.get("name"))
    return Response('<meta http-equiv="refresh" content="0;url=/categories">',
                    mimetype="text/html; charset=utf-8")


@app.route("/categories/delete", methods=["POST"])
def categories_delete_route():
    delete_payment_category(request.form.get("name"))
    return Response('<meta http-equiv="refresh" content="0;url=/categories">',
                    mimetype="text/html; charset=utf-8")


@app.route("/categories/reset", methods=["POST"])
def categories_reset_route():
    reset_payment_categories()
    return Response('<meta http-equiv="refresh" content="0;url=/categories">',
                    mimetype="text/html; charset=utf-8")


@app.route("/install-app", methods=["GET", "POST"])
def install_app_route():
    """Установка Local App «Платежи».

    Само Локальное приложение уже добавляет пункт меню «Платежи» (через поле
    «Название пункта меню»), поэтому отдельный placement.bind НЕ нужен — иначе
    появляется дубль. Здесь только подчищаем возможную лишнюю привязку
    LEFT_MENU (если она осталась от прошлых версий) и завершаем установку.
    Чат-бот не трогаем — он живёт отдельно (вебхук-конструктор).
    """
    if request.method == "GET":
        return _render_install_page()

    data = parse_request_data()
    auth = parse_auth_from_event(data)
    access_token = auth.get("access_token") or str(data.get("AUTH_ID") or "")
    client_endpoint = auth.get("client_endpoint") or derive_client_endpoint(auth.get("domain"))

    # Снимаем нашу старую LEFT_MENU-привязку (best-effort) — убирает дубль
    if access_token and client_endpoint:
        try:
            requests.post(
                f"{client_endpoint.rstrip('/')}/placement.unbind.json",
                data={"auth": access_token, "PLACEMENT": "LEFT_MENU",
                      "HANDLER": f"{APP_PUBLIC_URL}/pay"},
                timeout=10,
            )
        except Exception as e:
            print(f"install-app unbind error (ignored): {e}")

    return Response(
        """<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<script src="//api.bitrix24.com/api/v1/"></script></head>
<body style="font-family:system-ui,sans-serif;max-width:560px;margin:40px auto;padding:24px;">
<h2 style="color:#28a745;">✅ Приложение «Платежи» установлено</h2>
<p>Открой пункт <b>«Платежи»</b> в левом меню — заявитель определится автоматически.</p>
<script>try{if(window.BX24){BX24.init(function(){try{BX24.installFinish();}catch(e){}});}}catch(e){}</script>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


@app.route("/install-order-app", methods=["GET", "POST"])
def install_order_app_route():
    """Установка Local App «Заказы на производство».

    Как и «Платежи», само Локальное приложение добавляет пункт левого меню
    (через поле «Название пункта меню» с handler-URL `/order`), поэтому отдельный
    placement.bind здесь НЕ нужен — иначе появится дубль. Просто завершаем
    установку (installFinish) и подчищаем возможную старую LEFT_MENU-привязку.
    """
    if request.method == "GET":
        return Response(
            """<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<script src="//api.bitrix24.com/api/v1/"></script></head>
<body style="font-family:system-ui,sans-serif;max-width:560px;margin:40px auto;padding:24px;">
<h2>Заказы на производство</h2>
<p>Это обработчик установки Local App. Откройте установку из Битрикса.</p>
</body></html>""",
            mimetype="text/html; charset=utf-8",
        )

    data = parse_request_data()
    auth = parse_auth_from_event(data)
    access_token = auth.get("access_token") or str(data.get("AUTH_ID") or "")
    client_endpoint = auth.get("client_endpoint") or derive_client_endpoint(auth.get("domain"))

    if access_token and client_endpoint:
        try:
            requests.post(
                f"{client_endpoint.rstrip('/')}/placement.unbind.json",
                data={"auth": access_token, "PLACEMENT": "LEFT_MENU",
                      "HANDLER": f"{APP_PUBLIC_URL}/order"},
                timeout=10,
            )
        except Exception as e:
            print(f"install-order-app unbind error (ignored): {e}")

    return Response(
        """<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<script src="//api.bitrix24.com/api/v1/"></script></head>
<body style="font-family:system-ui,sans-serif;max-width:560px;margin:40px auto;padding:24px;">
<h2 style="color:#7b2db0;">✅ Приложение «Заказы на производство» установлено</h2>
<p>Открой пункт <b>«Заказы на производство»</b> в левом меню — заявитель определится автоматически.</p>
<script>try{if(window.BX24){BX24.init(function(){try{BX24.installFinish();}catch(e){}});}}catch(e){}</script>
</body></html>""",
        mimetype="text/html; charset=utf-8",
    )


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


def _unregister_bot_by_id(client_endpoint, access_token, bot_id):
    """Удаляет чат-бота по числовому BOT_ID. Это единственная форма,
    которую imbot.unregister реально принимает: вызов с CODE=... всегда
    возвращает Bot not found. Возвращает True если бот действительно
    удалён, False иначе. Все ошибки логируем, но не падаем — это
    зачистка перед регистрацией.
    """
    try:
        url = f"{client_endpoint.rstrip('/')}/imbot.unregister.json"
        resp = requests.post(
            url,
            data={"auth": access_token, "BOT_ID": int(bot_id)},
            timeout=10,
        )
        body_preview = safe_preview(resp.text, 200)
        print(f"imbot.unregister(BOT_ID={bot_id}) status={resp.status_code} body={body_preview}")
        if resp.status_code != 200:
            return False
        return not resp.json().get("error")
    except Exception as e:
        print(f"_unregister_bot_by_id({bot_id}): {e}")
        return False


def _bind_chat_events(client_endpoint, access_token, handler_url):
    """Привязывает события чат-бота к нашему handler URL через event.bind.

    Это критический шаг для Local App, который imbot.register НЕ делает
    автоматически. Без этих привязок Битрикс не знает, куда слать
    ONIMBOTMESSAGEADD и прочие события — даже если бот зарегистрирован.

    Перед привязкой пробуем event.unbind по тем же событиям —
    на случай повторной установки. Все ошибки логируем но не падаем.
    """
    events = ("ONIMBOTMESSAGEADD", "ONIMBOTJOINCHAT", "ONIMBOTDELETE")
    base = client_endpoint.rstrip("/")
    for event_name in events:
        # Снимаем старую привязку (best-effort) — нужно при переустановке,
        # иначе event.bind может вернуть ERROR_EVENT_FOUND.
        try:
            r = requests.post(
                f"{base}/event.unbind.json",
                data={"auth": access_token, "event": event_name,
                      "handler": handler_url},
                timeout=10,
            )
            print(f"event.unbind({event_name}) status={r.status_code} "
                  f"body={safe_preview(r.text, 200)}")
        except Exception as e:
            print(f"event.unbind({event_name}): {e}")

        # Привязываем заново
        try:
            r = requests.post(
                f"{base}/event.bind.json",
                data={"auth": access_token, "event": event_name,
                      "handler": handler_url},
                timeout=10,
            )
            print(f"event.bind({event_name}) status={r.status_code} "
                  f"body={safe_preview(r.text, 200)}")
        except Exception as e:
            print(f"event.bind({event_name}): {e}")


def _register_chat_bot(client_endpoint, access_token):
    """Регистрирует чат-бота через imbot.register от имени установившего.

    Главное открытие после ночи дебага:
    -----------------------------------
    Bitrix24 imbot.unregister(CODE=...) НЕ работает — всегда возвращает
    "Bot not found". Только imbot.unregister(BOT_ID=число) реально удаляет
    бота. Из-за этого все наши предыдущие переустановки оставляли в
    Битриксе СТАРУЮ запись бота 256, созданную самым первым багованным
    PR #3 (json-body вместо form-encoded). И каждый последующий
    imbot.register просто переиспользовал ту запись со СТАРЫМИ
    EVENT_MESSAGE_ADD = пустыми URL. Поэтому Битрикс никуда не доставлял
    события — handler URL у бота буквально не было прописано.

    Что делаем теперь (bulldoze + double-register):
      1) Заранее пытаемся снести любых известных «исторических» ботов
         по диапазону BOT_ID 255-269.
      2) Делаем первичный register — он либо вернёт существующий ID,
         либо создаст нового.
      3) Удаляем то, что вернулось (по BOT_ID).
      4) Делаем второй register — это даст ГАРАНТИРОВАННО чистую запись
         со свежими EVENT_MESSAGE_ADD URL.
      5) Дополнительно вызываем event.bind как страховка.

    PROPERTIES шлём form-encoded с bracket-notation — Битрикс не парсит
    nested JSON для этих полей.
    """
    bot_handler_url = f"{APP_PUBLIC_URL}/bot"
    register_url = f"{client_endpoint.rstrip('/')}/imbot.register.json"

    # Шаг 1: зачистка по диапазону известных исторических BOT_ID.
    # Большинство удалений вернут "Bot not found" — это нормально.
    print("[bulldoze] removing any historical bots by BOT_ID range")
    for bid in range(255, 270):
        _unregister_bot_by_id(client_endpoint, access_token, bid)

    # form-encoded payload для register (используем дважды).
    register_data = {
        "auth":                      access_token,
        "CODE":                      "dds_bot",
        "TYPE":                      "B",
        "EVENT_MESSAGE_ADD":         bot_handler_url,
        "EVENT_WELCOME_MESSAGE":     bot_handler_url,
        "EVENT_BOT_DELETE":          bot_handler_url,
        "PROPERTIES[NAME]":          "ДДС Бот",
        "PROPERTIES[WORK_POSITION]": "PDF-выписки Сбербанка → Google-таблица",
        "PROPERTIES[COLOR]":         "GREEN",
    }

    # Шаг 2: первый register — узнаём фактический BOT_ID
    resp1 = requests.post(register_url, data=register_data, timeout=20)
    print(f"[register #1] status={resp1.status_code} body={safe_preview(resp1.text, 500)}")
    try:
        result1 = resp1.json()
    except Exception as e:
        raise ValueError(f"imbot.register #1: невалидный JSON в ответе: {e}")
    if "error" in result1:
        raise ValueError(
            f"imbot.register #1: {result1.get('error_description') or result1.get('error')}"
        )
    bot_id_1 = result1.get("result")
    if not bot_id_1:
        raise ValueError(f"imbot.register #1: пустой result, ответ: {safe_preview(resp1.text, 300)}")

    # Шаг 3: удаляем то, что register вернул — даже если это новая запись,
    # без удаления нет гарантии, что Битрикс правильно сохранил EVENT_*
    # поля. После удаления следующий register создаст 100% свежую запись.
    print(f"[bulldoze] removing bot returned by first register: BOT_ID={bot_id_1}")
    _unregister_bot_by_id(client_endpoint, access_token, bot_id_1)

    # Шаг 4: финальный register — гарантированно создаёт новую запись
    # с правильными EVENT_MESSAGE_ADD = bot_handler_url
    resp2 = requests.post(register_url, data=register_data, timeout=20)
    print(f"[register #2 fresh] status={resp2.status_code} body={safe_preview(resp2.text, 500)}")
    try:
        result2 = resp2.json()
    except Exception as e:
        raise ValueError(f"imbot.register #2: невалидный JSON в ответе: {e}")
    if "error" in result2:
        raise ValueError(
            f"imbot.register #2: {result2.get('error_description') or result2.get('error')}"
        )
    final_bot_id = result2.get("result")
    if not final_bot_id:
        raise ValueError(f"imbot.register #2: пустой result, ответ: {safe_preview(resp2.text, 300)}")

    # Шаг 5: на всякий случай ещё и event.bind для app-level подписки.
    _bind_chat_events(client_endpoint, access_token, bot_handler_url)

    return final_bot_id


def _bind_payment_placement(client_endpoint, access_token):
    """Регистрирует приложение «Платежи» как пункт левого меню Битрикса.

    placement.bind(LEFT_MENU) добавляет в левое меню портала иконку,
    открывающую нашу форму заявки `/pay` в iframe. Перед привязкой
    снимаем старую (best-effort) — нужно при переустановке, иначе
    placement.bind вернёт ошибку «handler already binded».
    """
    base = client_endpoint.rstrip("/")
    handler_url = f"{APP_PUBLIC_URL}/pay"
    # Снимаем старую привязку (best-effort)
    try:
        requests.post(
            f"{base}/placement.unbind.json",
            data={"auth": access_token, "PLACEMENT": "LEFT_MENU", "HANDLER": handler_url},
            timeout=10,
        )
    except Exception as e:
        print(f"placement.unbind error (ignored): {e}")
    # Привязываем «Платежи»
    try:
        resp = requests.post(
            f"{base}/placement.bind.json",
            data={
                "auth":        access_token,
                "PLACEMENT":   "LEFT_MENU",
                "HANDLER":     handler_url,
                "TITLE":       "Платежи",
                "DESCRIPTION": "Создание заявок на оплату",
            },
            timeout=15,
        )
        print(f"placement.bind(LEFT_MENU) status={resp.status_code} body={safe_preview(resp.text, 300)}")
        return resp.status_code == 200 and not resp.json().get("error")
    except Exception as e:
        print(f"_bind_payment_placement error: {e}")
        return False


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
        # Пункт меню «Платежи» создаёт само Локальное приложение «Платежи»
        # (через поле «Название пункта меню»), поэтому здесь placement.bind
        # НЕ вызываем — иначе появляется дубль пункта меню.
        return _render_install_page(bot_id=bot_id)
    except Exception as e:
        print(f"INSTALL ERROR: {e}")
        return _render_install_page(error=str(e)), 500


if __name__ == "__main__":
    print("===== STARTING APP =====")
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
