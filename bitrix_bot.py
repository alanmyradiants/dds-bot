import os
import io
import re
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

# Имя бот-команды для кнопки «Отменить заявку». Регистрируется через
# /register-cancel-command; по клику Битрикс шлёт ONIMCOMMANDADD на /bot.
CANCEL_COMMAND = "cancelpay"

# Сотрудники-плательщики для поля «Кто оплачивает».
# Каждое имя сопоставляется по подстроке в ФИО (ID знать не нужно). Несколько
# имён через запятую → переопределяется env PAYMENT_DEFAULT_PAYER_NAME.
# Если совпал ровно один — поле блокируется; если несколько — выпадающий список.
PAYMENT_PAYER_NAMES = [
    n.strip()
    for n in os.getenv("PAYMENT_DEFAULT_PAYER_NAME", "Кисиев, Фаткуллина").split(",")
    if n.strip()
]

# Кто, КРОМЕ заявителя, может отменить любую заявку (по подстроке в ФИО).
# По умолчанию — Анастасия Фаткуллина (ответственная за платежи).
PAYMENT_CANCEL_EXTRA_NAMES = [
    n.strip()
    for n in os.getenv("PAYMENT_CANCEL_EXTRA_NAMES", "Фаткуллина").split(",")
    if n.strip()
]

# ─────────────────────────────────────────────
# Google Sheets credentials
# ─────────────────────────────────────────────
# ВАЖНО: разбираем креды «мягко». Раньше здесь был json.loads(os.environ[...])
# прямо на импорте — если переменная не задана или в ней битый JSON, падал ВЕСЬ
# модуль: не поднимался Flask, /health и /check тоже не отвечали. Снаружи это
# выглядело как «бот полностью умер», хотя проблема была в одной env-переменной.
# Теперь приложение стартует всегда, а про поломку креды сообщает /check.
GOOGLE_CREDS_ERROR = ""
try:
    _raw_google_creds = os.environ.get("GOOGLE_CREDENTIALS") or ""
    if not _raw_google_creds.strip():
        GOOGLE_CREDS_JSON = None
        GOOGLE_CREDS_ERROR = "переменная GOOGLE_CREDENTIALS не задана"
    else:
        GOOGLE_CREDS_JSON = json.loads(_raw_google_creds)
except Exception as _e:
    GOOGLE_CREDS_JSON = None
    GOOGLE_CREDS_ERROR = f"GOOGLE_CREDENTIALS — битый JSON: {_e}"
if GOOGLE_CREDS_ERROR:
    print(f"⚠️ Google Sheets: {GOOGLE_CREDS_ERROR}")

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
    "Логистика, отправка от фулфилмента до склада ВБ",
    "Логистика перемещения",
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
    "Оплата лекал",
    "Фулфилмент сборка",
    "Фулфилмент хранение",
    "Упаковочные материалы",
    "Подарки сотрудникам",
    "Программы и ПО",
    "Семинары и обучение",
    "Услуги пошива",
    "Изготовление бирок",
    "Коробки",
]

# Категории, выведенные из оборота. `init_sheets` удаляет их из листа
# «Категории заявок» — иначе старое название висело бы в форме вечно, ведь
# недостающие категории только дописываются, а лишние никогда не убирались.
# Уже созданные заявки со старой категорией не трогаем — это история.
PAYMENT_CATEGORIES_RETIRED = [
    # Разделена на «…отправка от фулфилмента до склада ВБ» и «…перемещения».
    "Логистика",
    # Убрана по просьбе заказчика (2026-07-29).
    "Транспортные расходы",
]


# Страницы, которые читают живые данные из Google Sheets (форма заявки, список
# категорий), нельзя кешировать: мобильный Битрикс держит iframe в кеше и после
# правки категорий показывает старый список — выглядит как «ничего не применилось».
NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def deployed_version():
    """Какой коммит реально задеплоен (Railway подставляет эти env сам).

    Без этого «задеплоилось или ещё собирается» приходится угадывать: сервер
    отвечает 200 и на старой версии тоже.
    """
    sha = os.getenv("RAILWAY_GIT_COMMIT_SHA", "")
    return {
        "commit": sha[:7] if sha else "unknown",
        "branch": os.getenv("RAILWAY_GIT_BRANCH", "unknown"),
        "deployment_id": os.getenv("RAILWAY_DEPLOYMENT_ID", "unknown"),
    }


# ─────────────────────────────────────────────
# Диагностика «бот молчит»
# ─────────────────────────────────────────────
# /check раньше проверял только «доступны ли сервисы». Но самый частый симптом —
# бот вообще не отвечает в чате, а причина одна из двух:
#   1) Битрикс НЕ ДОСТАВЛЯЕТ события на /bot (бот отписан/не зарегистрирован) —
#      тогда мы вообще ничего не получали;
#   2) события приходят, а imbot.message.add падает (бот удалён из портала,
#      не тот BOT_ID/CLIENT_ID, истёк вебхук) — тогда ошибка видна в ответе.
# Раньше оба случая выглядели снаружи одинаково: тишина. Теперь запоминаем
# последнее входящее событие и последнюю отправку — /check показывает, на каком
# из двух шагов рвётся цепочка.
_DIAG = {
    "started_at": datetime.now().isoformat(timespec="seconds"),
    "events_total": 0,
    "last_event": None,   # {at, event, dialog_id, has_pdf, from}
    "sends_total": 0,
    "send_errors": 0,
    "last_send": None,    # {at, dialog_id, ok, error}
    "downloads_total": 0,
    "download_errors": 0,
    "last_download": None,  # {at, file_id, ok, reason, steps}
}
_DIAG_LOCK = threading.Lock()


def _record_event(event, dialog_id, extra=None):
    with _DIAG_LOCK:
        _DIAG["events_total"] += 1
        _DIAG["last_event"] = {
            "at": datetime.now().isoformat(timespec="seconds"),
            "event": event or "",
            "dialog_id": str(dialog_id or ""),
            **(extra or {}),
        }


def _annotate_last_event(extra):
    """Дописывает детали к уже записанному событию, не увеличивая счётчик."""
    with _DIAG_LOCK:
        if isinstance(_DIAG.get("last_event"), dict):
            _DIAG["last_event"].update(extra or {})


def _record_send(dialog_id, ok, error=""):
    with _DIAG_LOCK:
        _DIAG["sends_total"] += 1
        if not ok:
            _DIAG["send_errors"] += 1
        _DIAG["last_send"] = {
            "at": datetime.now().isoformat(timespec="seconds"),
            "dialog_id": str(dialog_id or ""),
            "ok": bool(ok),
            "error": str(error or "")[:300],
        }


def _record_download(file_id, ok, trace):
    """Запоминает последнюю попытку скачать файл из Битрикса.

    До этого единственным местом, где было видно, ПОЧЕМУ файл не скачался,
    были логи Railway — а до них у пользователя доступа нет. Теперь трасса
    попыток видна в /check и целиком в /download-test.
    """
    with _DIAG_LOCK:
        _DIAG["downloads_total"] += 1
        if not ok:
            _DIAG["download_errors"] += 1
        _DIAG["last_download"] = {
            "at": datetime.now().isoformat(timespec="seconds"),
            "file_id": str(file_id or ""),
            "ok": bool(ok),
            "reason": "" if ok else trace.summary(),
            "steps": trace.steps,
        }

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
        "📤 [B]Если файл не скачивается[/B]\n"
        "Иногда Битрикс не отдаёт мне тело файла (чаще всего с пересланными "
        "вложениями). Тогда загрузите выписку через страницу "
        f"[url={APP_PUBLIC_URL}/upload]{APP_PUBLIC_URL}/upload[/url] — "
        "она отправляет файл прямо в обработку, минуя Диск Битрикса.\n\n"
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


def bitrix_portal_url():
    """Базовый URL портала Битрикс (напр. https://joto.bitrix24.ru) из вебхука."""
    for url in (BITRIX_WEBHOOK_URL, BITRIX_DISK_WEBHOOK_URL):
        if not url:
            continue
        p = urlparse(url)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
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
    if GOOGLE_CREDS_JSON is None:
        raise RuntimeError(GOOGLE_CREDS_ERROR or "нет кредов Google")
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
    for sheet_name in ["Транзакции", "Правила", "Заявки", "Категории заявок"]:
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

    # Лист «Категории заявок» — источник списка категорий для формы.
    # Заголовок ставим всегда. Стандартные категории из PAYMENT_CATEGORIES_DEFAULT
    # дописываем недеструктивно: добавляем только те, которых ещё нет в листе, —
    # так новые категории появляются после /init-sheets, а правки пользователя
    # (свои категории) не затираются. Отдельно вычищаем PAYMENT_CATEGORIES_RETIRED —
    # категории, которые переименовали/разделили.
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

    # Итоговый порядок: сначала стандартные — в порядке PAYMENT_CATEGORIES_DEFAULT,
    # потом кастомные категории пользователя (в том порядке, в каком они в листе).
    # Так новая категория встаёт на своё место в списке, а не в самый низ формы.
    custom = []
    for c in existing_cats:
        if (c not in PAYMENT_CATEGORIES_RETIRED
                and c not in PAYMENT_CATEGORIES_DEFAULT
                and c not in custom):
            custom.append(c)
    final_cats = list(PAYMENT_CATEGORIES_DEFAULT) + custom
    added = [c for c in final_cats if c not in existing_cats]
    removed = [c for c in existing_cats if c not in final_cats]
    if final_cats != existing_cats:
        # Порядок и состав изменились — переписываем столбец целиком.
        service.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID, range="Категории заявок!A2:A",
        ).execute()
        if final_cats:
            service.spreadsheets().values().update(
                spreadsheetId=SHEET_ID,
                range="Категории заявок!A2",
                valueInputOption="RAW",
                body={"values": [[c] for c in final_cats]},
            ).execute()
        if added:
            print(f"ℹ️ Дописаны недостающие категории: {', '.join(added)}")
        if removed:
            print(f"ℹ️ Удалены устаревшие категории: {', '.join(removed)}")

    print("✅ Таблица инициализирована")
    print("ℹ️ Правила заполнятся автоматически при загрузке PDF")

    # Возвращаем ИТОГ, а не просто «ок»: по ответу /init-sheets сразу видно,
    # применилась ли миграция категорий. Раньше ответ был одинаковый и когда
    # лист поменялся, и когда код на сервере ещё старый и делать нечего.
    return {
        "categories_added": added,
        "categories_removed": removed,
        "categories_total": len(final_cats),
        "categories": final_cats,
    }


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
            body = resp.json()
        except Exception:
            body = {}
        # Битрикс отвечает 200 и с ошибкой в теле («ERROR_BOT_NOT_FOUND» и т.п.).
        # Раньше мы это молча проглатывали: в логах был только
        # «imbot.message.add POST status=200», а сообщение до чата не доходило —
        # бот выглядел мёртвым без единого следа причины.
        if resp.status_code != 200 or (isinstance(body, dict) and body.get("error")):
            err = "HTTP {}".format(resp.status_code)
            if isinstance(body, dict) and body.get("error"):
                err = str(body.get("error_description") or body.get("error"))
            print(f"send_message FAILED ({err}): {safe_preview(resp.text, 300)}")
            _record_send(dialog_id, False, err)
            return None
        _record_send(dialog_id, True)
        return body.get("result") if isinstance(body, dict) else None
    except Exception as e:
        print(f"send_message error: {e}")
        _record_send(dialog_id, False, str(e))
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


# ─────────────────────────────────────────────
# Скачивание файла из Битрикса
# ─────────────────────────────────────────────
# Хроника проблемы (см. handoff 2026-07-24): disk.file.get отдаёт DOWNLOAD_URL
# со статусом 200, но закачка САМОГО ТЕЛА с joto.bitrix24.ru виснет — портал
# принимает соединение, отдаёт заголовки и не присылает байты. Снаружи это
# неотличимо от «нет прав», поэтому месяц ушёл на догадки. Здесь два принципа:
#   1) каждая попытка ПРОТОКОЛИРУЕТСЯ (шаг, статус, content-type, размер,
#      время, ошибка). Трасса уходит в /check и /download-test — причину видно
#      без логов Railway, до которых у пользователя нет доступа;
#   2) общий БЮДЖЕТ времени: раньше шесть попыток по 120с давали 12 минут
#      тишины в чате.

DOWNLOAD_BUDGET_SEC = 100   # потолок на ВСЕ попытки скачать один файл
REST_TIMEOUT_SEC    = 15    # метаданные REST отвечают за доли секунды


def _url_label(url):
    """Адрес для логов и трассы, очищенный от всего, что даёт доступ.

    Трассу пересылают в чатах и тикетах, поэтому режем два вида секретов:
    query (там auth-токен) и хеш публичной ссылки `/doc/<hash>` — сам этот хеш
    и есть пропуск к файлу, кто угодно откроет его без входа в портал.
    Хватает первых символов, чтобы отличить один шаг от другого.
    """
    path = str(url or "").split("?")[0]
    return re.sub(r"(/doc/)([^/]{6})[^/]+", r"\g<1>\g<2>…", path)


def _with_auth_param(url, token):
    """Добавляет ?auth=<token> к ссылке, если его там ещё нет."""
    if not url or not token:
        return url
    if "auth=" in (urlparse(url).query or ""):
        return url
    return url + ("&" if urlparse(url).query else "?") + "auth=" + token


# Два «профиля клиента». Bitrix-облако за WAF иногда молча тарпитит запросы с
# браузерным User-Agent, приходящие с дата-центрового IP (Railway): заголовки
# отдаёт, тело — нет. Мелкий JSON REST при этом проходит. Поэтому если тело
# файла не доехало, вторая попытка идёт «честным» клиентом: без маскировки под
# браузер, без сжатия и с Connection: close. Если дело не в WAF, вторая попытка
# просто повторит первую — цена одна лишняя запись в трассе.
_CLIENT_PROFILES = [
    ("browser", {"User-Agent": "Mozilla/5.0",
                 "Accept": "application/pdf,*/*"}),
    ("plain",   {"User-Agent": "dds-bot/1.0 (python-requests)",
                 "Accept": "*/*",
                 "Accept-Encoding": "identity",
                 "Connection": "close"}),
]


class DownloadTrace:
    """Журнал попыток скачивания: что пробовали, чем кончилось, сколько заняло.

    Нужен потому, что «файл не скачался» — это пять разных причин (нет прав,
    портал молчит, вернулась HTML-страница логина, истёк токен, файла нет),
    и снаружи они выглядят одинаково. `steps` уезжает в /download-test целиком,
    `reasons` — короткая выжимка для сообщения в чат.
    """

    def __init__(self):
        self.started = time.monotonic()
        self.steps = []
        self.reasons = []

    def add(self, step, **fields):
        entry = {"step": step, "t": round(time.monotonic() - self.started, 1)}
        for key, value in fields.items():
            if value not in (None, "", []):
                entry[key] = value
        self.steps.append(entry)
        print(f"[download] {json.dumps(entry, ensure_ascii=False)}")
        return entry

    def reason(self, text):
        """Короткая человекочитаемая причина (без дублей, в порядке появления)."""
        if text and text not in self.reasons:
            self.reasons.append(text)

    def seconds_left(self):
        return DOWNLOAD_BUDGET_SEC - (time.monotonic() - self.started)

    def out_of_time(self):
        # 5 секунд про запас: начинать попытку, на которую заведомо не хватит
        # времени, — только путать трассу.
        return self.seconds_left() <= 5

    def summary(self):
        return "; ".join(self.reasons[:4]) or "ни одна попытка не дала файла"


def extract_download_url(file_info):
    for key in ["DOWNLOAD_URL", "downloadUrl", "DOWNLOAD_URL_MACHINE", "URL_DOWNLOAD"]:
        value = file_info.get(key)
        if value:
            return value
    return None


def try_download(url, extra_headers=None, connect_timeout=10, read_timeout=30,
                 trace=None, label="download", session=None):
    """Качает файл по ссылке. Возвращает bytes или None.

    stream=True + короткий read-timeout: тело читается кусками, поэтому «немой»
    сервер отваливается за секунды, а не за две минуты. Если первая попытка
    упала ИМЕННО по таймауту чтения — повторяем неброузерным профилем
    (см. `_CLIENT_PROFILES`): возможно, тарпитит WAF, а не права.
    """
    trace = trace or DownloadTrace()
    body_incomplete = False

    for profile_name, base_headers in _CLIENT_PROFILES:
        if profile_name != "browser" and not body_incomplete:
            break  # второй профиль имеет смысл только после тарпита
        if trace.out_of_time():
            trace.add(f"{label}/{profile_name}", url=_url_label(url),
                      error="бюджет времени исчерпан")
            return None

        headers = dict(base_headers)
        if extra_headers:
            headers.update(extra_headers)
        info = {"url": _url_label(url), "profile": profile_name}

        # Фаза 1: соединиться и получить заголовки.
        getter = session.get if session is not None else requests.get
        try:
            resp = getter(
                url, headers=headers, timeout=(connect_timeout, read_timeout),
                allow_redirects=True, stream=True,
            )
        except Exception as e:
            info["error"] = f"{type(e).__name__}: {e}"
            info["verdict"] = "не удалось соединиться"
            trace.add(f"{label}/{profile_name}", **info)
            trace.reason(f"портал недоступен ({type(e).__name__})")
            return None

        # Фаза 2: прочитать тело. Разделено с фазой 1 намеренно: «портал принял
        # соединение, отдал заголовки и не прислал байты» — это ровно тот
        # симптом, который ловим (см. handoff), и выглядеть он может по-разному:
        # ReadTimeout, ChunkedEncodingError, ConnectionError, IncompleteRead.
        # Все они означают одно и то же и лечатся одинаково — повтором другим
        # профилем клиента.
        with resp:
            content_type = (resp.headers.get("Content-Type") or "").lower()
            info["status"] = resp.status_code
            info["ct"] = content_type
            if resp.history:
                hops = [_url_label(r.url) for r in resp.history]
                info["redirects"] = " → ".join(hops + [_url_label(resp.url)])
            if resp.status_code != 200:
                # Битрикс кладёт настоящую причину в тело ответа
                # ({"error":"expired_token"} и т.п.) — без неё «401» ничего
                # не говорит о том, ЧТО именно не так с авторизацией.
                try:
                    info["body"] = safe_preview(resp.content[:400].decode(
                        "utf-8", "replace"), 300)
                except Exception:
                    pass
                info["verdict"] = "HTTP != 200"
                trace.add(f"{label}/{profile_name}", **info)
                trace.reason(f"портал ответил {resp.status_code}")
                return None
            try:
                # Тело читаем кусками: read-timeout срабатывает между чанками,
                # поэтому «немой» сервер отваливается за секунды, а не за две
                # минуты.
                chunks, total = [], 0
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    chunks.append(chunk)
                    total += len(chunk)
                    if total > 25 * 1024 * 1024:  # предохранитель 25 МБ
                        break
                content = b"".join(chunks)
            except Exception as e:
                body_incomplete = True
                info["error"] = f"{type(e).__name__}: {e}"
                info["verdict"] = "заголовки пришли, тело файла — нет"
                trace.add(f"{label}/{profile_name}", **info)
                trace.reason("портал отдал заголовки, но не отдал тело файла")
                continue

        info["size"] = len(content)
        head = content.lstrip()[:64].lower()
        is_html = ("text/html" in content_type
                   or head.startswith(b"<!doctype html") or head.startswith(b"<html"))
        if is_html:
            # Это осознанный ответ портала «ты не авторизован», а не сбой связи:
            # менять профиль клиента бессмысленно, нужен другой способ доступа.
            info["verdict"] = "вместо файла пришла HTML-страница (логин/ошибка)"
            trace.add(f"{label}/{profile_name}", **info)
            trace.reason("вместо файла пришла страница логина Битрикса")
            return None

        # Пустое (или подозрительно короткое) тело при 200 — тот же отказ, что и
        # обрыв чтения: портал ответил, но файла не дал. Проверяем ДО разбора
        # content-type, иначе пустой ответ с "Content-Type: application/pdf"
        # уехал бы дальше как «успешно скачанный» ноль байт.
        if len(content) < 512:
            body_incomplete = True
            info["verdict"] = "тело пустое или слишком короткое для файла"
            trace.add(f"{label}/{profile_name}", **info)
            trace.reason("портал ответил 200, но файла в ответе нет")
            continue

        if (content.startswith(b"%PDF")
                or "application/pdf" in content_type
                or "application/octet-stream" in content_type
                or len(content) > 1024):
            info["verdict"] = "ok"
            trace.add(f"{label}/{profile_name}", **info)
            return content

        info["verdict"] = "ответ не похож на файл"
        trace.add(f"{label}/{profile_name}", **info)
        trace.reason("портал вернул не файл")
        return None

    return None


def _rest_get(endpoint, method, params, trace, label):
    """GET к REST-методу Битрикса с записью результата в трассу.

    Возвращает `result` метода или None. Отдельная функция, потому что все
    способы достать файл начинаются одинаково: дёрнуть метод, посмотреть, не
    вернул ли Битрикс 200 с `{"error": ...}` в теле (он так умеет).
    """
    if trace.out_of_time():
        trace.add(f"{label}/{method}", error="бюджет времени исчерпан")
        return None
    try:
        resp = requests.get(
            f"{endpoint.rstrip('/')}/{method}.json",
            params=params, timeout=REST_TIMEOUT_SEC,
        )
    except Exception as e:
        trace.add(f"{label}/{method}", error=f"{type(e).__name__}: {e}")
        trace.reason(f"{method}: сеть недоступна")
        return None

    entry = {"status": resp.status_code}
    if resp.status_code != 200:
        entry["body"] = safe_preview(resp.text, 200)
        trace.add(f"{label}/{method}", **entry)
        trace.reason(f"{method} → HTTP {resp.status_code}")
        return None
    try:
        payload = resp.json()
    except Exception:
        entry["body"] = safe_preview(resp.text, 200)
        trace.add(f"{label}/{method}", **entry, error="ответ не JSON")
        return None
    if payload.get("error"):
        entry["error"] = str(payload.get("error"))
        entry["error_description"] = safe_preview(payload.get("error_description"), 200)
        trace.add(f"{label}/{method}", **entry)
        trace.reason(f"{method} → {payload.get('error')}")
        return None
    result = payload.get("result")
    if not result:
        trace.add(f"{label}/{method}", **entry, verdict="пустой result")
        return None
    trace.add(f"{label}/{method}", **entry, verdict="ok")
    return result


def _download_url_both_ways(dl, endpoint, access_token, trace, label):
    """Качает DOWNLOAD_URL сначала КАК ЕСТЬ, потом с подставленным auth.

    Порядок важен. Для входящего вебхука авторизация лежит В ПУТИ ссылки
    (`/rest/1/<код>/download/`), а `auth` в query Битрикс пытается проверить как
    OAuth-токен — и отвечает 401, хотя без него та же ссылка работает. То есть
    «подставим auth на всякий случай» ломает рабочий запрос. Поэтому свой токен
    подставляем только вторым заходом, когда ссылка как есть не сработала.
    """
    had_auth = "auth=" in (urlparse(dl).query or "")
    trace.add(f"{label}/url", url=_url_label(dl), had_auth=had_auth)

    content = try_download(dl, trace=trace, label=f"{label}/body")
    if content:
        return content

    token = access_token or endpoint.rstrip("/").split("/")[-1]
    if had_auth or not token:
        return None
    return try_download(_with_auth_param(dl, token), trace=trace,
                        label=f"{label}/body-auth")


def fetch_via_disk_file_get(endpoint, file_id, trace, label, access_token=None):
    """disk.file.get → DOWNLOAD_URL → качаем."""
    params = {"id": file_id}
    if access_token:
        params["auth"] = access_token
    result = _rest_get(endpoint, "disk.file.get", params, trace, label)
    if not result:
        return None
    dl = extract_download_url(result)
    if not dl:
        trace.add(f"{label}/disk.file.get", verdict="в ответе нет DOWNLOAD_URL")
        trace.reason("в ответе Битрикса нет ссылки на скачивание")
        return None
    return _download_url_both_ways(dl, endpoint, access_token, trace, label)


def fetch_via_attached_object(endpoint, file_id, trace, label, access_token=None):
    """disk.attachedObject.get — для файлов, прикреплённых к чату.

    У chat-attached файлов есть отдельный «attached object», доступный ВСЕМ
    участникам чата, даже если прав на сам файл в Диске нет. Битрикс в части
    конфигураций принимает сюда и file_id (сам находит привязанный объект),
    поэтому пробуем — если не найдёт, вернёт ERROR_NOT_FOUND и мы пойдём дальше.
    """
    params = {"id": file_id}
    if access_token:
        params["auth"] = access_token
    result = _rest_get(endpoint, "disk.attachedObject.get", params, trace, label)
    if not result:
        return None
    dl = extract_download_url(result)
    if not dl:
        trace.add(f"{label}/disk.attachedObject.get", verdict="нет DOWNLOAD_URL")
        return None
    return _download_url_both_ways(dl, endpoint, access_token, trace, label)


def _public_page_download_links(html_text, base_url):
    """Вытаскивает из страницы публичного просмотра ссылки на скачивание.

    `disk.file.getExternalLink` отдаёт адрес вида `/doc/<hash>` — это СТРАНИЦА
    просмотра, а не файл (в трассе это видно как «пришла HTML-страница, 71 КБ»).
    Кнопка «Скачать» на ней ведёт на отдельный адрес, который здесь и ищем.
    """
    found = re.findall(r'href=["\']([^"\']+)["\']', html_text, re.I)
    found += re.findall(r'["\']((?:https?:)?\\?/[^"\'\s<>]*?download[^"\'\s<>]*)["\']',
                        html_text, re.I)
    links, seen = [], set()
    for raw in found:
        if "download" not in raw.lower():
            continue
        url = raw.replace("\\/", "/").replace("&amp;", "&").strip()
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = base_url + url
        elif not url.lower().startswith("http"):
            continue
        if url not in seen:
            seen.add(url)
            links.append(url)
    return links[:5]


def fetch_via_external_link(endpoint, file_id, trace, label, access_token=None):
    """disk.file.getExternalLink — ПУБЛИЧНАЯ ссылка, качается вообще без auth.

    Последний способ в очереди, и намеренно: метод не просто читает файл, а
    СОЗДАЁТ на него публичную ссылку в портале. Для банковской выписки это
    лишняя (пусть и неугадываемая) точка доступа, поэтому идём сюда, только
    когда все авторизованные способы уже провалились и альтернатива — совсем
    не обработать выписку. Отключается env DISK_EXTERNAL_LINK_FALLBACK=0.

    Сама ссылка ведёт на страницу просмотра, а не на файл, поэтому: пробуем
    `?action=download`, а если не вышло — открываем страницу и берём ссылку
    скачивания из неё. Всё в одной сессии: страница ставит cookie, которым
    портал авторизует скачивание.
    """
    params = {"id": file_id}
    if access_token:
        params["auth"] = access_token
    link = _rest_get(endpoint, "disk.file.getExternalLink", params, trace, label)
    if not link or not isinstance(link, str):
        return None
    if link.startswith("/"):
        link = bitrix_portal_url() + link

    session = requests.Session()
    sep = "&" if urlparse(link).query else "?"
    content = try_download(f"{link}{sep}action=download", trace=trace,
                           label=f"{label}/action-download", session=session)
    if content:
        return content

    # Читаем саму страницу и ищем на ней кнопку «Скачать».
    if trace.out_of_time():
        return None
    try:
        page = session.get(link, timeout=(10, 20),
                           headers={"User-Agent": "Mozilla/5.0"})
    except Exception as e:
        trace.add(f"{label}/page", error=f"{type(e).__name__}: {e}")
        return None
    if page.status_code != 200:
        trace.add(f"{label}/page", status=page.status_code)
        return None

    candidates = _public_page_download_links(page.text, bitrix_portal_url())
    trace.add(f"{label}/page", status=200, size=len(page.content),
              found_links=len(candidates))
    if not candidates:
        trace.reason("на публичной странице нет ссылки на скачивание")
    for i, cand in enumerate(candidates):
        if trace.out_of_time():
            break
        content = try_download(cand, trace=trace,
                               label=f"{label}/page-link-{i + 1}", session=session)
        if content:
            return content
    return None


def _download_pdf(file_id, fallback_url, auth, trace):
    """Перебирает способы скачать PDF. Возвращает bytes или бросает ValueError.

    Каждый способ = свежий URL + немедленная закачка (ссылки Битрикса
    короткоживущие). Порядок — от самого «правильного» к самому отчаянному:

      0. REST от имени пользователя, приславшего файл (у него точно есть права).
      1. disk.attachedObject.get — доступ по факту участия в чате.
      2. disk.file.get основным вебхуком.
      3. disk.file.get disk-вебхуком (2 попытки).
      4. Прямая ссылка из payload (auth в query, затем Bearer).
      5. Публичная внешняя ссылка — крайняя мера, см. fetch_via_external_link.

    Всё уложено в DOWNLOAD_BUDGET_SEC; трасса попыток пишется в `trace`.
    """
    trace = trace or DownloadTrace()
    auth = auth or {}
    user_token    = (auth.get("access_token") or "").strip()
    user_endpoint = (auth.get("client_endpoint") or "").strip()
    if user_token and not user_endpoint:
        # Событие пришло без client_endpoint — выводим его из вебхука
        # (портал тот же самый).
        user_endpoint = derive_client_endpoint()

    trace.add("start", file_id=str(file_id or ""),
              user_context=bool(user_token and user_endpoint),
              has_fallback_url=bool(fallback_url))

    # 0. Контекст пользователя — самый надёжный способ, если токен прислали.
    if user_token and user_endpoint:
        result = fetch_via_disk_file_get(user_endpoint, file_id, trace,
                                         "user", access_token=user_token)
        if result:
            return result
    else:
        trace.add("user", verdict="в событии нет access_token — пропускаем")

    # 1. Attached object: спасает случай «сотрудник прислал файл из личного
    #    Диска» — прав на файл нет, а на вложение в чате есть.
    for endpoint, label, token in [
        (BITRIX_WEBHOOK_URL, "main-attached", user_token or None),
        (BITRIX_DISK_WEBHOOK_URL, "disk-attached", None),
    ]:
        result = fetch_via_attached_object(endpoint, file_id, trace, label,
                                           access_token=token)
        if result:
            return result

    # 2-3. Вебхуки портала. Больше двух попыток на disk-вебхук смысла не имеет:
    #      если портал «немой», он немой на всех попытках.
    result = fetch_via_disk_file_get(BITRIX_WEBHOOK_URL, file_id, trace, "main")
    if result:
        return result
    for attempt in range(2):
        if trace.out_of_time():
            break
        result = fetch_via_disk_file_get(
            BITRIX_DISK_WEBHOOK_URL, file_id, trace, f"disk-{attempt + 1}")
        if result:
            return result
        if attempt == 0:
            time.sleep(2)

    # 4. Прямая ссылка из payload. Сначала auth в query (работает для /rest/),
    #    затем Bearer (иногда проходит для /bitrix/).
    if fallback_url:
        for label, token in [("user", user_token),
                             ("main", BITRIX_WEBHOOK_URL.rstrip("/").split("/")[-1]),
                             ("disk", DISK_TOKEN)]:
            if not token:
                continue
            result = try_download(_with_auth_param(fallback_url, token),
                                  trace=trace, label=f"payload-url/{label}")
            if result:
                return result
            result = try_download(fallback_url,
                                  {"Authorization": f"Bearer {token}"},
                                  trace=trace, label=f"payload-url/{label}-bearer")
            if result:
                return result

    # 5. Крайняя мера — публичная ссылка (создаёт точку доступа, см. выше).
    if os.getenv("DISK_EXTERNAL_LINK_FALLBACK", "1").strip() != "0":
        for endpoint, label, token in [
            (user_endpoint, "user-extlink", user_token),
            (BITRIX_DISK_WEBHOOK_URL, "disk-extlink", None),
            (BITRIX_WEBHOOK_URL, "main-extlink", None),
        ]:
            if not endpoint:
                continue
            result = fetch_via_external_link(endpoint, file_id, trace, label,
                                             access_token=token or None)
            if result:
                return result
    else:
        trace.add("extlink", verdict="отключено DISK_EXTERNAL_LINK_FALLBACK=0")

    raise ValueError(
        f"Не удалось скачать файл из Битрикса ({trace.summary()}).\n\n"
        f"Рабочий обходной путь: загрузите выписку на странице "
        f"{APP_PUBLIC_URL}/upload — файл уйдёт в обработку напрямую из браузера, "
        f"минуя Диск Битрикса.\n\n"
        f"Чтобы починить причину, откройте {APP_PUBLIC_URL}/download-test?file_id={file_id} "
        f"— там полная трасса попыток."
    )


def get_pdf_bytes(file_id, fallback_url=None, auth=None, trace=None):
    """Скачивает PDF из Битрикса и запоминает трассу попыток для /check.

    Тонкая обёртка над `_download_pdf`: сам перебор способов там, здесь —
    только фиксация результата, чтобы причина провала не оставалась
    исключительно в логах Railway.
    """
    trace = trace or DownloadTrace()
    try:
        content = _download_pdf(file_id, fallback_url, auth, trace)
    except Exception:
        _record_download(file_id, False, trace)
        raise
    _record_download(file_id, True, trace)
    return content


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


def handle_cancel_command(data):
    """Клик по кнопке «Отменить заявку» (ONIMCOMMANDADD) — в фоне.

    Обёртка над `_process_cancel_command`: сам /bot отвечает Битриксу сразу,
    а отмена (чтение/запись Sheets + сообщения в чат) идёт в отдельном потоке.
    """
    try:
        _process_cancel_command(data)
    except Exception as e:
        print(f"[cancel-command] ERROR: {e}")


def _process_cancel_command(data):
    """Разбирает payload команды и выполняет отмену заявки.

    Битрикс присылает поля в bracket-notation, напр.:
      data[COMMAND][0][COMMAND]        = 'cancelpay'
      data[COMMAND][0][COMMAND_PARAMS] = '<rid>'
      data[USER][ID]                   = <id нажавшего>
    Разбираем устойчиво (имена индексов могут отличаться) — полный payload
    печатается выше в логах, если что-то не совпадёт.
    """
    # ID заявки — из COMMAND_PARAMS.
    rid = ""
    command = ""
    for k, v in data.items():
        if k.endswith("[COMMAND_PARAMS]") and v:
            rid = str(v).strip()
        elif k.endswith("[COMMAND]") and v:
            command = str(v).strip()

    # Кто нажал кнопку.
    user_id = str(data.get("data[USER][ID]") or "").strip()
    if not user_id:
        for k, v in data.items():
            if (k.endswith("[FROM_USER_ID]") or k.endswith("[AUTHOR_ID]")
                    or k.endswith("[USER_ID]")) and v:
                user_id = str(v).strip()
                break

    print(f"[cancel-command] command={command!r} rid={rid!r} user_id={user_id!r}")

    if command and command != CANCEL_COMMAND:
        print("[cancel-command] другая команда — пропускаем")
        return

    ok, message = _do_cancel_payment(rid, user_id)
    print(f"[cancel-command] result ok={ok} msg={message!r}")
    # Если отменить нельзя (не заявитель / уже отменена) — личное сообщение
    # нажавшему, чтобы он понял, почему ничего не произошло.
    if not ok and user_id:
        send_message(user_id, f"⚠️ {message}")


@app.route("/bot", methods=["GET", "POST"])
def bot_handler():
    """Приём событий Битрикса. Отвечает СРАЗУ, работу делает в фоне.

    Раньше вся обработка шла внутри запроса: поиск PDF по истории чата (до 15с),
    user.get за ФИО (до 10с) и imbot.message.add (до 15с) — суммарно больше
    таймаута gunicorn/Битрикса. Если воркер убивали по таймауту, ответа не было
    вообще: Битрикс считал доставку неудачной, а в чате — тишина. Теперь
    единственное, что делает запрос, — фиксирует событие и отдаёт 200.
    """
    if request.method == "GET":
        return jsonify({"result": "ok"})

    data = parse_request_data()
    print("===== INCOMING REQUEST =====")
    print(safe_preview(data, 5000))

    event = data.get("event", "")
    print(f"EVENT: {event}")

    dialog_id = (
        data.get("data[PARAMS][DIALOG_ID]")
        or data.get("data[PARAMS][TO_CHAT_ID]")
    )
    # Фиксируем ЛЮБОЕ событие — по этой метке /check отличает «Битрикс молчит»
    # от «Битрикс шлёт, а мы не можем ответить».
    _record_event(event, dialog_id)

    # Клик по кнопке «Отменить заявку» → бот-команда ONIMCOMMANDADD.
    if event == "ONIMCOMMANDADD":
        threading.Thread(
            target=handle_cancel_command, args=(data,), daemon=True
        ).start()
        return jsonify({"result": "ok", "queued": "cancel_command"})

    if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
        return jsonify({"result": "ok", "skipped": True})

    threading.Thread(
        target=_handle_message_event, args=(data, dialog_id), daemon=True
    ).start()
    return jsonify({"result": "ok", "queued": event})


def _handle_message_event(data, dialog_id):
    """Разбор сообщения из чата: найти PDF (или ответить текстом). В фоне."""
    try:
        _handle_message_event_inner(data, dialog_id)
    except Exception as e:
        print(f"_handle_message_event ERROR: {e}")
        # Не молчим: если разбор упал, пользователь должен это увидеть.
        send_message(dialog_id, f"❌ Ошибка при разборе сообщения: {e}")


def _handle_message_event_inner(data, dialog_id):
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
        _annotate_last_event({"pdf": filename, "file_id": str(file_id)})
        uploader = extract_uploader_name(data)
        # Достаём auth-токен пользователя из события — нужен для скачивания
        # файлов, загруженных НЕ владельцем вебхука (см. get_pdf_bytes).
        auth = parse_auth_from_event(data)
        # В «Платежах» не анонсируем приём сразу — сначала ИИ проверит, выписка
        # ли это (иначе на чек бот бы написал «Получил PDF…»).
        if not is_pay:
            send_message(dialog_id, "📄 Получил PDF, начинаю обработку...")
        process_pdf_async(
            dialog_id, file_id, fallback_url, uploader, auth,
            require_statement_check=is_pay,
        )

    elif is_pay:
        # В чате «Платежи» на текст/чеки/прочее без выписки — молчим.
        print("Платежи: не выписка — молчим")
        return

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


def check_bot_registration():
    """Зарегистрирован ли наш бот в портале (imbot.bot.list).

    Если бота в списке нет — Битриксу некуда слать события и нечем отвечать:
    в чате будет полная тишина при живом сервере. Лечится /install-app.
    """
    info = {"ok": None, "expected_bot_id": BITRIX_BOT_ID or "(не задан)", "bots": []}
    try:
        resp = bitrix_post("imbot.bot.list", {}, timeout=15)
        try:
            body = resp.json()
        except Exception:
            body = {}
        if resp.status_code != 200 or body.get("error"):
            info["ok"] = False
            info["error"] = str(body.get("error_description") or body.get("error")
                                or f"HTTP {resp.status_code}")
            return info
        result = body.get("result") or {}
        # Битрикс отдаёт либо dict {bot_id: {...}}, либо список.
        items = result.items() if isinstance(result, dict) else enumerate(result)
        ids = []
        for key, bot in items:
            bot = bot if isinstance(bot, dict) else {}
            bot_id = str(bot.get("ID") or bot.get("id") or key)
            ids.append(bot_id)
            info["bots"].append({"id": bot_id, "code": bot.get("CODE") or bot.get("code") or ""})
        info["ok"] = bool(BITRIX_BOT_ID) and str(BITRIX_BOT_ID) in ids
        if not ids:
            info["ok"] = False
    except Exception as e:
        info["ok"] = False
        info["error"] = str(e)[:200]
    return info


def bot_delivery_report():
    """Проверки, которые нужны именно при жалобе «бот молчит»."""
    bot = check_bot_registration()
    with _DIAG_LOCK:
        diag = json.loads(json.dumps(_DIAG))  # снимок, чтобы не отдавать живой dict

    problems = []
    if bot.get("ok") is False:
        if not BITRIX_BOT_ID:
            problems.append("❌ Бот: не задан BITRIX_BOT_ID — боту нечем "
                            "подписаться на чат и нечем отвечать")
        elif bot.get("error"):
            problems.append(f"❌ Бот: не удалось получить список ботов — {bot['error']}")
        else:
            problems.append(
                f"❌ Бот: BITRIX_BOT_ID={BITRIX_BOT_ID} не найден в портале "
                f"(есть: {[b['id'] for b in bot.get('bots', [])] or 'ни одного'}) "
                f"→ прогнать /install-app"
            )

    if diag["events_total"] == 0:
        problems.append(
            f"⚠️ Битрикс не присылал ни одного события с момента старта сервера "
            f"({diag['started_at']}). Если в чат за это время писали — Битрикс НЕ "
            f"доставляет события на /bot: проверить регистрацию бота (/install-app)."
        )
    if diag.get("last_send") and not diag["last_send"].get("ok"):
        problems.append(
            f"❌ Последняя отправка сообщения ПРОВАЛИЛАСЬ "
            f"({diag['last_send']['at']}): {diag['last_send']['error']}. "
            f"События доходят, но бот не может ответить в чат."
        )
    if diag["send_errors"] and diag["send_errors"] == diag["sends_total"]:
        problems.append("❌ Ни одна отправка в чат не прошла с момента старта сервера.")

    last_dl = diag.get("last_download")
    if last_dl and not last_dl.get("ok"):
        problems.append(
            f"❌ Последнее скачивание файла из Битрикса провалилось "
            f"({last_dl['at']}, file_id={last_dl['file_id']}): {last_dl['reason']}. "
            f"Обходной путь для пользователя — {APP_PUBLIC_URL}/upload; полная "
            f"трасса — {APP_PUBLIC_URL}/download-test?file_id={last_dl['file_id']}"
        )
    if isinstance(last_dl, dict):
        # В /check хватает хвоста трассы: целиком её отдаёт /download-test.
        steps = last_dl.get("steps") or []
        last_dl["steps_total"] = len(steps)
        last_dl["steps"] = steps[-12:]

    return problems, {"bot": bot, "activity": diag}


@app.route("/check", methods=["GET"])
def check_services_route():
    """Первый шаг при «бот не работает».

    Показывает и доступность сервисов (Anthropic / Sheets / Bitrix), и то,
    доходят ли события от Битрикса и уходят ли сообщения обратно в чат.
    """
    try:
        problems = check_all_services()
        delivery_problems, delivery = bot_delivery_report()
        problems = problems + delivery_problems
        payload = {
            "ok": not problems,
            "version": deployed_version(),
            "env": {
                "ANTHROPIC_API_KEY": bool(ANTHROPIC_API_KEY),
                "GOOGLE_CREDENTIALS": GOOGLE_CREDS_JSON is not None,
                "BITRIX_BOT_ID": BITRIX_BOT_ID or "",
                "BOT_CLIENT_ID_set": bool(BOT_CLIENT_ID),
                "PAYMENT_CHAT_ID": PAYMENT_CHAT_ID,
            },
            **delivery,
        }
        if problems:
            payload["problems"] = problems
        else:
            payload["message"] = "Все сервисы работают ✅"
        return jsonify(payload), 200, NO_CACHE_HEADERS
    except Exception as e:
        return jsonify({"ok": False, "error": str(e),
                        "version": deployed_version()}), 200, NO_CACHE_HEADERS


@app.route("/download-test", methods=["GET"])
def download_test_route():
    """Диагностика скачивания файла из Битрикса — трасса всех попыток.

    Симптом «не удалось скачать файл» — это пять разных причин (нет прав,
    портал молчит, вернулась страница логина, истёк токен, файла нет),
    и снаружи они неотличимы. Логи Railway пользователю недоступны, поэтому
    трассу отдаём HTTP-эндпоинтом:

        /download-test?file_id=12345        — по ID файла на Диске
        /download-test?dialog_id=chat7552   — взять последний PDF из чата

    Ничего не пишет в таблицу: только качает и показывает, что произошло.
    """
    file_id = (request.args.get("file_id") or "").strip()
    dialog_id = (request.args.get("dialog_id") or "").strip()
    fallback_url = None
    filename = ""

    if not file_id and dialog_id:
        found = find_recent_pdf_in_chat(dialog_id, limit=10)
        if not found:
            return jsonify({
                "ok": False,
                "error": f"В последних сообщениях {dialog_id} не нашли PDF. "
                         f"ID чата можно посмотреть на {APP_PUBLIC_URL}/chats",
                "version": deployed_version(),
            }), 200, NO_CACHE_HEADERS
        file_id = found["file_id"]
        filename = found.get("filename") or ""
        fallback_url = found.get("url_download")

    if not file_id:
        return jsonify({
            "ok": False,
            "error": "Укажите ?file_id=… или ?dialog_id=chatNNNN",
            "version": deployed_version(),
        }), 200, NO_CACHE_HEADERS

    trace = DownloadTrace()
    payload = {"version": deployed_version(), "file_id": file_id,
               "filename": filename}
    try:
        content = get_pdf_bytes(file_id, fallback_url=fallback_url, trace=trace)
        payload["ok"] = True
        payload["size"] = len(content)
        payload["looks_like_pdf"] = content.startswith(b"%PDF")
        payload["message"] = ("Файл скачался. Значит проблема не в доступе к "
                              "Диску — смотрите остальные шаги обработки.")
    except Exception as e:
        payload["ok"] = False
        payload["error"] = str(e)
        payload["reasons"] = trace.reasons
        payload["hint"] = (f"Пока не починено — выписки можно грузить через "
                           f"{APP_PUBLIC_URL}/upload")
    payload["steps"] = trace.steps
    return jsonify(payload), 200, NO_CACHE_HEADERS


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# ─────────────────────────────────────────────
# Загрузка выписки через веб-форму (обход Bitrix-диска)
# Bytes файла приходят прямо из браузера → не зависим от скачивания из Bitrix.
# ─────────────────────────────────────────────

_UPLOAD_RESULTS = []  # последние результаты обработки (для показа на странице)


def _push_upload_result(status, text):
    _UPLOAD_RESULTS.insert(0, {"status": status, "text": text})
    del _UPLOAD_RESULTS[10:]  # держим последние 10


def _process_uploaded_statement(pdf_bytes, uploader=""):
    """Фоновая обработка выписки, загруженной через /upload. Не зависит от
    скачивания файла из Bitrix — байты уже у нас. Результат кладём в
    _UPLOAD_RESULTS, чтобы показать на странице после обновления."""
    from datetime import timezone, timedelta
    t = datetime.now(timezone(timedelta(hours=3))).strftime("%H:%M")
    try:
        problems = check_all_services()
        if problems:
            msg = "⚠️ Проблемы с сервисами: " + "; ".join(problems)
            _push_upload_result("error", f"{t} — {msg}")
            print("upload: " + msg)
            return
        account_owner, transactions = extract_transactions(pdf_bytes)
        clarify_list, skipped = write_to_sheets(
            transactions, uploader=uploader, account_owner=account_owner)
        total_in = sum(float(x.get("amount", 0) or 0)
                       for x in transactions if x.get("type") == "in")
        total_out = sum(float(x.get("amount", 0) or 0)
                        for x in transactions if x.get("type") == "out")
        dup = f", дублей пропущено: {skipped}" if skipped else ""
        _push_upload_result(
            "ok",
            f"{t} — ✅ Готово: {len(transactions)} транзакций{dup}. "
            f"Владелец: {account_owner or '—'}. "
            f"Поступления {total_in:,.0f} ₽ / списания {total_out:,.0f} ₽."
        )
        print(f"upload: готово, транзакций={len(transactions)} "
              f"владелец={account_owner!r} дублей={skipped}")
    except Exception as e:
        _push_upload_result("error", f"{t} — ❌ Ошибка: {e}")
        print(f"upload process ERROR: {e}")


def _render_upload_page(banner=""):
    rows = ""
    for r in _UPLOAD_RESULTS:
        bg = "#e8f5e9" if r["status"] == "ok" else "#fdecea"
        rows += f'<div class="res" style="background:{bg}">{r["text"]}</div>'
    if not rows:
        rows = ('<div class="muted">Пока пусто. Загрузите выписку — '
                'результат появится здесь.</div>')
    banner_html = f'<div class="banner">{banner}</div>' if banner else ""
    return f"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Загрузка выписки · ДДС</title>
<style>
*{{box-sizing:border-box}}body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
margin:0;background:#eef2f6;color:#1f2a37}}
.card{{max-width:640px;margin:0 auto;background:#fff;min-height:100vh}}
.head{{background:linear-gradient(135deg,#2fc6f6,#1f8ee0);color:#fff;padding:24px 20px}}
.head h1{{margin:0 0 6px;font-size:22px}}.head p{{margin:0;opacity:.95;font-size:14px}}
.body{{padding:20px}}
label{{display:block;font-weight:600;font-size:14px;margin:14px 0 6px}}
input[type=file],input[type=text]{{width:100%;padding:12px;border:1px solid #cfd8e3;
border-radius:10px;font-size:15px;background:#fff}}
button{{margin-top:18px;width:100%;padding:14px;border:0;border-radius:10px;
background:#1f8ee0;color:#fff;font-size:16px;font-weight:600;cursor:pointer}}
button:active{{background:#166fb0}}
.sheet{{display:block;text-align:center;margin:16px 0 6px;color:#1f8ee0;
text-decoration:none;font-weight:600}}
h2{{font-size:15px;margin:22px 0 10px;color:#5b6b7b}}
.res{{padding:10px 12px;border-radius:8px;font-size:14px;margin-bottom:8px;line-height:1.4}}
.muted{{color:#8a97a6;font-size:14px}}
.banner{{margin:16px 20px 0;padding:12px 14px;border-radius:10px;background:#e7f3ff;
border:1px solid #b6ddff;font-size:14px;line-height:1.45}}
</style></head>
<body><div class="card">
 <div class="head"><h1>📄 Загрузка выписки</h1>
 <p>Выберите PDF-выписку Сбербанка — бот разнесёт транзакции в таблицу «Расходы Сбер».</p></div>
 {banner_html}
 <div class="body">
  <form method="post" action="/upload/submit" enctype="multipart/form-data">
   <label>Файл выписки (PDF)</label>
   <input type="file" name="file" accept="application/pdf,.pdf" required>
   <label>Кто загрузил (необязательно)</label>
   <input type="text" name="uploader" placeholder="Имя">
   <button type="submit">Загрузить и обработать</button>
  </form>
  <a class="sheet" href="{SHEET_URL}" target="_blank">🔗 Открыть таблицу «Расходы Сбер»</a>
  <h2>Последние загрузки</h2>
  {rows}
 </div>
</div></body></html>"""


@app.route("/upload", methods=["GET"])
def upload_page():
    return Response(_render_upload_page(), mimetype="text/html; charset=utf-8")


@app.route("/upload/submit", methods=["POST"])
def upload_submit():
    f = request.files.get("file")
    if not f or not (f.filename or "").lower().endswith(".pdf"):
        return Response(
            _render_upload_page(banner="❌ Нужен PDF-файл выписки."),
            mimetype="text/html; charset=utf-8")
    pdf_bytes = f.read()
    if not pdf_bytes:
        return Response(
            _render_upload_page(banner="❌ Файл пустой, попробуйте ещё раз."),
            mimetype="text/html; charset=utf-8")
    uploader = (request.form.get("uploader") or "").strip()
    # Обрабатываем в фоне, чтобы не упереться в таймаут веб-сервера.
    threading.Thread(
        target=_process_uploaded_statement,
        args=(pdf_bytes, uploader),
        daemon=True,
    ).start()
    return Response(
        _render_upload_page(
            banner="✅ Выписка принята, обрабатываю… Обновите страницу через "
                   "30–60 секунд — результат появится в списке «Последние "
                   "загрузки» ниже и в таблице."),
        mimetype="text/html; charset=utf-8")


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
        summary = init_sheets() or {}
        return jsonify({
            "ok": True,
            "message": "Таблица инициализирована",
            "version": deployed_version(),
            **summary,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "version": deployed_version()})


@app.route("/version", methods=["GET"])
def version_route():
    """Какая версия кода реально крутится на сервере.

    Нужен, чтобы не гадать «задеплоилось или нет»: если после пуша тут старый
    SHA — Railway ещё собирает (или деплой упал), и дёргать /init-sheets рано.
    """
    return jsonify({
        "version": deployed_version(),
        "payment_categories_default": PAYMENT_CATEGORIES_DEFAULT,
        "payment_categories_retired": PAYMENT_CATEGORIES_RETIRED,
    })


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


def _pick_disk_storage(storages):
    """Выбирает хранилище для загрузки счёта.

    Приоритет — ОБЩИЙ диск компании (ENTITY_TYPE == "common"): он виден всем
    сотрудникам, поэтому бухгалтер/плательщик сможет открыть файл. Личный диск
    (ENTITY_TYPE == "user") — приватный, файл в нём даёт «Доступ запрещён»
    любому, кроме владельца вебхука. Фолбэк — первое доступное хранилище.
    """
    def etype(s):
        return str(s.get("ENTITY_TYPE") or "").lower()

    for s in storages:
        if etype(s) == "common":
            return s
    # Хоть что-то, но не личный диск, если есть выбор.
    for s in storages:
        if etype(s) != "user":
            return s
    return storages[0]


def upload_invoice_to_disk(filename, content_bytes):
    """Загружает файл счёта на Bitrix-диск, возвращает ссылку для просмотра.

    Кладёт файл в ОБЩИЙ диск компании (см. `_pick_disk_storage`), чтобы его мог
    открыть любой сотрудник (плательщик/бухгалтер), а не только владелец вебхука.
    Дополнительно пытается получить публичную внешнюю ссылку
    (`disk.file.getExternalLink`) — она открывается вообще без проверки прав,
    поэтому используется как самая надёжная. Фолбэк — абсолютный DETAIL_URL.
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
        storage = _pick_disk_storage(storages)
        storage_id = storage["ID"]
        print(f"upload_invoice_to_disk: хранилище ID={storage_id} "
              f"ENTITY_TYPE={storage.get('ENTITY_TYPE')} NAME={storage.get('NAME')}")

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
        file_id = f.get("ID")

        # Публичная внешняя ссылка — открывается без авторизации, поэтому
        # надёжнее всего решает «Доступ запрещён». Работает, если в портале
        # включены внешние ссылки на файлы диска; иначе — фолбэк на DETAIL_URL.
        ext_link = _disk_external_link(file_id)
        if ext_link:
            return ext_link

        link = (f.get("DETAIL_URL") or f.get("DOWNLOAD_URL") or "").strip()
        # DETAIL_URL у диска часто относительный ("/company/personal/...") —
        # делаем абсолютным, иначе превью в чате видит «несуществующий домен».
        if link.startswith("/"):
            link = bitrix_portal_url() + link
        return link
    except Exception as e:
        print(f"upload_invoice_to_disk error: {e}")
        return ""


def _disk_external_link(file_id):
    """Публичная (без авторизации) ссылка на файл диска или "".

    disk.file.getExternalLink возвращает URL, который открывается без входа в
    портал — им может воспользоваться любой, у кого есть ссылка. Если фича
    внешних ссылок в портале выключена, метод вернёт ошибку — тогда "".
    """
    if not file_id:
        return ""
    try:
        resp = bitrix_disk_post("disk.file.getExternalLink", {"id": file_id})
        if resp.status_code != 200:
            print(f"_disk_external_link status={resp.status_code} body={safe_preview(resp.text,200)}")
            return ""
        link = (resp.json().get("result") or "").strip()
        if link.startswith("/"):
            link = bitrix_portal_url() + link
        return link
    except Exception as e:
        print(f"_disk_external_link error: {e}")
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
        headers=NO_CACHE_HEADERS,
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
                # Оборачиваем в [url] — иначе Битрикс строит превью-карточку
                # ссылки (и показывает 404, когда не может её развернуть).
                lines.append(f"📎 [url={file_link}]Открыть счёт[/url]")
            lines.append("")
            lines.append(f"[USER={payer_id}]{payer_name}[/USER], нужно оплатить 🙏")
            # Кнопка отмены — это БОТ-КОМАНДА (а не ссылка): по клику Битрикс
            # шлёт боту событие ONIMCOMMANDADD с ID нажавшего, без браузера и
            # BX24. Это надёжно и даёт проверить, что отменяет именно заявитель.
            # Команда регистрируется через /register-cancel-command.
            keyboard = [{
                "TEXT": "❌ Отменить заявку",
                "COMMAND": CANCEL_COMMAND,
                "COMMAND_PARAMS": rid,
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


def _do_cancel_payment(rid, user_id):
    """Общая логика отмены заявки. Возвращает (ok: bool, message: str).

    Отменить может ТОЛЬКО заявитель (user_id == requester_id из таблицы).
    Используется и веб-страницей `/pay/cancel/confirm`, и обработчиком
    кнопки-команды в чате (ONIMCOMMANDADD).
    """
    rid = (rid or "").strip()
    user_id = str(user_id or "").strip()
    if not rid:
        return False, "Не указан ID заявки"

    row_number, values = find_payment_row_by_rid(rid)
    if not row_number:
        return False, "Заявка не найдена"

    status        = (values[11] if len(values) > 11 else "").strip()
    requester_id  = (values[13] if len(values) > 13 else "").strip()
    message_id    = (values[14] if len(values) > 14 else "").strip()
    requester_nm  = (values[1] if len(values) > 1 else "").strip()
    amount        = (values[3] if len(values) > 3 else "").strip()
    recipient     = (values[4] if len(values) > 4 else "").strip()
    category      = (values[2] if len(values) > 2 else "").strip()

    if status == "Отменена":
        return False, "Заявка уже отменена"

    # Отменить может автор заявки ИЛИ один из PAYMENT_CANCEL_EXTRA_NAMES
    # (по умолчанию — Анастасия Фаткуллина).
    is_requester = bool(user_id) and user_id == str(requester_id)
    canceller_name = requester_nm if is_requester else ""
    allowed = is_requester
    if not allowed and user_id:
        canceller_name = resolve_user_name(user_id)
        low = canceller_name.lower()
        if any(sub.lower() in low for sub in PAYMENT_CANCEL_EXTRA_NAMES):
            allowed = True
    if not allowed:
        extra = " или ".join(PAYMENT_CANCEL_EXTRA_NAMES) or "ответственный"
        return False, f"Отменить может только заявитель ({requester_nm}) или {extra}"

    set_payment_status(row_number, "Отменена")

    by_whom = "заявителем" if is_requester else canceller_name

    # Правим исходное сообщение в чате — помечаем отменённым и убираем кнопку.
    if message_id:
        cancelled = "\n".join([
            "❌ [B]ЗАЯВКА ОТМЕНЕНА[/B]",
            f"👤 Заявитель: {requester_nm}",
            f"💰 Сумма: {amount}",
            f"📂 Категория: {category}",
            f"🏦 Получатель: {recipient}",
            "",
            f"[I]Отменена: {by_whom}[/I]",
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
            f"[I]Отменил(а): {by_whom}[/I]",
        ]))

    return True, "Заявка помечена как отменённая"


@app.route("/pay/cancel/confirm", methods=["POST"])
def payment_cancel_confirm_route():
    """Выполняет отмену заявки: проверяет автора, ставит статус, правит сообщение."""
    try:
        rid = (request.form.get("rid") or "").strip()
        user_id = (request.form.get("user_id") or "").strip()
        ok, message = _do_cancel_payment(rid, user_id)
        return jsonify({"ok": ok, "message": message})
    except Exception as e:
        print(f"payment_cancel_confirm_route error: {e}")
        return jsonify({"ok": False, "message": "Внутренняя ошибка"})


def register_cancel_command():
    """Регистрирует бот-команду «Отменить заявку» через входящий вебхук.

    Делается один раз (дёрнуть /register-cancel-command). После этого кнопка
    COMMAND в уведомлении начнёт присылать боту ONIMCOMMANDADD на /bot.
    Требует BITRIX_BOT_ID (BOT_ID нашего чат-бота).
    """
    if not BITRIX_BOT_ID:
        return False, "Не задан BITRIX_BOT_ID"
    payload = {
        "BOT_ID": BITRIX_BOT_ID,
        "CLIENT_ID": BOT_CLIENT_ID,
        "COMMAND": CANCEL_COMMAND,
        "COMMON": "N",
        "HIDDEN": "Y",
        "EXTRANET": "N",
        "EVENT_COMMAND_ADD": f"{APP_PUBLIC_URL}/bot",
        "LANG": [{"LANGUAGE_ID": "ru", "TITLE": "Отмена заявки на оплату"}],
    }
    try:
        resp = bitrix_post("imbot.command.register", payload, timeout=20)
        body = resp.json()
    except Exception as e:
        return False, f"Ошибка запроса: {e}"
    if body.get("error"):
        # Команда уже зарегистрирована — это не ошибка для нас.
        desc = body.get("error_description") or body.get("error")
        if "exists" in str(desc).lower() or "already" in str(desc).lower():
            return True, f"Команда уже зарегистрирована: {desc}"
        return False, f"{desc}"
    return True, f"Команда «{CANCEL_COMMAND}» зарегистрирована (result={body.get('result')})"


@app.route("/register-cancel-command", methods=["GET"])
def register_cancel_command_route():
    ok, message = register_cancel_command()
    icon = "✅" if ok else "❌"
    return Response(
        f"<!DOCTYPE html><html lang='ru'><head><meta charset='utf-8'></head>"
        f"<body style='font-family:system-ui,sans-serif;padding:32px;'>"
        f"<h2>{icon} Регистрация кнопки отмены</h2><p>{message}</p></body></html>",
        mimetype="text/html; charset=utf-8",
        status=200 if ok else 500,
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
    return Response(html, mimetype="text/html; charset=utf-8",
                    headers=NO_CACHE_HEADERS)


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
