import os
import json
import requests
from urllib.parse import parse_qs
from flask import Flask, request, jsonify

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL", "").rstrip("/")


def parse_request_data():
    """Парсим входящий запрос в любом формате."""
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


def safe_preview(value, limit=2000):
    """Безопасно режем длинные строки для логов."""
    try:
        text = str(value)
    except Exception:
        text = repr(value)

    if len(text) > limit:
        return text[:limit] + " ...[truncated]"
    return text


def send_message(dialog_id, text):
    """Пробуем отправить сообщение в чат."""
    if not dialog_id:
        print("send_message skipped: no dialog_id")
        return None

    if not BITRIX_WEBHOOK_URL:
        print("send_message skipped: BITRIX_WEBHOOK_URL is empty")
        return None

    try:
        r = requests.post(
            f"{BITRIX_WEBHOOK_URL}/im.message.add.json",
            json={"DIALOG_ID": dialog_id, "MESSAGE": text},
            timeout=15,
        )
        print(f"send_message status={r.status_code}")
        print(f"send_message response={safe_preview(r.text, 3000)}")
        return r
    except Exception as e:
        print(f"send_message error: {e}")
        return None


def call_bitrix_get(method_name, params=None, timeout=20):
    """Универсальный GET-вызов Bitrix API."""
    if not BITRIX_WEBHOOK_URL:
        print(f"{method_name} skipped: BITRIX_WEBHOOK_URL is empty")
        return None, None

    url = f"{BITRIX_WEBHOOK_URL}/{method_name}.json"
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        print(f"{method_name} status={r.status_code}")
        print(f"{method_name} response={safe_preview(r.text, 5000)}")
        try:
            return r, r.json()
        except Exception:
            return r, None
    except Exception as e:
        print(f"{method_name} error: {e}")
        return None, None


def download_url_for_debug(url):
    """Диагностическая загрузка URL, чтобы увидеть что реально приходит."""
    if not url:
        print("download_url_for_debug skipped: empty url")
        return

    print(f"DEBUG DOWNLOAD URL: {safe_preview(url, 500)}")

    session = requests.Session()
    attempts = [url]

    for idx, try_url in enumerate(attempts, start=1):
        try:
            r = session.get(
                try_url,
                timeout=30,
                allow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0"},
            )

            content_type = r.headers.get("Content-Type")
            location = r.headers.get("Location")
            first_bytes = r.content[:20]

            print(f"download attempt #{idx}")
            print(f"status={r.status_code}")
            print(f"final_url={safe_preview(r.url, 500)}")
            print(f"content_type={content_type}")
            print(f"location={location}")
            print(f"size={len(r.content)}")
            print(f"first_bytes={first_bytes}")

            text_preview = ""
            try:
                text_preview = r.text[:1000]
            except Exception:
                text_preview = str(r.content[:200])

            print(f"text_preview={safe_preview(text_preview, 1200)}")

        except Exception as e:
            print(f"download attempt #{idx} error: {e}")


def find_pdf_in_payload(data):
    """
    Ищем PDF в плоском payload Bitrix.
    Возвращаем:
    - file_id
    - chat_id
    - url_download
    - url_show
    - unified_link
    - filename
    """
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

    # Совсем запасной путь
    file_id = data.get("data[PARAMS][FILE_ID][0]")
    if file_id:
        result["file_id"] = file_id
        result["filename"] = "document.pdf"

    return result


def extract_disk_id_from_im_chat_files(chat_files_json, target_file_id):
    """Пытаемся достать diskId из ответа im.chat.files.get."""
    if not chat_files_json:
        return None

    result = chat_files_json.get("result", {})
    files = result.get("files", [])

    if isinstance(files, dict):
        files = list(files.values())

    for item in files:
        if str(item.get("id")) == str(target_file_id):
            disk_id = item.get("diskId") or item.get("DISK_ID")
            if disk_id:
                return disk_id

    return None


def debug_bitrix(dialog_id, file_id, chat_id, url_download=None, url_show=None, unified_link=None):
    """Главная диагностика Bitrix."""
    print("========== BITRIX DEBUG START ==========")
    print(f"BITRIX_WEBHOOK_URL={safe_preview(BITRIX_WEBHOOK_URL, 300)}")
    print(f"dialog_id={dialog_id}")
    print(f"file_id={file_id}")
    print(f"chat_id={chat_id}")
    print(f"url_download={safe_preview(url_download, 500)}")
    print(f"url_show={safe_preview(url_show, 500)}")
    print(f"unified_link={safe_preview(unified_link, 500)}")

    # 1. Проверка отправки сообщения
    print("----- TEST im.message.add -----")
    send_message(dialog_id, "Тест: бот получил PDF и запускает диагностику.")

    # 2. Проверка im.chat.files.get
    disk_id = None
    if chat_id:
        print("----- TEST im.chat.files.get -----")
        _, chat_files_json = call_bitrix_get(
            "im.chat.files.get",
            params={"CHAT_ID": chat_id, "LIMIT": 50},
            timeout=20,
        )
        disk_id = extract_disk_id_from_im_chat_files(chat_files_json, file_id)
        print(f"EXTRACTED disk_id from im.chat.files.get: {disk_id}")
    else:
        print("chat_id is empty, skip im.chat.files.get")

    # 3. Проверка disk.file.get по disk_id
    if disk_id:
        print("----- TEST disk.file.get by disk_id -----")
        call_bitrix_get("disk.file.get", params={"id": disk_id}, timeout=20)
    else:
        print("disk_id not found, skip disk.file.get by disk_id")

    # 4. Иногда file_id и есть disk id — тоже проверим
    if file_id:
        print("----- TEST disk.file.get by file_id -----")
        call_bitrix_get("disk.file.get", params={"id": file_id}, timeout=20)
    else:
        print("file_id is empty, skip disk.file.get by file_id")

    # 5. Проверяем загрузку urlDownload
    if url_download:
        print("----- TEST raw download urlDownload -----")
        download_url_for_debug(url_download)
    else:
        print("url_download is empty, skip raw urlDownload")

    # 6. Проверяем urlShow
    if url_show:
        print("----- TEST raw download urlShow -----")
        download_url_for_debug(url_show)
    else:
        print("url_show is empty, skip raw urlShow")

    # 7. unifiedLink
    if unified_link:
        print("----- TEST raw download unified_link -----")
        download_url_for_debug(unified_link)
    else:
        print("unified_link is empty, skip unified_link")

    print("========== BITRIX DEBUG END ==========")


@app.route("/bot", methods=["GET", "POST"])
def bot_handler():
    if request.method == "GET":
        return jsonify({"result": "ok", "mode": "debug"})

    data = parse_request_data()

    print("===== INCOMING REQUEST =====")
    print(safe_preview(data, 8000))

    event = data.get("event", "")
    print(f"EVENT: {event}")

    if event not in ("ONIMBOTMESSAGEADD", "ONIMJOINCHAT"):
        print("Skip unsupported event")
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

    file_info = find_pdf_in_payload(data)

    print("===== FOUND FILE INFO =====")
    print(safe_preview(file_info, 3000))

    file_id = file_info.get("file_id")
    chat_id = file_info.get("chat_id")
    url_download = file_info.get("url_download")
    url_show = file_info.get("url_show")
    unified_link = file_info.get("unified_link")
    filename = file_info.get("filename")

    if filename and str(filename).lower().endswith(".pdf"):
        debug_bitrix(
            dialog_id=dialog_id,
            file_id=file_id,
            chat_id=chat_id,
            url_download=url_download,
            url_show=url_show,
            unified_link=unified_link,
        )
    else:
        print("PDF not found in payload")
        send_message(dialog_id, "PDF не найден в payload. Смотри логи сервера.")

    return jsonify({"result": "ok"})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mode": "debug"})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
