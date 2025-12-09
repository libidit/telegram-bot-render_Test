# V1.0 — Только авторизация
import os
import json
import logging
import requests
import threading
import time
from datetime import datetime, timedelta, timezone

from flask import Flask, request
import gspread
from google.oauth2 import service_account
from filelock import FileLock

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ==================== ENV ====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

if not all([TELEGRAM_TOKEN, SPREADSHEET_ID, GOOGLE_CREDS_JSON]):
    raise RuntimeError("Missing required env vars")

creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = service_account.Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

# ==================== Московское время ====================
MSK = timezone(timedelta(hours=3))
def now_msk():
    return datetime.now(MSK)

# ==================== Листы ====================
USERS_SHEET = "Пользователи"
USERS_HEADERS = [
    "TelegramID", "ФИО", "Роль", "Статус",
    "Запросил у", "Дата создания",
    "Подтвердил", "Дата подтверждения"
]

def get_ws(sheet_name, headers=None):
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=3000, cols=20)
    if headers and ws.row_values(1) != headers:
        ws.clear()
        ws.insert_row(headers, 1)
    return ws

ws_users = get_ws(USERS_SHEET, USERS_HEADERS)

# ==================== Отправка сообщений ====================
def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        # Теперь markup - это InlineKeyboardMarkup
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception(f"send error: {e}")

# Отправка ответа на callback_query (важно для исчезновения "часиков" на кнопке)
def answer_callback(callback_id, text=None):
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery", json=payload, timeout=5)
    except:
        pass

# Редактирование сообщения (для замены кнопок после нажатия)
def edit_message(chat_id, message_id, text, markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText", json=payload, timeout=10)
    except:
        pass

# ==================== Таймауты ====================
states = {}
last_activity = {}
TIMEOUT = 600

def timeout_worker():
    while True:
        time.sleep(30)
        now = time.time()
        for uid in list(states):
            if now - last_activity.get(uid, now) > TIMEOUT:
                try:
                    send(states[uid]["chat"], "Диалог прерван — неактивность 10 минут.")
                except:
                    pass
                states.pop(uid, None)
                last_activity.pop(uid, None)

threading.Thread(target=timeout_worker, daemon=True).start()

# ==================== Пользователи / Авторизация ====================
def get_user(uid):
    """Возвращает dict с данными пользователя или None."""
    try:
        values = ws_users.get_all_values()
        for row in values[1:]:
            if row and row[0] == str(uid):
                role = row[2] if len(row) > 2 and row[2].strip() else "operator"
                status = row[3] if len(row) > 3 and row[3].strip() else "ожидает"
                fio = row[1] if len(row) > 1 else ""
                return {"id": str(uid), "fio": fio, "role": role, "status": status}
    except Exception as e:
        log.exception("get_user error: %s", e)
    return None

def add_user(uid, fio, requested_by=""):
    """Добавляет запись в лист Пользователи с ролью operator и статусом 'ожидает'."""
    try:
        ws_users.append_row([
            str(uid),
            fio,
            "operator",
            "ожидает",
            requested_by or "",
            now_msk().strftime("%Y-%m-%d %H:%M:%S"),
            "",  # Подтвердил
            ""   # Дата подтверждения
        ], value_input_option="USER_ENTERED")
    except Exception as e:
        log.exception("add_user error: %s", e)

def find_user_row_index(uid):
    """Возвращает номер строки в листе для данного uid (1-based), или None."""
    try:
        values = ws_users.get_all_values()
        for idx, row in enumerate(values[1:], start=2):
            if row and row[0] == str(uid):
                return idx
    except Exception as e:
        log.exception("find_user_row_index error: %s", e)
    return None

def update_user_status(uid, status):
    idx = find_user_row_index(uid)
    if idx:
        try:
            ws_users.update(f"D{idx}", [[status]])
        except Exception as e:
            log.exception("update_user_status error: %s", e)


def update_user_role(uid, role):
    idx = find_user_row_index(uid)
    if idx:
        try:
            ws_users.update(f"C{idx}", [[role]])
        except Exception as e:
            log.exception("update_user_role error: %s", e)

def get_approvers():
    """Возвращает список chat_id (int) всех подтверждённых admin и master."""
    res = []
    try:
        values = ws_users.get_all_values()[1:]
        for row in values:
            if len(row) >= 4:
                role = row[2].strip() if row[2] else ""
                status = row[3].strip() if row[3] else ""
                if role in ("admin", "master") and status == "подтвержден":
                    if row[0].strip().isdigit():
                        res.append(int(row[0].strip()))
    except Exception as e:
        log.exception("get_approvers error: %s", e)
    return res

# Клавиатуры для авторизации
# MAIN_KB и CANCEL_KB используются для обычных ответов, но их использование сведено к минимуму
def keyboard(rows):
    return {
        "keyboard": [[{"text": t} for t in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

MAIN_KB = keyboard([["Старт/Стоп", "Брак"]])
CANCEL_KB = keyboard([["Отмена"]])

# ==================== Уведомление и выбор роли (Inline) ====================
def notify_approvers_new_user(uid, fio):
    """Уведомление всем подтверждённым админам и мастерам с inline-кнопками."""
    approvers = get_approvers()
    if not approvers:
        log.info("No approvers found to notify for new user %s", uid)
        return

    # Callback data для начального действия
    approve_data = f"pre_approve_{uid}"
    reject_data = f"reject_{uid}"

    # InlineKeyboardMarkup
    kb = {
        "inline_keyboard": [
            [
                {"text": "✅ Подтвердить и выбрать роль", "callback_data": approve_data},
            ],
            [
                {"text": "❌ Отклонить", "callback_data": reject_data},
            ]
        ]
    }
    
    text = (f"Новая заявка на доступ:\n"
            f"ФИО: {fio}\n"
            f"TelegramID: {uid}\n\n"
            f"Нажмите кнопку для выбора роли или отклонения.")
    for a in approvers:
        try:
            send(a, text, kb)
        except Exception as e:
            log.exception("notify approver error: %s", e)

def can_approver_confirm(approver_role, target_role):
    """
    Правила подтверждения:
    - admin подтверждает всех
    - master подтверждает операторов и мастеров, но не админов
    """
    if approver_role == "admin":
        return True
    if approver_role == "master":
        return target_role in ("operator", "master")
    return False

def build_role_selection_kb(target_uid, approver_role):
    """
    Создает Inline-клавиатуру для выбора роли.
    Callback data: confirm_<target_uid>_<role>
    """
    buttons = []
    buttons.append({"text": "Оператор (operator)", "callback_data": f"confirm_{target_uid}_operator"})
    buttons.append({"text": "Мастер (master)", "callback_data": f"confirm_{target_uid}_master"})
    if approver_role == "admin":
        buttons.append({"text": "Админ (admin)", "callback_data": f"confirm_{target_uid}_admin"})

    kb_rows = []
    row = []
    for i, b in enumerate(buttons):
        row.append(b)
        if len(row) == 2:
            kb_rows.append(row)
            row = []
    if row:
        kb_rows.append(row)
    
    # Кнопка отмены выбора (отменяет процесс выбора роли, но не отклоняет заявку)
    kb_rows.append([{"text": "Отмена выбора роли", "callback_data": f"cancel_confirm_{target_uid}"}])

    return {"inline_keyboard": kb_rows}

def ask_role_selection(approver_uid, target_uid, fio, approver_role, message_id):
    """
    Редактирует сообщение, заменяя кнопки на выбор роли.
    """
    kb = build_role_selection_kb(target_uid, approver_role)

    msg = (
        f"Вы подтверждаете пользователя:\n"
        f"ФИО: {fio}\n"
        f"TelegramID: {target_uid}\n\n"
        f"**Выберите роль для пользователя:**"
    )
    edit_message(approver_uid, message_id, msg, kb)


# Функция для обработки предварительного подтверждения (Pre-approve)
def pre_approve_process(approver_uid, target_uid, message_id):
    approver = get_user(approver_uid)
    target = get_user(target_uid)

    if not approver or approver["status"] != "подтвержден":
        edit_message(approver_uid, message_id, "Вы не зарегистрированы или не подтверждены — нет прав подтверждать.")
        return False

    if not target:
        edit_message(approver_uid, message_id, f"Пользователь {target_uid} не найден в списке заявок.")
        return False
    
    # Проверка, что заявка еще в статусе 'ожидает'
    if target["status"] != "ожидает":
        edit_message(approver_uid, message_id, f"Заявка пользователя {target['fio']} уже обработана ({target['status']}).")
        return False

    # Проверка прав (по текущей роли target в таблице, которая 'operator' для новых)
    target_role_to_check = target["role"] if target["role"] in ("operator", "master", "admin") else "operator"
    if not can_approver_confirm(approver["role"], target_role_to_check):
        edit_message(approver_uid, message_id, "У вас нет прав подтверждать этого пользователя.")
        return False

    # Переход к выбору роли
    ask_role_selection(approver_uid, target_uid, target["fio"], approver["role"], message_id)
    return True

# Функция для обработки финального подтверждения (Confirm)
def confirm_process(approver_uid, target_uid, new_role, message_id):
    approver = get_user(approver_uid)
    target = get_user(target_uid)

    if not approver or approver["status"] != "подтвержден":
        edit_message(approver_uid, message_id, "Вы не можете назначать роли.")
        return False

    if not target:
        edit_message(approver_uid, message_id, "Целевой пользователь не найден.")
        return False
    
    # Проверка, что заявка еще в статусе 'ожидает'
    if target["status"] != "ожидает":
        edit_message(approver_uid, message_id, f"Заявка пользователя {target['fio']} уже обработана ({target['status']}).")
        return False

    # Проверка разрешений мастера
    if approver["role"] == "master" and new_role == "admin":
        edit_message(approver_uid, message_id, "Мастер не может назначать роль admin.")
        return False

    # Проверка прав (должен быть возможность подтвердить эту роль)
    if not can_approver_confirm(approver["role"], new_role):
        edit_message(approver_uid, message_id, "У вас нет прав назначать такую роль.")
        return False

    # Назначаем роль и статус подтверждён
    update_user_role(target_uid, new_role)
    update_user_status(target_uid, "подтвержден")

    # Записываем подтверждающего и дату
    idx = find_user_row_index(target_uid)
    if idx:
        try:
            ws_users.update(f"G{idx}", [[str(approver_uid)]])
            ws_users.update(f"H{idx}", [[now_msk().strftime("%Y-%m-%d %H:%M:%S")]])
        except Exception as e:
            log.exception("confirm_process: writing confirm data error: %s", e)

    final_msg = f"✅ **Подтверждено!** Пользователь **{target['fio']}** получил роль **{new_role}**."
    edit_message(approver_uid, message_id, final_msg)
    
    try:
        send(int(target_uid), f"Ваша заявка подтверждена! Ваша роль: {new_role}.")
    except:
        pass
    return True

# Функция для обработки отклонения (Reject)
def reject_process(approver_uid, target_uid, message_id):
    approver = get_user(approver_uid)
    target = get_user(target_uid)
    
    if not approver or approver["status"] != "подтвержден":
        edit_message(approver_uid, message_id, "У вас нет прав отклонять заявки.")
        return False

    if not target:
        edit_message(approver_uid, message_id, f"Пользователь {target_uid} не найден.")
        return False
    
    # Проверка, что заявка еще в статусе 'ожидает'
    if target["status"] != "ожидает":
        edit_message(approver_uid, message_id, f"Заявка пользователя {target['fio']} уже обработана ({target['status']}).")
        return False

    # Проверка прав
    target_role_to_check = target["role"] if target["role"] in ("operator", "master", "admin") else "operator"
    if not can_approver_confirm(approver["role"], target_role_to_check):
        edit_message(approver_uid, message_id, "У вас нет прав отклонять этого пользователя.")
        return False

    # Ставим статус "отклонен"
    update_user_status(target_uid, "отклонен")

    idx = find_user_row_index(target_uid)
    if idx:
        try:
            # Запись, кто отклонил и когда
            ws_users.update(f"G{idx}", [[str(approver_uid)]])
            ws_users.update(f"H{idx}", [[now_msk().strftime("%Y-%m-%d %H:%M:%S")]])
        except Exception as e:
            log.exception("reject_process: update sheet error %s", e)

    reject_msg = f"❌ **Отклонено.** Заявка пользователя **{target['fio']}** отклонена."
    edit_message(approver_uid, message_id, reject_msg)
    
    try:
        send(int(target_uid), "В доступе отказано.")
    except:
        pass
    return True

# Функция для отмены выбора роли
def cancel_confirm_process(approver_uid, target_uid, message_id):
    target = get_user(target_uid)
    if not target or target["status"] != "ожидает":
        edit_message(approver_uid, message_id, "Заявка уже обработана или не найдена.")
        return False

    # Восстанавливаем исходное сообщение с кнопками pre_approve и reject
    fio = target["fio"]
    approve_data = f"pre_approve_{target_uid}"
    reject_data = f"reject_{target_uid}"

    kb = {
        "inline_keyboard": [
            [
                {"text": "✅ Подтвердить и выбрать роль", "callback_data": approve_data},
            ],
            [
                {"text": "❌ Отклонить", "callback_data": reject_data},
            ]
        ]
    }
    
    text = (f"Новая заявка на доступ:\n"
            f"ФИО: {fio}\n"
            f"TelegramID: {target_uid}\n\n"
            f"Выбор роли отменён. Нажмите кнопку для выбора роли или отклонения.")
    edit_message(approver_uid, message_id, text, kb)
    return True


# ==================== Основная логика ====================
def process_message(uid, chat, text):
    """Обрабатывает текстовые сообщения (заявки на регистрацию и команды /start/Отмена)."""
    last_activity[uid] = time.time()
    
    if uid not in states:
        states[uid] = {"chat": chat}

    state = states[uid]
    
    # --- Проверка пользователя (авторизация) ---
    u = get_user(uid)
    
    # Если нет в таблице — спросить ФИО (заявка на регистрацию)
    if u is None:
        if state.get("fio_wait"):
            fio = text.strip()
            if not fio or fio == "Отмена":
                send(chat, "Операция отменена. Для регистрации введите /start", MAIN_KB)
                states.pop(uid, None)
                return
                
            add_user(uid, fio)
            notify_approvers_new_user(uid, fio)
            send(chat, "Спасибо! Ваша заявка отправлена на подтверждение.")
            states.pop(uid, None)
            return
        else:
            # попросить ФИО
            states[uid]["fio_wait"] = True
            send(chat, "Вы не зарегистрированы. Введите ваше ФИО:", CANCEL_KB)
            return

    # Пользователь найден, но не подтвержден
    if u["status"] != "подтвержден":
        send(chat, "Ваш доступ пока не подтверждён администратором.")
        return

    # === Главные команды (для подтвержденных пользователей) ===
    if text in ("/start", "Отмена"):
        send(chat, "Ваш доступ подтверждён.", MAIN_KB)
        states.pop(uid, None)
        return
    
    # Если подтвержденный пользователь ввел что-то другое
    send(chat, "Ваш доступ подтверждён.", MAIN_KB)


def process_callback_query(callback_query):
    """Обрабатывает нажатия на Inline-кнопки."""
    uid = callback_query["from"]["id"]
    data = callback_query["data"]
    message = callback_query["message"]
    chat_id = message["chat"]["id"]
    message_id = message["message_id"]

    answer_callback(callback_query["id"])

    # 1. Начальное подтверждение
    if data.startswith("pre_approve_"):
        target_uid = data.split("_")[-1]
        pre_approve_process(uid, target_uid, message_id)
        return

    # 2. Финальное подтверждение (выбор роли)
    if data.startswith("confirm_"):
        try:
            parts = data.split("_")
            target_uid = parts[1]
            new_role = parts[2]
        except IndexError:
            edit_message(chat_id, message_id, "Ошибка: неверный формат данных.")
            return

        confirm_process(uid, target_uid, new_role, message_id)
        return

    # 3. Отклонение
    if data.startswith("reject_"):
        target_uid = data.split("_")[-1]
        reject_process(uid, target_uid, message_id)
        return

    # 4. Отмена выбора роли
    if data.startswith("cancel_confirm_"):
        target_uid = data.split("_")[-1]
        cancel_confirm_process(uid, target_uid, message_id)
        return


# ==================== Flask ====================
app = Flask(__name__)
LOCK_PATH = "/tmp/bot.lock"

if os.getenv("RENDER"):
    token = os.getenv("TELEGRAM_TOKEN")
    domain = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    if token and domain:
        url = f"https://{domain}/"
        requests.get(f"https://api.telegram.org/bot{token}/setWebhook?url={url}")
        print(f"Вебхук установлен: {url}")

@app.route("/", methods=["POST"])
def webhook():
    update = request.get_json()
    if not update:
        return "ok", 200

    with FileLock(LOCK_PATH):
        try:
            if "message" in update:
                m = update["message"]
                user_id = m["from"]["id"]
                chat_id = m["chat"]["id"]
                text = (m.get("text") or "").strip()
                process_message(user_id, chat_id, text)
            
            elif "callback_query" in update:
                process_callback_query(update["callback_query"])
        
        except Exception as e:
            log.exception("processing error: %s", e)
    return "ok", 200

@app.route("/")
def index():
    return "Bot is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
