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
# Новые поля пользователей: добавлены Подтвердил и Дата подтверждения
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
# Используется для уведомлений и ответов пользователю
def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception(f"send error: {e}")

# ==================== Таймауты ====================
# Оставлены для сохранения базовой структуры состояния и активности
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
def keyboard(rows):
    return {
        "keyboard": [[{"text": t} for t in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

MAIN_KB = keyboard([["Старт/Стоп", "Брак"]]) # Оставлена, но не используется в урезанной логике
CANCEL_KB = keyboard([["Отмена"]])

def notify_approvers_new_user(uid, fio):
    """Уведомление всем подтверждённым админам и мастерам с reply-кнопками, отправляющими команды."""
    approvers = get_approvers()
    if not approvers:
        log.info("No approvers found to notify for new user %s", uid)
        return

    approve_cmd = f"/approve_{uid}"
    reject_cmd = f"/reject_{uid}"

    kb = {
        "keyboard": [[{"text": approve_cmd}, {"text": reject_cmd}], [{"text": "Игнорировать"}]],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    text = (f"Новая заявка на доступ:\n"
            f"ФИО: {fio}\n"
            f"TelegramID: {uid}\n\n"
            f"Нажмите кнопку для быстрого подтверждения или отклонения.")
    for a in approvers:
        try:
            send(a, text, kb)
        except Exception as e:
            log.exception("notify approver error: %s", e)

def can_approver_confirm(approver_role, target_role):
    """
    Правила подтверждения:
    - admin подтверждает всех
    - master подтверждает операторов и мастеров, но не админов (вариант B)
    """
    if approver_role == "admin":
        return True
    if approver_role == "master":
        return target_role in ("operator", "master")
    return False

def ask_role_selection(approver_uid, target_uid, fio, approver_role):
    """
    Отправляет сообщение с клавиатурой для выбора роли:
    options: operator, master (и admin, если approver_role == 'admin')
    Кнопки отправляют команды формата /setrole_<target_uid>_<role>
    """
    buttons = []
    buttons.append("operator")
    buttons.append("master")
    if approver_role == "admin":
        buttons.append("admin")

    # формируем строки по 2 кнопки в ряд, затем кнопку Отмена
    kb_rows = []
    row = []
    for i, b in enumerate(buttons):
        row.append(f"/setrole_{target_uid}_{b}")
        if len(row) == 2:
            kb_rows.append(row)
            row = []
    if row:
        kb_rows.append(row)
    # Команда /cancel_setrole не была определена, но для чистоты оставим кнопку
    kb_rows.append(["Отмена"]) 

    kb = {
        "keyboard": [[{"text": t} for t in r] for r in kb_rows],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

    msg = (
        f"Вы подтверждаете пользователя:\n"
        f"ФИО: {fio}\n"
        f"TelegramID: {target_uid}\n\n"
        f"Выберите роль для пользователя:"
    )

    send(approver_uid, msg, kb)

def approve_by(approver_uid, target_uid):
    approver = get_user(approver_uid)
    target = get_user(target_uid)

    if not approver:
        send(approver_uid, "Вы не зарегистрированы или не подтверждены — нет прав подтверждать.")
        return

    if approver["status"] != "подтвержден":
        send(approver_uid, "Ваш аккаунт не подтвержден — нет прав подтверждать.")
        return

    if not target:
        send(approver_uid, f"Пользователь {target_uid} не найден в списке заявок.")
        return

    # Проверка прав (по текущей роли target в таблице)
    # Используем 'operator' как дефолт, если роль не определена в таблице (что не должно произойти, но для безопасности)
    target_role_to_check = target["role"] if target["role"] in ("operator", "master", "admin") else "operator"
    if not can_approver_confirm(approver["role"], target_role_to_check):
        send(approver_uid, "У вас нет прав подтверждать этого пользователя.")
        return

    # Перед подтверждением — предложить выбрать роль
    ask_role_selection(approver_uid, target_uid, target["fio"], approver["role"])

def reject_by(approver_uid, target_uid):
    approver = get_user(approver_uid)
    target = get_user(target_uid)
    if not approver:
        send(approver_uid, "Вы не зарегистрированы или не подтверждены — нет прав отклонять.")
        return
    if approver["status"] != "подтвержден":
        send(approver_uid, "Ваш аккаунт не подтвержден — нет прав отклонять.")
        return
    if not target:
        send(approver_uid, f"Пользователь {target_uid} не найден.")
        return

    # Проверка на master/admin права для отклонения, как в approve_by (подразумевается, что отклонять можно тех, кого можно подтверждать)
    target_role_to_check = target["role"] if target["role"] in ("operator", "master", "admin") else "operator"
    if not can_approver_confirm(approver["role"], target_role_to_check):
        send(approver_uid, "У вас нет прав отклонять этого пользователя.")
        return

    # Ставим статус "отклонен"
    update_user_status(target_uid, "отклонен")

    idx = find_user_row_index(target_uid)
    if idx:
        try:
            # Запись, кто отклонил и когда
            ws_users.update(f"G{idx}", str(approver_uid))  # Подтвердил (кто отклонил)
            ws_users.update(f"H{idx}", now_msk().strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            log.exception("reject_by: update sheet error %s", e)

    send(approver_uid, f"Заявка пользователя {target['fio']} отклонена.")
    try:
        send(int(target_uid), "В доступе отказано.")
    except:
        pass

# ==================== Основная логика ====================
def process(uid, chat, text, user_repr):
    last_activity[uid] = time.time()

    # Инициализация состояния для нового пользователя
    if uid not in states:
        states[uid] = {"chat": chat, "cancel_used": False}

    state = states[uid]

    # ---- обработка выбора роли ----
    # формат команды: /setrole_<target_uid>_<role>
    if text.startswith("/setrole_"):
        try:
            payload = text[len("/setrole_"):]
            # payload: "<target_uid>_<role>"
            parts = payload.split("_", 1)
            if len(parts) != 2:
                send(chat, "Неверная команда выбора роли.")
                return
            target_uid = parts[0]
            new_role = parts[1]
        except Exception:
            send(chat, "Неверная команда выбора роли.")
            return

        approver = get_user(uid)
        target = get_user(target_uid)

        if not approver or approver["status"] != "подтвержден":
            send(chat, "Вы не можете назначать роли.")
            return

        if not target:
            send(chat, "Целевой пользователь не найден.")
            return

        # Проверка разрешений мастера: мастер не может назначать admin
        if approver["role"] == "master" and new_role == "admin":
            send(chat, "Мастер не может назначать роль admin.")
            return

        # Проверить допустимые роли
        if new_role not in ("operator", "master", "admin"):
            send(chat, "Неверная роль.")
            return

        # Проверка: master не должен назначать higher role beyond allowed by can_approver_confirm
        if not can_approver_confirm(approver["role"], new_role if new_role in ("operator","master","admin") else "operator"):
            send(chat, "У вас нет прав назначать такую роль.")
            return

        # Назначаем роль и статус подтверждён
        update_user_role(target_uid, new_role)
        update_user_status(target_uid, "подтвержден")

        # Записываем подтверждающего и дату
        idx = find_user_row_index(target_uid)
        if idx:
            try:
                ws_users.update(f"G{idx}", [[str(uid)]])
                ws_users.update(f"H{idx}", [[now_msk().strftime("%Y-%m-%d %H:%M:%S")]])
            except Exception as e:
                log.exception("setrole: writing confirm data error: %s", e)

        send(chat, f"Пользователь {target['fio']} подтверждён и получил роль {new_role}.")
        try:
            send(int(target_uid), f"Ваша заявка подтверждена! Ваша роль: {new_role}.")
        except:
            pass
        return

    # === Обработка быстрых команд подтверждения/отклонения (от админов/мастеров) ===
    # форматы: /approve_<uid> и /reject_<uid>
    if text.startswith("/approve_") or text.startswith("/reject_"):
        parts = text.split("_", 1)
        if len(parts) == 2:
            cmd = parts[0]
            target_part = parts[1]
            target_id = "".join(ch for ch in target_part if ch.isdigit())
            if not target_id:
                send(chat, "Неверный формат команды.")
                return
            if text.startswith("/approve_"):
                # Вместо мгновенного подтверждения, открываем выбор роли (ask_role_selection)
                approve_by(uid, target_id)
            else:
                reject_by(uid, target_id)
            return

    # === Проверка пользователя (авторизация) ===
    u = get_user(uid)

    # Если нет в таблице — спросить ФИО (заявка на регистрацию)
    if u is None:
        # если уже ждали ФИО — обработать введённое как ФИО
        if state.get("fio_wait"):
            fio = text.strip()
            if not fio:
                send(chat, "Введите корректное ФИО:", CANCEL_KB)
                return
            add_user(uid, fio, requested_by="")
            notify_approvers_new_user(uid, fio)
            send(chat, "Спасибо! Ваша заявка отправлена на подтверждение.")
            states.pop(uid, None)
            return
        else:
            # попросить ФИО
            states[uid]["fio_wait"] = True
            send(chat, "Вы не зарегистрированы. Введите ваше ФИО:")
            return

    # Пользователь найден, но не подтвержден (статус "ожидает" или "отклонен")
    if u["status"] != "подтвержден":
        send(chat, "Ваш доступ пока не подтверждён администратором.")
        return

    # === Главные команды ===
    # Оставлены только команды, которые могут завершить диалог
    if text == "Отмена":
        states.pop(uid, None) # Выход из любого состояния
        send(chat, "Отменено.", MAIN_KB)
        return
        
    if text == "/start":
        states.pop(uid, None)
        send(chat, "Добро пожаловать. Ваш доступ подтверждён.", MAIN_KB)
        return

    # Если пользователь подтвержден, но ввел что-то, не относящееся к авторизации (оставленные команды)
    send(chat, "Выберите действие:", MAIN_KB)


# ==================== Flask ====================
app = Flask(__name__)
LOCK_PATH = "/tmp/bot.lock"

if os.getenv("RENDER"):
    token = os.getenv("TELEGRAM_TOKEN")
    domain = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    if token and domain:
        url = f"https://{domain}/"
        # Установка вебхука
        requests.get(f"https://api.telegram.org/bot{token}/setWebhook?url={url}")
        print(f"Вебхук установлен: {url}")

@app.route("/", methods=["POST"])
def webhook():
    update = request.get_json()
    if not update:
        return "ok", 200

    # Telegram may post different update types; we handle 'message'
    if "message" not in update:
        return "ok", 200

    m = update["message"]
    chat_id = m["chat"]["id"]
    user_id = m["from"]["id"]
    text = (m.get("text") or "").strip()
    username = m["from"].get("username", "")
    user_repr = f"{user_id} (@{username or 'no_user'})"

    with FileLock(LOCK_PATH):
        try:
            process(user_id, chat_id, text, user_repr)
        except Exception as e:
            log.exception("processing error: %s", e)
    return "ok", 200

@app.route("/")
def index():
    return "Bot is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
