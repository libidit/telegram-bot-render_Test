# V4.1 — Полный файл: Авторизация пользователей, подтверждение заявок, выбор роли, журнал подтверждений
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
refresh_controllers()

# ==================== Московское время ====================
MSK = timezone(timedelta(hours=3))
def now_msk():
    return datetime.now(MSK)

# ==================== Листы ====================
STARTSTOP_SHEET = "Старт-Стоп"
DEFECT_SHEET = "Брак"
CTRL_STARTSTOP_SHEET = "Контр_Старт-Стоп"
CTRL_DEFECT_SHEET = "Контр_Брак"

HEADERS_STARTSTOP = ["Дата","Время","Номер линии","Действие","Причина","ЗНП","Метров брака","Вид брака","Пользователь","Время отправки","Статус"]

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

ws_startstop = get_ws(STARTSTOP_SHEET, HEADERS_STARTSTOP)
ws_defect = get_ws(DEFECT_SHEET)
ws_users = get_ws(USERS_SHEET, USERS_HEADERS)

# ==================== Контролёры ====================
def get_controllers(sheet_name):
    try:
        ws = sh.worksheet(sheet_name)
        ids = ws.col_values(1)[1:]
        return [int(i.strip()) for i in ids if i.strip().isdigit()]
    except:
        return []

controllers_startstop = []
controllers_defect = []

def refresh_controllers():
    global controllers_startstop, controllers_defect
    try:
        new_startstop = get_controllers(CTRL_STARTSTOP_SHEET)
        new_defect = get_controllers(CTRL_DEFECT_SHEET)

        controllers_startstop = new_startstop
        controllers_defect = new_defect

        log.info(f"[Контролёры обновлены] Старт/Стоп = {controllers_startstop}, Брак = {controllers_defect}")
    except Exception as e:
        log.exception("Ошибка обновления контролёров: %s", e)

# ==================== Последние записи ====================
def get_last_records(ws, n=2):
    try:
        values = ws.get_all_values()
        if len(values) <= 1:
            return []
        return values[-n:]
    except:
        return []

# ==================== Уведомления ====================
def notify_controllers(ids, message):
    for cid in ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": message, "parse_mode": "HTML"},
                timeout=10
            )
        except:
            pass

# ==================== Запись в таблицу ====================
def append_row(data):
    flow = data.get("flow", "startstop")
    ws = ws_defect if flow == "defect" else ws_startstop
    ts = now_msk().strftime("%Y-%m-%d %H:%M:%S")
    user = data["user"]

    if flow == "defect":
        row = [data["date"], data["time"], data["line"], "брак",
               data.get("znp", ""), data["meters"],
               data.get("defect_type", ""), user, ts, ""]
        msg = (f"НОВАЯ ЗАПИСЬ БРАКА\n"
               f"Линия: {data['line']}\n"
               f"{data['date']} {data['time']}\n"
               f"ЗНП: <code>{data.get('znp','—')}</code>\n"
               f"Метров брака: {data['meters']}\n"
               f"Вид брака: {data.get('defect_type','—')}")
        notify_controllers(controllers_defect, msg)
    else:
        row = [data["date"], data["time"], data["line"], data["action"],
               data.get("reason", ""), data.get("znp", ""), data.get("meters",""),
               data.get("defect_type",""), user, ts, ""]
        action_ru = "Запуск" if data["action"] == "запуск" else "Остановка"
        msg = (f"НОВАЯ ЗАПИСЬ СТАРТ/СТОП\n"
               f"Линия: {data['line']}\n"
               f"{data['date']} {data['time']}\n"
               f"Действие: {action_ru}\n"
               f"Причина: {data.get('reason','—')}")
        notify_controllers(controllers_startstop, msg)

    ws.append_row(row, value_input_option="USER_ENTERED")

# ==================== Клавиатуры ====================
def keyboard(rows):
    return {
        "keyboard": [[{"text": t} for t in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

MAIN_KB = keyboard([["Старт/Стоп", "Брак"]])
FLOW_MENU_KB = keyboard([["Новая запись"], ["Отменить последнюю запись"], ["Назад"]])
CANCEL_KB = keyboard([["Отмена"]])
CONFIRM_KB = keyboard([["Да, отменить"], ["Нет, оставить"]])

NUM_LINE_KB = {
    "keyboard": [
        [{"text": "1"}, {"text": "2"}, {"text": "3"}, {"text": "4"}, {"text": "5"}],
        [{"text": "6"}, {"text": "7"}, {"text": "8"}, {"text": "9"}, {"text": "10"}],
        [{"text": "11"}, {"text": "12"}, {"text": "13"}, {"text": "14"}, {"text": "15"}],
        [{"text": "Отмена"}]
    ],
    "resize_keyboard": True,
    "one_time_keyboard": True,
    "input_field_placeholder": "Выберите номер линии"
}

REASONS_CACHE = {"kb": None, "until": 0}
DEFECTS_CACHE = {"kb": None, "until": 0}

def build_kb(sheet_name, extra=None):
    if extra is None: extra = []
    try:
        values = sh.worksheet(sheet_name).col_values(1)[1:]
        items = [v.strip() for v in values if v.strip()] + extra
        rows = [items[i:i+2] for i in range(0, len(items), 2)]
        rows.append(["Отмена"])
        return keyboard(rows)
    except:
        return keyboard([extra[i:i+2] for i in range(0, len(extra), 2)] + [["Отмена"]])

def get_reasons_kb():
    now = time.time()
    if now > REASONS_CACHE["until"]:
        REASONS_CACHE["kb"] = build_kb("Причина остановки", ["Другое"])
        REASONS_CACHE["until"] = now + 300
    return REASONS_CACHE["kb"]

def get_defect_kb():
    now = time.time()
    if now > DEFECTS_CACHE["until"]:
        DEFECTS_CACHE["kb"] = build_kb("Вид брака", ["Другое", "Без брака"])
        DEFECTS_CACHE["until"] = now + 300
    return DEFECTS_CACHE["kb"]

# ==================== Отправка сообщений ====================
def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception(f"send error: {e}")

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

# Интервал обновления (в минутах)
CONTROLLERS_REFRESH_INTERVAL_MIN = 1440  # 24 часа

def controllers_refresh_worker():
    while True:
        refresh_controllers()
        time.sleep(CONTROLLERS_REFRESH_INTERVAL_MIN * 60)

# Запуск фонового обновления контролёров
threading.Thread(target=controllers_refresh_worker, daemon=True).start()

# ==================== Поиск последней активной записи ====================
def find_last_active_record(ws, user_repr):
    values = ws.get_all_values()
    if ws.title == "Брак":
        user_col = 7   # row[7] — пользователь
        status_col = 9 # row[9] — статус
    else:
        user_col = 8   # row[8] — пользователь (Старт/Стоп)
        status_col = 10 # row[10] — статус

    for i in range(len(values)-1, 0, -1):
        row = values[i]
        if len(row) <= user_col:
            continue
        if row[user_col].strip() == user_repr and (len(row) <= status_col or row[status_col].strip() != "ОТМЕНЕНО"):
            return row, i+1
    return None, None

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
    kb_rows.append(["/cancel_setrole", "Отмена"])

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
    if not can_approver_confirm(approver["role"], target["role"]):
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

    if approver["role"] == "master" and new_role == "admin":
        send(chat, "Мастер не может назначать роль admin.")
        return

    # Ставим статус "отклонен"
    update_user_status(target_uid, "отклонен")

    idx = find_user_row_index(target_uid)
    if idx:
        try:
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
                # Instead of instant approve, open role selection (ask_role_selection)
                approve_by(uid, target_id)
            else:
                reject_by(uid, target_id)
            return

    # === Проверка пользователя (авторизация) ===
    u = get_user(uid)

    # Если нет в таблице — спросить ФИО
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

    # Пользователь найден, но не подтвержден
    if u["status"] != "подтвержден":
        send(chat, "Ваш доступ пока не подтверждён администратором.")
        return

    # === Главные команды ===
    if text == "Назад":
        states.pop(uid, None)
        send(chat, "Главное меню:", MAIN_KB)
        return

    if text == "Отмена":
        state.pop("pending_cancel", None)
        send(chat, "Отменено.", MAIN_KB)
        return

    # === Выбор потока ===
    if "flow" not in state:
        if text in ("/start", "Старт/Стоп"):
            send(chat, "<b>Старт/Стоп</b>\nВыберите действие:", FLOW_MENU_KB)
            state.update({"flow": "startstop"})
            return
        elif text == "Брак":
            send(chat, "<b>Брак</b>\nВыберите действие:", FLOW_MENU_KB)
            state.update({"flow": "defect"})
            return
        else:
            send(chat, "Выберите действие:", MAIN_KB)
            return

    flow = state["flow"]

    # === Запрос на отмену ===
    if text == "Отменить последнюю запись":
        if state.get("cancel_used", False):
            send(chat, "Вы уже отменили одну запись в этом сеансе. Сделайте новую запись, чтобы снова отменить.", FLOW_MENU_KB)
            return

        ws = ws_defect if flow == "defect" else ws_startstop
        row, row_num = find_last_active_record(ws, user_repr)

        if not row:
            send(chat, "У вас нет активных записей для отмены.", FLOW_MENU_KB)
            return

        state["pending_cancel"] = {"ws": ws, "row": row, "row_num": row_num}

        if flow == "startstop":
            action = "Запуск" if row[3] == "запуск" else "Остановка"
            msg = (f"Отменить эту запись?\n\n"
                   f"<b>Старт/Стоп</b>\n"
                   f"{row[0]} {row[1]} | Линия {row[2]}\n"
                   f"Действие: {action}\n"
                   f"Причина: {row[4] if len(row)>4 else '—'}")
        else:
            msg = (f"Отменить эту запись?\n\n"
                   f"<b>Брак</b>\n"
                   f"{row[0]} {row[1]} | Линия {row[2]}\n"
                   f"ЗНП: <code>{row[4]}</code>\n"
                   f"Метров: {row[5]}\n"
                   f"Вид брака: {row[6] if len(row)>6 else '—'}")

        send(chat, msg, CONFIRM_KB)
        return

    # === Подтверждение отмены ===
    if "pending_cancel" in state:
        pend = state["pending_cancel"]

        if text == "Да, отменить":
            ws = pend["ws"]
            row = pend["row"]
            row_num = pend["row_num"]

            status_col = "K" if flow == "startstop" else "J"
            ws.update(f"{status_col}{row_num}", [["ОТМЕНЕНО"]])

            if flow == "startstop":
                action = "Запуск" if row[3] == "запуск" else "Остановка"
                notify_controllers(controllers_startstop,
                    f"ОТМЕНЕНА ЗАПИСЬ СТАРТ/СТОП\nПользователь: {user_repr}\n"
                    f"Линия: {row[2]}\n{row[0]} {row[1]}\nДействие: {action}")
                send(chat, f"Запись отменена:\n{row[0]} {row[1]} | Линия {row[2]} | {action}", FLOW_MENU_KB)
            else:
                notify_controllers(controllers_defect,
                    f"ОТМЕНЕНА ЗАПИСЬ БРАКА\nПользователь: {user_repr}\n"
                    f"Линия: {row[2]}\n{row[0]} {row[1]}\nЗНП: <code>{row[4]}</code>\nМетров: {row[5]}")
                send(chat, f"Запись брака отменена:\n{row[0]} {row[1]} | Линия {row[2]}", FLOW_MENU_KB)

            state["cancel_used"] = True
            state.pop("pending_cancel", None)
            return

        if text == "Нет, оставить":
            send(chat, "Отмена отменена. Запись сохранена.", FLOW_MENU_KB)
            state.pop("pending_cancel", None)
            return

    # === Новая запись ===
    if text == "Новая запись":
        state["cancel_used"] = False  # сброс флага отмены

        if flow == "defect":
            records = get_last_records(ws_defect, 2)
            msg = "<b>Последние записи Брака:</b>\n\n"
            msg += "\n".join(f"• {r[0]} {r[1]} | Линия {r[2]} | <code>{r[4] if len(r)>4 else '—'}</code> | {r[5] if len(r)>5 else '—'}м"
                             for r in records) if records else "Нет записей."
            send(chat, msg)
            state.update({"step": "line", "data": {"action": "брак"}})
        else:
            records = get_last_records(ws_startstop, 2)
            msg = "<b>Последние записи Старт/Стоп:</b>\n\n"
            msg += "\n".join(f"• {r[0]} {r[1]} | Линия {r[2]} | {'Запуск' if r[3]=='запуск' else 'Остановка'} | {r[4] if len(r)>4 else '—'}"
                             for r in records) if records else "Нет записей."
            send(chat, msg)
            state.update({"step": "line", "data": {}})

        send(chat, "Введите номер линии (1–15):", NUM_LINE_KB)
        return

    # === Обработка шагов записи ===
    st = state
    step = st.get("step")
    data = st.get("data", {})

    if not step:
        send(chat, "Выберите действие:", FLOW_MENU_KB)
        return

    # --- Step: line ---
    if step == "line":
        if not (text.isdigit() and 1 <= int(text) <= 15):
            send(chat, "Номер линии должен быть от 1 до 15:", NUM_LINE_KB)
            return
        data["line"] = text
        st["step"] = "date"
        today = now_msk().strftime("%d.%m.%Y")
        yest = (now_msk() - timedelta(days=1)).strftime("%d.%m.%Y")
        send(chat, "Дата:", keyboard([[today, yest], ["Другая дата", "Отмена"]]))
        return

    # --- Step: date ---
    if step == "date":
        if text == "Другая дата":
            st["step"] = "date_custom"
            send(chat, "Введите дату (дд.мм.гггг):", CANCEL_KB)
            return
        try:
            datetime.strptime(text, "%d.%m.%Y")
            data["date"] = text
        except:
            send(chat, "Неверный формат даты.", CANCEL_KB)
            return
        st["step"] = "time"
        now = now_msk()
        t = [now.strftime("%H:%M"), (now-timedelta(minutes=10)).strftime("%H:%M"),
             (now-timedelta(minutes=20)).strftime("%H:%M"), (now-timedelta(minutes=30)).strftime("%H:%M")]
        send(chat, "Время:", keyboard([[t[0], t[1], "Другое время"], [t[2], t[3], "Отмена"]]))
        return

    if step == "date_custom":
        try:
            datetime.strptime(text, "%d.%m.%Y")
            data["date"] = text
            st["step"] = "time"
            now = now_msk()
            t = [now.strftime("%H:%M"), (now-timedelta(minutes=10)).strftime("%H:%M"),
                 (now-timedelta(minutes=20)).strftime("%H:%M"), (now-timedelta(minutes=30)).strftime("%H:%M")]
            send(chat, "Время:", keyboard([[t[0], t[1], "Другое время"], [t[2], t[3], "Отмена"]]))
        except:
            send(chat, "Введите дату в формате дд.мм.гггг", CANCEL_KB)
        return

    # --- Step: time ---
    if step in ("time", "time_custom"):
        if text == "Другое время":
            st["step"] = "time_custom"
            send(chat, "Введите время (чч:мм):", CANCEL_KB)
            return
        if not (len(text) == 5 and text[2] == ":" and text[:2].isdigit() and text[3:].isdigit()):
            send(chat, "Неверный формат времени.", CANCEL_KB)
            return
        data["time"] = text
        if flow == "defect":
            st["step"] = "znp_prefix"
            now = now_msk()
            curr_month = now.month
            curr_year = now.year
            
            # текущий месяц
            curr = f"{curr_month:02d}{str(curr_year)[2:]}"
            
            # предыдущий месяц
            if curr_month == 1:
                prev_month = 12
                prev_year = curr_year - 1
            else:
                prev_month = curr_month - 1
                prev_year = curr_year
            
            prev = f"{prev_month:02d}{str(prev_year)[2:]}"

            kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
            send(chat, "Префикс ЗНП:", keyboard(kb))
        else:
            st["step"] = "action"
            send(chat, "Действие:", keyboard([["Запуск", "Остановка"], ["Отмена"]]))
        return

    # --- Step: action ---
    if step == "action":
        if text not in ("Запуск", "Остановка"):
            send(chat, "Выберите действие:", keyboard([["Запуск", "Остановка"], ["Отмена"]]))
            return
        data["action"] = "запуск" if text == "Запуск" else "остановка"
        if data["action"] == "запуск":
            st["step"] = "znp_prefix"
            now = now_msk()
            curr_month = now.month
            curr_year = now.year
            
            # текущий месяц
            curr = f"{curr_month:02d}{str(curr_year)[2:]}"
            
            # предыдущий месяц
            if curr_month == 1:
                prev_month = 12
                prev_year = curr_year - 1
            else:
                prev_month = curr_month - 1
                prev_year = curr_year
            
            prev = f"{prev_month:02d}{str(prev_year)[2:]}"

            kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
            send(chat, "Префикс ЗНП:", keyboard(kb))
        else:
            st["step"] = "reason"
            send(chat, "Причина остановки:", get_reasons_kb())
        return

    # --- Step: reason ---
    if step in ("reason", "reason_custom"):
        if text == "Другое" and step == "reason":
            st["step"] = "reason_custom"
            send(chat, "Введите причину остановки:", CANCEL_KB)
            return
        data["reason"] = text
        st["step"] = "znp_prefix"
        now = now_msk()
        curr_month = now.month
        curr_year = now.year

        # текущий месяц
        curr = f"{curr_month:02d}{str(curr_year)[2:]}"

        # предыдущий месяц
        if curr_month == 1:
            prev_month = 12
            prev_year = curr_year - 1
        else:
            prev_month = curr_month - 1
            prev_year = curr_year

        prev = f"{prev_month:02d}{str(prev_year)[2:]}"

        kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
        send(chat, "Префикс ЗНП:", keyboard(kb))
        return

    # --- Step: znp_prefix / znp_manual ---
    if step in ("znp_prefix", "znp_manual"):
        now = now_msk()
        curr_month = now.month
        curr_year = now.year
        
        # текущий месяц
        curr = f"{curr_month:02d}{str(curr_year)[2:]}"
        
        # предыдущий месяц
        if curr_month == 1:
            prev_month = 12
            prev_year = curr_year - 1
        else:
            prev_month = curr_month - 1
            prev_year = curr_year
        
        prev = f"{prev_month:02d}{str(prev_year)[2:]}"

        valid = [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]

        if step == "znp_prefix":
            if text in valid:
                data["znp_prefix"] = text
                send(chat, f"Последние 4 цифры ЗНП для <b>{text}</b>-XXXX:", CANCEL_KB)
                return
            if text == "Другое":
                st["step"] = "znp_manual"
                send(chat, "Введите полный ЗНП (например, D1225-1234):", CANCEL_KB)
                return
            if text.isdigit() and len(text) == 4 and "znp_prefix" in data:
                data["znp"] = f"{data['znp_prefix']}-{text}"
                st["step"] = "meters"
                send(chat, "Сколько метров брака?", CANCEL_KB)
                return
            send(chat, "Выберите префикс:", keyboard([[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]))
            return

        if step == "znp_manual":
            if len(text) == 10 and text[5] == "-" and text[:5].upper() in valid:
                data["znp"] = text.upper()
                st["step"] = "meters"
                send(chat, "Сколько метров брака?", CANCEL_KB)
                return
            send(chat, "Неправильный формат ЗНП.\nПример: <code>D1225-1234</code>", CANCEL_KB)
            return

    # --- Step: meters ---
    if step == "meters":
        if not text.isdigit() or int(text) <= 0:
            send(chat, "Укажите количество метров брака (число > 0):", CANCEL_KB)
            return
        data["meters"] = text
        st["step"] = "defect_type"
        send(chat, "Вид брака:", get_defect_kb())
        return

    # --- Step: defect_type / defect_custom ---
    if step in ("defect_type", "defect_custom"):
        if text == "Другое" and step == "defect_type":
            st["step"] = "defect_custom"
            send(chat, "Опишите вид брака:", CANCEL_KB)
            return
        data["defect_type"] = "" if text == "Без брака" else text
        data["user"] = user_repr
        data["flow"] = flow
        append_row(data)
        send(chat, f"<b>Записано!</b>\nЛиния {data['line']} • {data['date']} {data['time']}", MAIN_KB)
        states.pop(uid, None)
        return

    send(chat, "Выберите действие:", FLOW_MENU_KB)

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
