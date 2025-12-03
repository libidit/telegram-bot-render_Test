# V 3.9 — Отмена с подтверждением + защита от дублей + АВТОРИЗАЦИЯ ПОЛЬЗОВАТЕЛЕЙ (ПОЛНАЯ ВЕРСИЯ)
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
STARTSTOP_SHEET = "Старт-Стоп"
DEFECT_SHEET = "Брак"
CTRL_STARTSTOP_SHEET = "Контр_Старт-Стоп"
CTRL_DEFECT_SHEET = "Контр_Брак"
USERS_SHEET = "Пользователи"

HEADERS_STARTSTOP = ["Дата","Время","Номер линии","Действие","Причина","ЗНП","Метров брака","Вид брака","Пользователь","Время отправки","Статус"]
USERS_HEADERS = ["TelegramID", "ФИО", "Роль", "Статус", "Запросил у", "Дата создания", "Подтвердил", "Дата подтверждения"]

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

controllers_startstop = get_controllers(CTRL_STARTSTOP_SHEET)
controllers_defect = get_controllers(CTRL_DEFECT_SHEET)

def refresh_controllers():
    global controllers_startstop, controllers_defect
    try:
        controllers_startstop = get_controllers(CTRL_STARTSTOP_SHEET)
        controllers_defect = get_controllers(CTRL_DEFECT_SHEET)
        log.info(f"Контролёры обновлены: Старт/Стоп={len(controllers_startstop)}, Брак={len(controllers_defect)}")
    except Exception as e:
        log.exception("Ошибка обновления контролёров: %s", e)

# Обновление раз в сутки
threading.Thread(target=lambda: [time.sleep(86400), refresh_controllers() or True][1], daemon=True).start()

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
                send(states[uid]["chat"], "Диалог прерван — неактивность 10 минут.")
                states.pop(uid, None)
                last_activity.pop(uid, None)

threading.Thread(target=timeout_worker, daemon=True).start()

# ==================== Пользователи и авторизация ====================
def get_user(uid):
    try:
        values = ws_users.get_all_values()
        for row in values[1:]:
            if row and len(row) > 0 and row[0] == str(uid):
                role = row[2].strip() if len(row) > 2 else "operator"
                status = row[3].strip() if len(row) > 3 else "ожидает"
                fio = row[1] if len(row) > 1 else ""
                return {"id": str(uid), "fio": fio.strip(), "role": role, "status": status}
    except Exception as e:
        log.exception("get_user error: %s", e)
    return None

def add_user(uid, fio):
    try:
        ws_users.append_row([
            str(uid), fio, "operator", "ожидает", "", now_msk().strftime("%Y-%m-%d %H:%M:%S"), "", ""
        ], value_input_option="USER_ENTERED")
    except Exception as e:
        log.exception("add_user error: %s", e)

def find_user_row_index(uid):
    try:
        values = ws_users.get_all_values()
        for idx, row in enumerate(values[1:], start=2):
            if row and row[0] == str(uid):
                return idx
    except:
        return None

def update_user(uid, role=None, status=None, confirmed_by=None):
    idx = find_user_row_index(uid)
    if not idx:
        return
    try:
        if role: ws_users.update(f"C{idx}", [[role]])
        if status: ws_users.update(f"D{idx}", [[status]])
        if confirmed_by:
            ws_users.update(f"G{idx}", [[str(confirmed_by)]])
            ws_users.update(f"H{idx}", [[now_msk().strftime("%Y-%m-%d %H:%M:%S")]])
    except Exception as e:
        log.exception("update_user error: %s", e)

def get_approvers():
    res = []
    try:
        values = ws_users.get_all_values()[1:]
        for row in values:
            if len(row) >= 4 and row[2].strip() in ("admin", "master") and row[3].strip() == "подтвержден":
                if row[0].strip().isdigit():
                    res.append(int(row[0].strip()))
    except:
        pass
    return res

def notify_approvers_new_user(uid, fio):
    approvers = get_approvers()
    if not approvers:
        return
    kb = {
        "keyboard": [[{"text": f"/approve_{uid}"}, {"text": f"/reject_{uid}"}]],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    text = f"<b>Новая заявка на доступ</b>\nФИО: {fio}\nID: <code>{uid}</code>\n\nНажмите кнопку:"
    for a in approvers:
        try:
            send(a, text, kb)
        except:
            pass

# ==================== Обработка команд подтверждения ====================
def handle_admin_commands(uid, text, chat):
    if text.startswith("/approve_") or text.startswith("/reject_") or text.startswith("/setrole_"):
        try:
            cmd = text.split("_", 1)[0][1:]
            target_id = int(text.split("_", 1)[1].split("_", 1)[0])
        except:
            send(chat, "Неверная команда.")
            return True

        approver = get_user(uid)
        if not approver or approver["status"] != "подтвержден" or approver["role"] not in ("admin", "master"):
            send(chat, "У вас нет прав.")
            return True

        target = get_user(target_id)
        if not target:
            send(chat, "Пользователь не найден.")
            return True

        if text.startswith("/approve_"):
            roles = ["operator", "master"]
            if approver["role"] == "admin":
                roles.append("admin")
            kb_rows = [[f"/setrole_{target_id}_{r}"] for r in roles] + [["Отмена"]]
            kb = {"keyboard": [[{"text": t} for t in row] for row in kb_rows], "resize_keyboard": True}
            send(chat, f"Подтверждение пользователя:\n<b>{target['fio']}</b>\nВыберите роль:", kb)
            return True

        if text.startswith("/reject_"):
            update_user(target_id, status="отклонен", confirmed_by=uid)
            send(chat, f"Заявка {target['fio']} отклонена.")
            send(target_id, "Ваш доступ отклонён администратором.")
            return True

        if text.startswith("/setrole_"):
            try:
                role = text.split("_", 2)[2]
                if role not in ("operator", "master", "admin"):
                    return True
                if approver["role"] == "master" and role == "admin":
                    send(chat, "Мастер не может назначать роль admin.")
                    return True
                update_user(target_id, role=role, status="подтвержден", confirmed_by=uid)
                send(chat, f"Пользователь {target['fio']} подтверждён как <b>{role}</b>.")
                send(target_id, f"Доступ подтверждён!\nВаша роль: <b>{role}</b>")
            except:
                pass
            return True
    return False

# ==================== Поиск последней активной записи ====================
def find_last_active_record(ws, user_repr):
    values = ws.get_all_values()
    if ws.title == "Брак":
        user_col = 7
        status_col = 9
    else:
        user_col = 8
        status_col = 10

    for i in range(len(values)-1, 0, -1):
        row = values[i]
        if len(row) <= user_col:
            continue
        if row[user_col].strip() == user_repr and (len(row) <= status_col or row[status_col].strip() != "ОТМЕНЕНО"):
            return row, i+1
    return None, None

# ==================== Основная логика ====================
def process(uid, chat, text, user_repr):
    last_activity[uid] = time.time()

    if uid not in states:
        states[uid] = {"chat": chat, "cancel_used": False}

    state = states[uid]

    # === Обработка команд админов ===
    if handle_admin_commands(uid, text, chat):
        return

    # === Авторизация ===
    user = get_user(uid)

    if user is None:
        if state.get("waiting_fio"):
            fio = text.strip()
            if not fio:
                send(chat, "Введите ваше ФИО:")
                return
            add_user(uid, fio)
            notify_approvers_new_user(uid, fio)
            send(chat, "Спасибо! Ваша заявка отправлена на подтверждение администратору.")
            states.pop(uid, None)
            return
        state["waiting_fio"] = True
        send(chat, "Привет! Вы не зарегистрированы в системе.\n\nВведите ваше <b>ФИО</b> для регистрации:")
        return

    if user["status"] != "подтвержден":
        send(chat, "Ваш доступ ожидает подтверждения администратором.")
        return

    # === ПОЛЬЗОВАТЕЛЬ ПОДТВЕРЖДЁН — РАБОТАЕМ КАК РАНЬШЕ ===
    if text == "Назад":
        states.pop(uid, None)
        send(chat, "Главное меню:", MAIN_KB)
        return
    
    if text == "Отмена":
        states.pop(uid, None)
        last_activity.pop(uid, None)
        send(chat, "Отменено.", MAIN_KB)
        return

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

    # === Отмена последней записи ===
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
        state["cancel_used"] = False

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

    # === Обработка шагов ===
    st = state
    step = st.get("step")
    data = st.get("data", {})

    if not step:
        send(chat, "Выберите действие:", FLOW_MENU_KB)
        return

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

    if step == "action":
        if text not in ("Запуск", "Остановка"):
            send(chat, "Выберите действие:", keyboard([["Запуск", "Остановка"], ["Отмена"]]))
            return
        data["action"] = "запуск" if text == "Запуск" else "остановка"
        if data["action"] == "запуск":
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
            
            prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
            kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
            send(chat, "Префикс ЗНП:", keyboard(kb))
        else:
            st["step"] = "reason"
            send(chat, "Причина остановки:", get_reasons_kb())
        return

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

    if step == "meters":
        if not text.isdigit() or int(text) <= 0:
            send(chat, "Укажите количество метров брака (число > 0):", CANCEL_KB)
            return
        data["meters"] = text
        st["step"] = "defect_type"
        send(chat, "Вид брака:", get_defect_kb())
        return

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
    if not update or "message" not in update:
        return "ok", 200
    m = update["message"]
    chat_id = m["chat"]["id"]
    user_id = m["from"]["id"]
    text = (m.get("text") or "").strip()
    username = m["from"].get("username", "")
    user_repr = f"{user_id} (@{username or 'no_user'})"

    with FileLock(LOCK_PATH):
        process(user_id, chat_id, text, user_repr)
    return "ok", 200

@app.route("/")
def index():
    return "Bot is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
