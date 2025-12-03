# V 3.9 — Отмена с подтверждением + защита от дублей + ПОЛНАЯ АВТОРИЗАЦИЯ ПОЛЬЗОВАТЕЛЕЙ
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
USERS_SHEET = "Пользователи"  # ← Новый лист для пользователей

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
ws_users = get_ws(USERS_SHEET, USERS_HEADERS)  # ← Создаём/получаем лист пользователей

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
    controllers_startstop = get_controllers(CTRL_STARTSTOP_SHEET)
    controllers_defect = get_controllers(CTRL_DEFECT_SHEET)

# Обновление контролёров раз в сутки
threading.Thread(target=lambda: [time.sleep(86400), refresh_controllers()], daemon=True).start()

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

# ==================== АВТОРИЗАЦИЯ ПОЛЬЗОВАТЕЛЕЙ ====================
def get_user(uid):
    try:
        values = ws_users.get_all_values()
        for row in values[1:]:
            if row and len(row) > 0 and row[0] == str(uid):
                fio = row[1] if len(row) > 1 else ""
                role = row[2].strip() if len(row) > 2 else "operator"
                status = row[3].strip() if len(row) > 3 else "ожидает"
                return {"id": str(uid), "fio": fio.strip(), "role": role, "status": status}
    except Exception as e:
        log.exception("get_user error: %s", e)
    return None

def add_user(uid, fio):
    try:
        ws_users.append_row([
            str(uid), fio.strip(), "operator", "ожидает", "", now_msk().strftime("%Y-%m-%d %H:%M:%S"), "", ""
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
        "inline_keyboard": [[
            {"text": "Подтвердить", "callback_data": f"approve_{uid}"},
            {"text": "Отклонить", "callback_data": f"reject_{uid}"}
        ]]
    }
    text = f"<b>Новая заявка на доступ</b>\nФИО: {fio}\nID: <code>{uid}</code>"
    for a in approvers:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": a, "text": text, "parse_mode": "HTML", "reply_markup": json.dumps(kb)}
            )
        except:
            pass

# ==================== Поиск последней активной записи ====================
def find_last_active_record(ws, user_repr):
    values = ws.get_all_values()
    user_col = 7 if ws.title == "Брак" else 8
    status_col = 9 if ws.title == "Брак" else 10

    for i in range(len(values)-1, 0, -1):
        row = values[i]
        if len(row) <= user_col:
            continue
        if row[user_col].strip() == user_repr and (len(row) <= status_col or row[status_col].strip() != "ОТМЕНЕНО"):
            return row, i+1
    return None, None

# ==================== Обработка callback от инлайн-кнопок ====================
def handle_callback(callback):
    data = callback["data"]
    uid = callback["from"]["id"]
    chat_id = callback["message"]["chat"]["id"]

    if data.startswith("approve_") or data.startswith("reject_"):
        target_id = int(data.split("_")[1])
        user = get_user(uid)
        if not user or user["role"] not in ("admin", "master") or user["status"] != "подтвержден":
            return

        target = get_user(target_id)
        if not target:
            return

        if data.startswith("approve_"):
            roles = ["operator", "master"]
            if user["role"] == "admin":
                roles.append("admin")
            kb = {"inline_keyboard": [[{"text": r, "callback_data": f"setrole_{target_id}_{r}"}] for r in roles]}
            text = f"Выберите роль для <b>{target['fio']}</b>:"
            send(chat_id, text, kb)
        elif data.startswith("reject_"):
            update_user(target_id, status="отклонен", confirmed_by=uid)
            send(chat_id, f"Заявка отклонена: {target['fio']}")
            send(target_id, "Ваша заявка на доступ отклонена.")

    elif data.startswith("setrole_"):
        parts = data.split("_")
        target_id = int(parts[1])
        role = parts[2]
        if role not in ("operator  master", "admin"):
            return
        confirmer = get_user(uid)
        if confirmer["role"] == "master" and role == "admin":
            send(chat_id, "Мастер не может назначать админа.")
            return
        update_user(target_id, role=role, status="подтвержден", confirmed_by=uid)
        target = get_user(target_id)
        send(chat_id, f"Пользователь {target['fio']} подтверждён как <b>{role}</b>")
        send(target_id, f"Доступ подтверждён!\nРоль: <b>{role}</b>\nТеперь вы можете пользоваться ботом.")

# ==================== Основная логика ====================
def process(uid, chat, text, user_repr):
    last_activity[uid] = time.time()

    if uid not in states:
        states[uid] = {"chat": chat, "cancel_used": False}

    state = states[uid]
    user = get_user(uid)

    # === Первый вход — регистрация ===
    if user is None:
        if state.get("waiting_fio"):
            fio = text.strip()
            if not fio:
                send(chat, "Введите ваше ФИО:")
                return
            add_user(uid, fio)
            notify_approvers_new_user(uid, fio)
            send(chat, "Спасибо! Ваша заявка отправлена на подтверждение администратору.\nОжидайте...")
            states.pop(uid, None)
            return
        state["waiting_fio"] = True
        send(chat, "Привет! Это бот учёта простоев и брака.\n\nДля начала введите ваше <b>ФИО</b>:")
        return

    # === Ожидает подтверждения ===
    if user["status"] != "подтвержден":
        send(chat, "Ваша заявка ожидает подтверждения администратором.\nКак только вас подтвердят — вы сможете работать.")
        return

    # === ПОЛЬЗОВАТЕЛЬ ПОДТВЕРЖДЁН — РАБОТАЕМ ===
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
            send(chat, f"Привет, {user['fio'].split()[0]}!\nВыберите действие:", MAIN_KB)
            return

    flow = state["flow"]

    # === Отмена записи ===
    if text == "Отменить последнюю запись":
        if state.get("cancel_used", False):
            send(chat, "Вы уже отменили одну запись. Сначала сделайте новую.", FLOW_MENU_KB)
            return
        ws = ws_defect if flow == "defect" else ws_startstop
        row, row_num = find_last_active_record(ws, user_repr)
        if not row:
            send(chat, "У вас нет активных записей для отмены.", FLOW_MENU_KB)
            return
        state["pending_cancel"] = {"ws": ws, "row": row, "row_num": row_num}
        action = "Запуск" if flow == "startstop" and row[3] == "запуск" else "Остановка" if flow == "startstop" else ""
        msg = f"Отменить запись?\n\n"
        if flow == "startstop":
            msg += f"<b>Старт/Стоп</b>\n{row[0]} {row[1]} | Линия {row[2]}\nДействие: {action}\nПричина: {row[4] if len(row)>4 else '—'}"
        else:
            msg += f"<b>Брак</b>\n{row[0]} {row[1]} | Линия {row[2]}\nЗНП: <code>{row[4]}</code>\nМетров: {row[5]}\nВид: {row[6] if len(row)>6 else '—'}"
        send(chat, msg, CONFIRM_KB)
        return

    if "pending_cancel" in state:
        if text == "Да, отменить":
            pend = state["pending_cancel"]
            status_col = "K" if flow == "startstop" else "J"
            pend["ws"].update(f"{status_col}{pend['row_num']}", [["ОТМЕНЕНО"]])
            send(chat, "Запись успешно отменена!", FLOW_MENU_KB)
            state["cancel_used"] = True
            state.pop("pending_cancel", None)
            return
        if text == "Нет, оставить":
            send(chat, "Отмена отменена.", FLOW_MENU_KB)
            state.pop("pending_cancel", None)
            return

    # === Новая запись и вся остальная логика (полностью сохранена из твоей версии) ===
    if text == "Новая запись":
        state["cancel_used"] = False
        if flow == "defect":
            records = get_last_records(ws_defect, 2)
            msg = "<b>Последние записи Брака:</b>\n\n" + "\n".join(
                f"• {r[0]} {r[1]} | Линия {r[2]} | <code>{r[4] if len(r)>4 else '—'}</code> | {r[5] if len(r)>5 else '—'}м"
                for r in records) if records else "Нет записей."
            send(chat, msg)
            state.update({"step": "line", "data": {"action": "брак"}})
        else:
            records = get_last_records(ws_startstop, 2)
            msg = "<b>Последние записи Старт/Стоп:</b>\n\n" + "\n".join(
                f"• {r[0]} {r[1]} | Линия {r[2]} | {'Запуск' if r[3]=='запуск' else 'Остановка'} | {r[4] if len(r)>4 else '—'}"
                for r in records) if records else "Нет записей."
            send(chat, msg)
            state.update({"step": "line", "data": {}})
        send(chat, "Введите номер линии (1–15):", NUM_LINE_KB)
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
    if state.get("step") in ("defect_type", "defect_custom"):
            if text == "Другое" and state["step"] == "defect_type":
                state["step"] = "defect_custom"
                send(chat, "Опишите вид брака:", CANCEL_KB)
                return
            data = state["data"]
            data["defect_type"] = "" if text == "Без брака" else text
            data["user"] = f"{user['fio']} ({uid})"
            data["flow"] = flow
            append_row(data)
            send(chat, f"<b>Записано!</b>\nЛиния {data['line']} • {data['date']} {data['time']}", MAIN_KB)
            states.pop(uid, None)
            return
    
        # (остальные шаги — полностью как у тебя, не трогаю)
    
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

    if "callback_query" in update:
        handle_callback(update["callback_query"])
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery", json={
            "callback_query_id": update["callback_query"]["id"]
        })
        return "ok", 200

    if "message" not in update:
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
