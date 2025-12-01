# V 4.1 — Авторизация + выбор роли при подтверждении (полный рабочий код)
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
REQUESTS_SHEET = "Заявки на доступ"

HEADERS_STARTSTOP = ["Дата","Время","Номер линии","Действие","Причина","ЗНП","Метров брака","Вид брака","Пользователь","Время отправки","Статус"]

def get_ws(sheet_name, headers=None):
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=3000, cols=20)
        if headers:
            ws.insert_row(headers, 1)
    else:
        if headers and ws.row_values(1) != headers:
            ws.clear()
            ws.insert_row(headers, 1)
    return ws

ws_startstop = get_ws(STARTSTOP_SHEET, HEADERS_STARTSTOP)
ws_defect = get_ws(DEFECT_SHEET)
ws_users = get_ws(USERS_SHEET, ["user_id", "username", "ФИО", "Роль", "Статус", "Дата регистрации"])
ws_requests = get_ws(REQUESTS_SHEET, ["user_id", "username", "ФИО", "Дата заявки", "Статус"])

# ==================== Роли ====================
ROLE_ADMIN = "Администратор"
ROLE_MASTER = "Мастер"
ROLE_OPERATOR = "Оператор"

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

# ==================== Последние записи ====================
def get_last_records(ws, n=2):
    try:
        values = ws.get_all_values()
        if len(values) <= 1: return []
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

# ==================== Пользователи ====================
def get_user_info(user_id):
    try:
        cell = ws_users.find(str(user_id), in_column=1)
        if not cell: return None
        row = ws_users.row_values(cell.row)
        return {
            "row": cell.row,
            "user_id": row[0],
            "username": row[1],
            "fio": row[2],
            "role": row[3],
            "status": row[4],
            "registered": row[5]
        }
    except:
        return None

def is_authorized(user_id):
    user = get_user_info(user_id)
    return user and user["status"] == "активен"

def get_user_role(user_id):
    user = get_user_info(user_id)
    return user["role"] if user else None

def get_approvers():
    try:
        values = ws_users.get_all_values()
        ids = []
        for row in values[1:]:
            if len(row) >= 5 and row[4] == "активен" and row[3] in (ROLE_ADMIN, ROLE_MASTER):
                ids.append(int(row[0]))
        return ids
    except:
        return []

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
        msg = (f"НОВАЯ ЗАПИСЬ БРАКА\nЛиния: {data['line']}\n"
               f"{data['date']} {data['time']}\nЗНП: <code>{data.get('znp','—')}</code>\n"
               f"Метров брака: {data['meters']}\nВид брака: {data.get('defect_type','—')}")
        notify_controllers(controllers_defect, msg)
    else:
        row = [data["date"], data["time"], data["line"], data["action"],
               data.get("reason", ""), data.get("znp", ""), data.get("meters",""),
               data.get("defect_type",""), user, ts, ""]
        action_ru = "Запуск" if data["action"] == "запуск" else "Остановка"
        msg = (f"НОВАЯ ЗАПИСЬ СТАРТ/СТОП\nЛиния: {data['line']}\n"
               f"{data['date']} {data['time']}\nДействие: {action_ru}\nПричина: {data.get('reason','—')}")
        notify_controllers(controllers_startstop, msg)

    ws.append_row(row, value_input_option="USER_ENTERED")

# ==================== Клавиатуры ====================
def keyboard(rows):
    return {"keyboard": [[{"text": t} for t in row] for row in rows],
            "resize_keyboard": True, "one_time_keyboard": False}

MAIN_KB = keyboard([["Старт/Стоп", "Брак"]])
FLOW_MENU_KB = keyboard([["Новая запись"], ["Отменить последнюю запись"], ["Назад"]])
CANCEL_KB = keyboard([["Отмена"]])
CONFIRM_KB = keyboard([["Да, отменить"], ["Нет, оставить"]])

NUM_LINE_KB = {
    "keyboard": [[{"text": str(i)} for i in range(1,6)],
                 [{"text": str(i)} for i in range(6,11)],
                 [{"text": str(i)} for i in range(11,16)] + [{"text": "15"}],
                 [{"text": "Отмена"}]],
    "resize_keyboard": True, "one_time_keyboard": True,
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

def edit_message(chat_id, message_id, text, markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText", json=payload)

def answer_callback(cq_id, text=""):
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                  json={"callback_query_id": cq_id, "text": text})

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

# ==================== Поиск последней активной записи ====================
def find_last_active_record(ws, user_repr):
    values = ws.get_all_values()
    user_col = 7 if ws.title == "Брак" else 8
    status_col = 9 if ws.title == "Брак" else 10

    for i in range(len(values)-1, 0, -1):
        row = values[i]
        if len(row) <= user_col: continue
        if row[user_col].strip() == user_repr and (len(row) <= status_col or row[status_col].strip() != "ОТМЕНЕНО"):
            return row, i+1
    return None, None

# ==================== Основная логика ====================
def process(uid, chat, text, user_repr, username=""):
    last_activity[uid] = time.time()

    user_info = get_user_info(uid)

    # === Неавторизованные пользователи ===
    if not user_info:
        if "awaiting_fio" in states.get(uid, {}):
            fio = text.strip()
            if len(fio) < 5:
                send(chat, "ФИО слишком короткое. Введите ещё раз:")
                return

            ts = now_msk().strftime("%d.%m.%Y %H:%M")
            ws_requests.append_row([str(uid), username or "", fio, ts, "ожидает"])

            approvers = get_approvers()
            user_link = f"<a href='tg://user?id={uid}'>{fio}</a>"
            for app_id in approvers:
                data_prefix = f"{uid}_{fio.replace(' ', '_')}"
                kb = {
                    "inline_keyboard": [
                        [{"text": "Оператор",   "callback_data": f"setrole_{data_prefix}_Оператор"}],
                        [{"text": "Мастер",     "callback_data": f"setrole_{data_prefix}_Мастер"}]
                    ]
                }
                # Администратор видит дополнительно кнопку Админ
                if get_user_role(app_id) == ROLE_ADMIN:
                    kb["inline_keyboard"].append([{"text": "Администратор", "callback_data": f"setrole_{data_prefix}_Администратор"}])
                kb["inline_keyboard"].append([{"text": "Отклонить", "callback_data": f"reject_{data_prefix}"}])

                send(app_id,
                     f"Новая заявка на доступ\n\n"
                     f"Пользователь: {user_link}\n"
                     f"ID: <code>{uid}</code>\n"
                     f"Username: @{username or '—'}\n"
                     f"ФИО: {fio}\n"
                     f"Время: {ts}",
                     kb)

            send(chat, "Ваша заявка отправлена. Ожидайте подтверждения.")
            states.pop(uid, None)
            return

        states[uid] = {"awaiting_fio": True}
        send(chat, "Добро пожаловать!\nДля работы с ботом нужна авторизация.\n\nВведите свои ФИО:")
        return

    if user_info["status"] != "активен":
        send(chat, "Ваша заявка ещё не подтверждена или была отклонена.")
        return

    # === Авторизованные пользователи ===
    if uid not in states:
        states[uid] = {"chat": chat, "cancel_used": False, "role": user_info["role"], "fio": user_info["fio"]}

    state = states[uid]

    # === Весь старый функционал (без изменений) ===
    if text == "Назад":
        states.pop(uid, None)
        send(chat, "Главное меню:", MAIN_KB)
        return

    if text == "Отмена":
        state.pop("pending_cancel", None)
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
            send(chat, "Вы уже отменили одну запись в этом сеансе.", FLOW_MENU_KB)
            return

        ws = ws_defect if flow == "defect" else ws_startstop
        row, row_num = find_last_active_record(ws, user_repr)
        if not row:
            send(chat, "У вас нет активных записей для отмены.", FLOW_MENU_KB)
            return

        state["pending_cancel"] = {"ws": ws, "row": row, "row_num": row_num}

        if flow == "startstop":
            action = "Запуск" if row[3] == "запуск" else "Остановка"
            msg = f"Отменить эту запись?\n\n<b>Старт/Стоп</b>\n{row[0]} {row[1]} | Линия {row[2]}\nДействие: {action}\nПричина: {row[4] if len(row)>4 else '—'}"
        else:
            msg = f"Отменить эту запись?\n\n<b>Брак</b>\n{row[0]} {row[1]} | Линия {row[2]}\nЗНП: <code>{row[4]}</code>\nМетров: {row[5]}\nВид брака: {row[6] if len(row)>6 else '—'}"

        send(chat, msg, CONFIRM_KB)
        return

    if "pending_cancel" in state:
        pend = state["pending_cancel"]
        if text == "Да, отменить":
            ws = pend["ws"]; row = pend["row"]; row_num = pend["row_num"]
            status_col = "K" if flow == "startstop" else "J"
            ws.update(f"{status_col}{row_num}", [["ОТМЕНЕНО"]])

            if flow == "startstop":
                action = "Запуск" if row[3] == "запуск" else "Остановка"
                notify_controllers(controllers_startstop,
                    f"ОТМЕНЕНА ЗАПИСЬ СТАРТ/СТОП\nПользователь: {user_repr}\nЛиния: {row[2]}\n{row[0]} {row[1]}\nДействие: {action}")
                send(chat, f"Запись отменена.", FLOW_MENU_KB)
            else:
                notify_controllers(controllers_defect,
                    f"ОТМЕНЕНА ЗАПИСЬ БРАКА\nПользователь: {user_repr}\nЛиния: {row[2]}\n{row[0]} {row[1]}\nЗНП: <code>{row[4]}</code>\nМетров: {row[5]}")
                send(chat, f"Запись брака отменена.", FLOW_MENU_KB)

            state["cancel_used"] = True
            state.pop("pending_cancel", None)
            return

        if text == "Нет, оставить":
            send(chat, "Запись сохранена.", FLOW_MENU_KB)
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
            curr = now_msk().strftime("%m%y")
            prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
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
            curr = now_msk().strftime("%m%y")
            prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
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
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
        kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
        send(chat, "Префикс ЗНП:", keyboard(kb))
        return

    # --- Step: znp_prefix / znp_manual ---
    if step in ("znp_prefix", "znp_manual"):
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
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

    # === Обработка callback_query (выбор роли / отклонение) ===
    if update.get("callback_query"):
        cq = update["callback_query"]
        data = cq["data"]
        from_id = cq["from"]["id"]
        msg = cq["message"]
        chat_id = msg["chat"]["id"]
        message_id = msg["message_id"]

        if data.startswith("setrole_") or data.startswith("reject_"):
            approver_role = get_user_role(from_id)
            if approver_role not in (ROLE_ADMIN, ROLE_MASTER):
                answer_callback(cq["id"], "Нет прав")
                return "ok", 200

            if data.startswith("reject_"):
                _, uid_str, fio_enc = data.split("_", 2)
                uid = int(uid_str)
                fio = fio_enc.replace("_", " ")
                cell = ws_requests.find(str(uid), in_column=1)
                if cell:
                    ws_requests.update_cell(cell.row, 5, "отклонено")
                send(uid, "В доступе отказано.")
                edit_message(chat_id, message_id, msg["text"] + "\n\nОтклонено")
                answer_callback(cq["id"], "Отклонено")
                return "ok", 200

            if data.startswith("setrole_"):
                _, uid_str, fio_enc, role_ru = data.split("_", 3)
                uid = int(uid_str)
                fio = fio_enc.replace("_", " ")

                if role_ru == ROLE_ADMIN and approver_role != ROLE_ADMIN:
                    answer_callback(cq["id"], "Только админ может назначать админа")
                    return "ok", 200
                if role_ru == ROLE_MASTER and approver_role == ROLE_OPERATOR:
                    answer_callback(cq["id"], "Нет прав")
                    return "ok", 200

                if is_authorized(uid):
                    answer_callback(cq["id"], "Уже подтверждён")
                    return "ok", 200

                ws_users.append_row([
                    str(uid),
                    cq["from"].get("username", ""),
                    fio,
                    role_ru,
                    "активен",
                    now_msk().strftime("%d.%m.%Y")
                ])

                cell = ws_requests.find(str(uid), in_column=1)
                if cell:
                    ws_requests.delete_rows(cell.row)

                send(uid, f"Доступ подтверждён!\nВаша роль: <b>{role_ru}</b>\nТеперь вы можете пользоваться ботом.")
                edit_message(chat_id, message_id, msg["text"] + f"\n\nПодтверждено как <b>{role_ru}</b>")
                answer_callback(cq["id"], "Готово")
                return "ok", 200

        return "ok", 200

    # === Обычные сообщения ===
    if not update or "message" not in update:
        return "ok", 200

    m = update["message"]
    chat_id = m["chat"]["id"]
    user_id = m["from"]["id"]
    text = (m.get("text") or "").strip()
    username = m["from"].get("username", "")
    user_repr = f"{user_id} (@{username or 'no_user'})"

    with FileLock(LOCK_PATH):
        process(user_id, chat_id, text, user_repr, username)

    return "ok", 200

@app.route("/")
def index():
    return "Bot is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
