# V 3.9.4 Final — Отмена только одной записи за сеанс (сброс после завершения записи)
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

HEADERS_STARTSTOP = ["Дата","Время","Номер линии","Действие","Причина","ЗНП","Метров брака","Вид брака","Пользователь","Время отправки","Статус"]

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
    except: return []

# ==================== Уведомления ====================
def notify_controllers(ids, message):
    for cid in ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": message, "parse_mode": "HTML"},
                timeout=10
            )
        except: pass

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
               f"Метров: {data['meters']}\nВид брака: {data.get('defect_type','—')}")
        notify_controllers(controllers_defect, msg)
    else:
        row = [data["date"], data["time"], data["line"], data["action"],
               data.get("reason", ""), data.get("znp", ""), data.get("meters",""),
               data.get("defect_type",""), user, ts, ""]
        action_ru = "Запуск" if data["action"] == "запуск" else "Остановка"
        msg = (f"НОВАЯ ЗАПИСЬ СТАРТ/СТОП\nЛиния: {data['line']}\n"
               f"{data['date']} {data['time']}\nДействие: {action_ru}\n"
               f"Причина: {data.get('reason','—')}")
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
                 [{"text": str(i)} for i in range(11,16)],
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

# ==================== Отправка ====================
def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception("send error")

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
    try:
        values = ws.get_all_values()
        if len(values) <= 1: return None, None
        header = values[0]
        try:
            status_idx = header.index("Статус")
        except:
            status_idx = 10 if len(header) > 10 else 9
        for i in range(len(values)-1, 0, -1):
            row = values[i]
            if len(row) < 9 or row[8].strip() != user_repr: continue
            status = row[status_idx].strip() if len(row) > status_idx else ""
            if status != "ОТМЕНЕНО":
                return row, i+1
        return None, None
    except: return None, None

# ==================== Основная логика ====================
def process(uid, chat, text, user_repr):
    last_activity[uid] = time.time()

    if text == "Назад":
        states.pop(uid, None)
        send(chat, "Главное меню:", MAIN_KB)
        return
    if text == "Отмена":
        states.pop(uid, None)
        send(chat, "Отменено.", MAIN_KB)
        return

    # === Вход в раздел ===
    if uid not in states:
        if text in ("/start", "Старт/Стоп"):
            send(chat, "<b>Старт/Стоп</b>\nВыберите действие:", FLOW_MENU_KB)
            states[uid] = {"flow": "startstop", "chat": chat, "can_cancel": True}
            return
        elif text == "Брак":
            send(chat, "<b>Брак</b>\nВыберите действие:", FLOW_MENU_KB)
            states[uid] = {"flow": "defect", "chat": chat, "can_cancel": True}
            return
        else:
            send(chat, "Выберите действие:", MAIN_KB)
            return

    flow = states[uid]["flow"]

    # === Отмена последней записи ===
    if text == "Отменить последнюю запись":
        if not states[uid].get("can_cancel", False):
            send(chat, "Вы уже отменили одну запись.\nЗавершите новую запись, чтобы снова получить возможность отмены.", FLOW_MENU_KB)
            return

        ws = ws_defect if flow == "defect" else ws_startstop
        row, row_num = find_last_active_record(ws, user_repr)
        if not row:
            send(chat, "У вас нет активных записей для отмены.", FLOW_MENU_KB)
            return

        states[uid]["pending_cancel"] = {"ws": ws, "row": row, "row_num": row_num}

        if flow == "startstop":
            action = "Запуск" if row[3] == "запуск" else "Остановка"
            msg = f"Отменить запись?\n\n<b>Старт/Стоп</b>\n{row[0]} {row[1]} | Линия {row[2]}\nДействие: {action}\nПричина: {row[4] if len(row)>4 else '—'}"
        else:
            msg = f"Отменить запись?\n\n<b>Брак</b>\n{row[0]} {row[1]} | Линия {row[2]}\nЗНП: <code>{row[4] if len(row)>4 else '—'}</code>\nМетров: {row[5] if len(row)>5 else '—'}"

        send(chat, msg, CONFIRM_KB)
        return

    # === Подтверждение отмены ===
    if "pending_cancel" in states[uid]:
        if text == "Да, отменить":
            pend = states[uid]["pending_cancel"]
            ws, row, row_num = pend["ws"], pend["row"], pend["row_num"]
            col = "K" if flow == "startstop" else "J"
            ws.update(f"{col}{row_num}", [["ОТМЕНЕНО"]])

            states[uid]["can_cancel"] = False  # Блокируем повторную отмену

            if flow == "startstop":
                action = "Запуск" if row[3] == "запуск" else "Остановка"
                notify_controllers(controllers_startstop, f"ОТМЕНЕНА ЗАПИСЬ СТАРТ/СТОП\nПользователь: {user_repr}\nЛиния: {row[2]}\n{row[0]} {row[1]}\nДействие: {action}")
                send(chat, "Запись отменена.", FLOW_MENU_KB)
            else:
                notify_controllers(controllers_defect, f"ОТМЕНЕНА ЗАПИСЬ БРАКА\nПользователь: {user_repr}\nЛиния: {row[2]}\n{row[0]} {row[1]}")
                send(chat, "Запись брака отменена.", FLOW_MENU_KB)

            states[uid].pop("pending_cancel", None)
            return
        if text == "Нет, оставить":
            send(chat, "Запись сохранена.", FLOW_MENU_KB)
            states[uid].pop("pending_cancel", None)
            return

    # === Новая запись ===
    if text == "Новая запись":
        if flow == "defect":
            records = get_last_records(ws_defect, 2)
            msg = "<b>Последние записи Брака:</b>\n\n" + ("\n".join(
                f"• {r[0]} {r[1]} | Линия {r[2]} | <code>{r[4] if len(r)>4 else '—'}</code> | {r[5] if len(r)>5 else '—'}м" for r in records
            ) if records else "Нет записей.")
            send(chat, msg)
            states[uid].update({"step": "line", "data": {"action": "брак"}})
        else:
            records = get_last_records(ws_startstop, 2)
            msg = "<b>Последние записи Старт/Стоп:</b>\n\n" + ("\n".join(
                f"• {r[0]} {r[1]} | Линия {r[2]} | {'Запуск' if r[3]=='запуск' else 'Остановка'} | {r[4] if len(r)>4 else '—'}" for r in records
            ) if records else "Нет записей.")
            send(chat, msg)
            states[uid].update({"step": "line", "data": {}})
        send(chat, "Введите номер линии (1–15):", NUM_LINE_KB)
        return

    # === Все шаги ввода ===
    if "step" not in states[uid]:
        send(chat, "Выберите действие:", FLOW_MENU_KB)
        return

    st = states[uid]
    step = st["step"]
    data = st["data"]

    # --- Линия ---
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

    # --- Дата ---
    if step == "date":
        if text == "Другая дата":
            st["step"] = "date_custom"
            send(chat, "Введите дату (дд.мм.гггг):", CANCEL_KB)
            return
        try:
            datetime.strptime(text, "%d.%m.%Y")
            data["date"] = text
            st["step"] = "time"
            now = now_msk()
            t = [now.strftime("%H:%M"), (now-timedelta(minutes=10)).strftime("%H:%M"),
                 (now-timedelta(minutes=20)).strftime("%H:%M"), (now-timedelta(minutes=30)).strftime("%H:%M")]
            send(chat, "Время:", keyboard([[t[0], t[1], "Другое время"], [t[2], t[3], "Отмена"]]))
        except:
            send(chat, "Неверный формат даты.", keyboard([[today, yest], ["Другая дата", "Отмена"]]))
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

    # --- Время ---
    if step == "time":
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

    if step == "time_custom":
        if not (len(text) == 5 and text[2] == ":" and text[:2].isdigit() and text[3:].isdigit()):
            send(chat, "Формат времени: чч:мм", CANCEL_KB)
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

    # --- Действие (только Старт/Стоп) ---
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

    # --- Причина остановки ---
    if step == "reason":
        if text == "Другое":
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

    if step == "reason_custom":
        data["reason"] = text
        st["step"] = "znp_prefix"
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
        kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
        send(chat, "Префикс ЗНП:", keyboard(kb))
        return

    # --- ЗНП ---
    if step == "znp_prefix":
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
        valid = [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]
        if text in valid:
            data["znp_prefix"] = text
            st["step"] = "znp_digits"
            send(chat, f"Последние 4 цифры ЗНП для <b>{text}</b>-XXXX:", CANCEL_KB)
            return
        if text == "Другое":
            st["step"] = "znp_manual"
            send(chat, "Введите полный ЗНП (например, D1225-1234):", CANCEL_KB)
            return
        send(chat, "Выберите префикс:", keyboard([[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]))
        return

    if step == "znp_digits":
        if text.isdigit() and len(text) == 4:
            data["znp"] = f"{data['znp_prefix']}-{text}"
            st["step"] = "meters"
            send(chat, "Сколько метров брака?", CANCEL_KB)
            return
        send(chat, "Введите ровно 4 цифры.", CANCEL_KB)
        return

    if step == "znp_manual":
        if len(text) == 10 and text[5] == "-" and text[:5].upper() in [f"D{now_msk().strftime('%m%y')}", f"L{now_msk().strftime('%m%y')}", f"D{(now_msk() - timedelta(days=35)).strftime('%m%y')}", f"L{(now_msk() - timedelta(days=35)).strftime('%m%y')}"]:
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

    # --- Вид брака (финал) ---
    if step == "defect_type":
        if text == "Другое":
            st["step"] = "defect_custom"
            send(chat, "Опишите вид брака:", CANCEL_KB)
            return
        data["defect_type"] = "" if text == "Без брака" else text
        data["user"] = user_repr
        data["flow"] = flow
        append_row(data)

        states[uid]["can_cancel"] = True  # Сброс после успешного завершения записи

        send(chat, f"<b>Записано!</b>\nЛиния {data['line']} • {data['date']} {data['time']}", MAIN_KB)
        states.pop(uid, None)
        return

    if step == "defect_custom":
        data["defect_type"] = text
        data["user"] = user_repr
        data["flow"] = flow
        append_row(data)

        states[uid]["can_cancel"] = True  # Сброс после успешного завершения записи

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
    if not update or "message" not in update: return "ok", 200
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
    return "Бот работает!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
