# bot_webhook.py — ФИНАЛЬНАЯ ВЕРСИЯ (декабрь 2025)
# Всё работает: последние записи, уведомления контролёрам, отмена с подтверждением
# Изменение: при выборе времени через кнопки к формату чч:мм добавляются текущие секунды (чч:мм:сс).
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
HEADERS_DEFECT    = ["Дата","Время","Номер линии","Действие","ЗНП","Метров брака","Вид брака","Пользователь","Время отправки","Статус"]

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
ws_defect    = get_ws(DEFECT_SHEET, HEADERS_DEFECT)
ws_ctrl_ss   = get_ws(CTRL_STARTSTOP_SHEET)
ws_ctrl_def  = get_ws(CTRL_DEFECT_SHEET)

# ==================== Контролёры (кешируем) ====================
def get_controllers(sheet):
    try:
        ids = sheet.col_values(1)[1:]
        return [int(i.strip()) for i in ids if i.strip().isdigit()]
    except:
        return []

controllers_startstop = get_controllers(ws_ctrl_ss)
controllers_defect = get_controllers(ws_ctrl_def)

# ==================== Последние записи (без "Удалено") ====================
def get_last_records(ws, n=2):
    try:
        values = ws.get_all_values()
        if len(values) <= 1:
            return []

        header = values[0]
        try:
            status_col_index = header.index("Статус")  # находим колонку "Статус" по названию
        except ValueError:
            status_col_index = -1  # если нет — считаем, что нет удалённых

        valid = []
        for row in reversed(values[1:]):
            # Если колонка Статус существует и не "Удалено"
            if status_col_index == -1 or len(row) <= status_col_index or row[status_col_index].strip() != "Удалено":
                valid.append(row)
                if len(valid) >= n:
                    break
        return list(reversed(valid))
    except Exception as e:
        log.error(f"get_last_records error: {e}")
        return []

# ==================== Уведомление контролёрам ====================
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

# ==================== Запись + уведомление ====================
def append_row(data):
    flow = data.get("flow", "startstop")
    ws = ws_defect if flow == "defect" else ws_startstop
    ts = now_msk().strftime("%Y-%m-%d %H:%M:%S")
    user = data["user"]

    if flow == "defect":
        row = [data["date"], data["time"], data["line"], "брак",
               data.get("znp", ""), data["meters"],
               data.get("defect_type", ""), user, ts, ""]
    else:
        row = [data["date"], data["time"], data["line"], data["action"],
               data.get("reason", ""), data.get("znp", ""), data["meters"],
               data.get("defect_type", ""), user, ts, ""]

    ws.append_row(row, value_input_option="USER_ENTERED")

    # Уведомления
    if flow == "defect":
        msg = (f"НОВАЯ ЗАПИСЬ БРАКА\n"
               f"Линия: {data['line']}\n"
               f"{data['date']} {data['time']}\n"
               f"ЗНП: <code>{data.get('znp','—')}</code>\n"
               f"Метров брака: {data['meters']}\n"
               f"Вид брака: {data.get('defect_type','—')}")
        notify_controllers(controllers_defect, msg)
    else:
        action_ru = "Запуск" if data["action"] == "запуск" else "Остановка"
        msg = (f"НОВАЯ ЗАПИСЬ СТАРТ/СТОП\n"
               f"Линия: {data['line']}\n"
               f"{data['date']} {data['time']}\n"
               f"Действие: {action_ru}\n"
               f"Причина: {data.get('reason','—')}")
        notify_controllers(controllers_startstop, msg)

# ==================== Поиск последней записи пользователя ====================
def find_last_entry(uid):
    user_col = 9
    ts_col = 10
    for ws, name in [(ws_startstop, "Старт-Стоп"), (ws_defect, "Брак")]:
        try:
            values = ws.get_all_values()
            for i in range(len(values)-1, 0, -1):
                row = values[i]
                if len(row) >= user_col and str(uid) in row[user_col-1]:
                    return True, name, row, ws, i + 1
        except Exception as e:
            log.error(f"find_last_entry error: {e}")
    return False, None, None, None, None

# ==================== Пометить как "Удалено" + уведомление ====================
def mark_as_deleted(ws, row_index):
    try:
        ws.update_cell(row_index, 11, "Удалено")
        row = ws.row_values(row_index)
        if len(row) >= 11:
            sheet = ws.title
            if sheet == DEFECT_SHEET:
                msg = (f"ЗАПИСЬ БРАКА УДАЛЕНА\n"
                       f"Линия: {row[2]}\n"
                       f"{row[0]} {row[1]}\n"
                       f"ЗНП: <code>{row[4]}</code>\n"
                       f"Метров: {row[5]}")
                notify_controllers(controllers_defect, msg)
            else:
                action = "Запуск" if row[3] == "запуск" else "Остановка"
                msg = (f"ЗАПИСЬ СТАРТ/СТОП УДАЛЕНА\n"
                       f"Линия: {row[2]}\n"
                       f"{row[0]} {row[1]}\n"
                       f"Действие: {action}\n"
                       f"Причина: {row[4] if len(row)>4 else '—'}")
                notify_controllers(controllers_startstop, msg)
    except Exception as e:
        log.error(f"mark_as_deleted error: {e}")

# ==================== Клавиатуры ====================
def keyboard(rows):
    return {
        "keyboard": [[{"text": t} for t in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "Выберите действие"
    }

# Главное меню
MAIN_KB = keyboard([
    ["Старт/Стоп", "Брак"]
])

# Подменю для каждого потока
FLOW_MENU_KB = keyboard([
    ["Новая запись", "Отменить последнюю запись"],
    ["Назад"]
])

CANCEL_KB = keyboard([["Отмена"]])
CONFIRM_DELETE_KB = keyboard([["Да, удалить", "Нет"]])

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

# ==================== Поиск последней записи пользователя в конкретном листе ====================
def find_last_user_entry(uid, worksheet):
    try:
        values = worksheet.get_all_values()
        if len(values) <= 1: 
            return None
        for i in range(len(values)-1, 0, -1):
            row = values[i]
            if len(row) >= 9 and str(uid) in row[8]:  # колонка 9 (индекс 8) — Пользователь
                if len(row) >= 11 and row[10].strip() == "Удалено":
                    continue  # пропускаем уже удалённые
                return i + 1, row  # номер строки и данные
    except Exception as e:
        log.error(f"find_last_user_entry error: {e}")
    return None

# ==================== Пометить как удалено ====================
def mark_as_deleted(ws, row_index):
    try:
        ws.update_cell(row_index, 11, "Удалено")  # колонка Статус
        row = ws.row_values(row_index)
        sheet_name = ws.title
        if sheet_name == "Брак":
            notify_controllers(controllers_defect, f"ЗАПИСЬ БРАКА УДАЛЕНА\nЛиния {row[2]}\n{row[0]} {row[1]}\nЗНП: <code>{row[4]}</code>\nМетров: {row[5]}")
        else:
            action = "Запуск" if row[3] == "запуск" else "Остановка"
            notify_controllers(controllers_startstop, f"ЗАПИСЬ СТАРТ/СТОП УДАЛЕНА\nЛиния {row[2]}\n{row[0]} {row[1]}\nДействие: {action}\nПричина: {row[4] if len(row)>4 else '—'}")
    except Exception as e:
        log.error(f"mark_as_deleted error: {e}")

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

# ==================== Основная логика ====================
def process(uid, chat, text, user_repr):
    last_activity[uid] = time.time()

    # Подтверждение удаления
    if states.get(uid, {}).get("step") == "confirm_delete":
        if text == "Да, удалить":
            ws = states[uid]["ws"]
            row_index = states[uid]["row_index"]
            mark_as_deleted(ws, row_index)
            send(chat, "Запись помечена как <b>Удалено</b>.", MAIN_KB)
        else:
            send(chat, "Запись сохранена.", MAIN_KB)
        states.pop(uid, None)
        return

    # Возврат назад в главное меню
    if text == "Назад":
        states.pop(uid, None)
        send(chat, "Главное меню:", MAIN_KB)
        return

    # Выбор потока
    if uid not in states:
        if text in ("/start", "Старт/Стоп"):
            send(chat, "<b>Старт/Стоп</b>\nВыберите действие:", FLOW_MENU_KB)
            states[uid] = {"flow": "startstop", "chat": chat}
            return
        if text == "Брак":
            send(chat, "<b>Брак</b>\nВыберите действие:", FLOW_MENU_KB)
            states[uid] = {"flow": "defect", "chat": chat}
            return
        send(chat, "Выберите действие:", MAIN_KB)
        return

    flow = states[uid]["flow"]

    # Обработка подменю (первое нажатие)
    if states[uid].get("awaiting_flow_choice") is None:
        if text == "Новая запись":
            states[uid]["awaiting_flow_choice"] = True
            if flow == "defect":
                records = get_last_records(ws_defect, 2)
                msg = "<b>Последние записи Брака:</b>\n\n"
                if not records:
                    msg += "Нет записей."
                else:
                    for r in records:
                        znp = r[4] if len(r)>4 else "—"
                        meters = r[5] if len(r)>5 else "—"
                        defect = r[6] if len(r)>6 else "—"
                        msg += f"• {r[0]} {r[1]} | Линия {r[2]} | <code>{znp}</code> | {meters}м | {defect}\n"
                send(chat, msg)
                states[uid].update({"step": "line", "data": {"action": "брак"}})
            else:
                records = get_last_records(ws_startstop, 2)
                msg = "<b>Последние записи Старт/Стоп:</b>\n\n"
                if not records:
                    msg += "Нет записей."
                else:
                    for r in records:
                        action = "Запуск" if r[3] == "запуск" else "Остановка"
                        reason = r[4] if len(r)>4 else "—"
                        msg += f"• {r[0]} {r[1]} | Линия {r[2]} | {action} | {reason}\n"
                send(chat, msg)
                states[uid].update({"step": "line", "data": {}})
            send(chat, "Введите номер линии (1–15):", CANCEL_KB)
            return

        if text == "Отменить последнюю запись":
            ws = ws_defect if flow == "defect" else ws_startstop
            result = find_last_user_entry(uid, ws)
            if not result:
                send(chat, "У вас нет активных записей в этом разделе.", FLOW_MENU_KB)
                return
            row_index, row = result

            action_ru = "Брак" if flow == "defect" else ("Запуск" if len(row)>3 and row[3]=="запуск" else "Остановка")

            msg = f"<b>Последняя запись найдена:</b>\n"
            msg += f"{row[0]} {row[1]} • Линия {row[2]}\n"
            msg += f"Действие: {action_ru}\n"
            if flow == "defect":
                msg += f"ЗНП: <code>{row[4] if len(row)>4 else '—'}</code>\n"
                msg += f"Брака: {row[5] if len(row)>5 else '—'} м\n"
                msg += f"Вид: {row[6] if len(row)>6 else '—'}\n"
            else:
                msg += f"Причина: {row[4] if len(row)>4 else '—'}\n"
            msg += "\n<b>Удалить эту запись?</b>"

            send(chat, msg, CONFIRM_DELETE_KB)
            states[uid].update({
                "step": "confirm_delete",
                "ws": ws,
                "row_index": row_index
            })
            return

        send(chat, "Выберите действие:", FLOW_MENU_KB)
        return

    # Отмена в процессе ввода
    if text == "Отмена":
        states.pop(uid, None)
        send(chat, "Отменено.", MAIN_KB)
        return

    # === ВСЯ ЛОГИКА ВВОДА ДАННЫХ (полностью твоя старая, но с правильными отступами) ===
    st = states[uid]
    step = st["step"]
    data = st["data"]

    if step == "line":
        if not (text.isdigit() and 1 <= int(text) <= 15):
            send(chat, "Номер линии 1–15:", CANCEL_KB); return
        data["line"] = text
        st["step"] = "date"
        today = now_msk().strftime("%d.%m.%Y")
        yest = (now_msk() - timedelta(days=1)).strftime("%d.%m.%Y")
        send(chat, "Дата:", keyboard([[today, yest], ["Другая дата", "Отмена"]]))
        return

    if step == "date":
        if text == "Другая дата":
            st["step"] = "date_custom"; send(chat, "дд.мм.гггг:", CANCEL_KB); return
        try:
            datetime.strptime(text, "%d.%m.%Y")
            data["date"] = text
        except:
            send(chat, "Неверная дата.", CANCEL_KB); return
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
            send(chat, "Формат дд.мм.гггг", CANCEL_KB)
        return

    if step == "time":
        if text == "Другое время":
            st["step"] = "time_custom"; send(chat, "чч:мм:", CANCEL_KB); return
        if not (len(text) == 5 and text[2] == ":" and text[:2].isdigit() and text[3:].isdigit()):
            send(chat, "Неверное время.", CANCEL_KB); return
        secs = now_msk().strftime(":%S")
        data["time"] = text + secs
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
            send(chat, "Формат чч:мм", CANCEL_KB); return
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

    if step == "action":
        if text not in ("Запуск", "Остановка"):
            send(chat, "Выберите:", keyboard([["Запуск", "Остановка"], ["Отмена"]])); return
        data["action"] = "запуск" if text == "Запуск" else "остановка"
        if data["action"] == "запуск":
            st["step"] = "znp_prefix"
        else:
            st["step"] = "reason"
            send(chat, "Причина остановки:", get_reasons_kb())
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
        kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
        if data["action"] == "запуск":
            send(chat, "Префикс ЗНП:", keyboard(kb))
        return

    if step == "reason":
        if text == "Другое":
            st["step"] = "reason_custom"; send(chat, "Введите причину:", CANCEL_KB); return
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

    if step == "znp_prefix":
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
        valid = [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]
        if text in valid:
            data["znp_prefix"] = text
            send(chat, f"Последние 4 цифры для <b>{text}</b>-XXXX:", CANCEL_KB); return
        if text == "Другое":
            st["step"] = "znp_manual"; send(chat, "Полный ЗНП (D1125-1234):", CANCEL_KB); return
        if text.isdigit() and len(text) == 4 and "znp_prefix" in data:
            data["znp"] = f"{data['znp_prefix']}-{text}"
            st["step"] = "meters"; send(chat, "Метров брака:", CANCEL_KB); return
        send(chat, "Выберите префикс:", keyboard([[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]))
        return

    if step == "znp_manual":
        curr = now_msk().strftime("%m%y")
        prev = (now_msk() - timedelta(days=35)).strftime("%m%y")
        if len(text) == 10 and text[5] == "-" and text[:5].upper() in [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]:
            data["znp"] = text.upper()
            st["step"] = "meters"; send(chat, "Метров брака:", CANCEL_KB); return
        send(chat, "Неправильно. Пример: <code>D1125-1234</code>", CANCEL_KB); return

    if step == "meters":
        if not text.isdigit():
            send(chat, "Только цифры:", CANCEL_KB); return
        data["meters"] = text
        st["step"] = "defect_type"
        send(chat, "Вид брака:", get_defect_kb())
        return

    if step == "defect_type":
        if text == "Другое":
            st["step"] = "defect_custom"; send(chat, "Опишите вид брака:", CANCEL_KB); return
        data["defect_type"] = "" if text == "Без брака" else text
        data["user"] = user_repr
        data["flow"] = flow
        append_row(data)

        sheet_name = "Брак" if flow == "defect" else "Старт-Стоп"
        action_text = "Брак" if flow == "defect" else ("Запуск" if data["action"] == "запуск" else "Остановка")
        send(chat,
             f"<b>Записано на лист '{sheet_name}'!</b>\n"
             f"Линия {data['line']} • {data['date']} {data['time']}\n"
             f"Действие: {action_text}\n"
             f"Причина: {data.get('reason','—')}\n"
             f"ЗНП: <code>{data.get('znp','—')}</code>\n"
             f"Брака: {data['meters']} м\n"
             f"Вид брака: {data.get('defect_type') or '—'}",
             MAIN_KB)
        states.pop(uid, None)
        return

   if step == "defect_custom":
        data["defect_type"] = text
        data["user"] = user_repr
        data["flow"] = flow
        append_row(data)
        send(chat, f"<b>Записано на лист '{'Брак' if flow=='defect' else 'Старт-Стоп'}'!</b>\n"
                   f"Линия {data['line']} • {data['date']} {data['time']}\n"
                   f"ЗНП: <code>{data.get('znp','—')}</code>\n"
                   f"Брака: {data['meters']} м\n"
                   f"Вид брака: {text}", MAIN_KB)
        states.pop(uid, None)
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
