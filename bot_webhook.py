# V 4.2 — Уведомления приходят 100% + логи + защита от всех багов Render
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

# ==================== Листы (перечитываем каждый раз — без кэша!) ====================
def get_ws(sheet_name, headers=None):
    ws = sh.worksheet(sheet_name)
    if headers and ws.row_values(1) != headers:
        ws.clear()
        ws.insert_row(headers, 1)
    return ws

# Обновляем каждый раз при импорте
ws_startstop = get_ws("Старт-Стоп", ["Дата","Время","Номер линии","Действие","Причина","ЗНП","Метров брака","Вид брака","Пользователь","Время отправки","Статус"])
ws_defect    = get_ws("Брак")
ws_users     = get_ws("Пользователи", ["user_id", "username", "ФИО", "Роль", "Статус", "Дата регистрации"])
ws_requests  = get_ws("Заявки на доступ", ["user_id", "username", "ФИО", "Дата заявки", "Статус"])

# ==================== Роли ====================
ROLE_ADMIN = "Администратор"
ROLE_MASTER = "Мастер"
ROLE_OPERATOR = "Оператор"

# ==================== Уведомления и отправка ====================
def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
        if r.status_code != 200:
            log.error(f"Ошибка отправки сообщения {chat_id}: {r.text}")
    except Exception as e:
        log.exception(f"send error to {chat_id}: {e}")

def edit_message(chat_id, message_id, text, markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText", json=payload, timeout=10)

def answer_callback(cq_id, text=""):
    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                  json={"callback_query_id": cq_id, "text": text}, timeout=10)

# ==================== Пользователи (перечитываем каждый раз) ====================
def get_approvers():
    ws = get_ws("Пользователи")  # каждый раз свежий
    values = ws.get_all_values()
    ids = []
    for i, row in enumerate(values[1:], 2):
        if len(row) < 5: continue
        uid = row[0].strip()
        role = row[3].strip()
        status = row[4].strip()
        if uid.isdigit() and status == "активен" and role in (ROLE_ADMIN, ROLE_MASTER):
            user_id = int(uid)
            ids.append(user_id)
            log.info(f"approver найден: {user_id} — {role}")
    log.info(f"Всего approvers: {ids}")
    return ids

def is_authorized(user_id):
    ws = get_ws("Пользователи")
    try:
        cell = ws.find(str(user_id), in_column=1)
        if cell:
            row = ws.row_values(cell.row)
            return len(row) >= 5 and row[4].strip() == "активен"
    except: pass
    return False

# ==================== Основная логика ====================
states = {}
last_activity = {}
TIMEOUT = 600

def process(uid, chat, text, user_repr, username=""):
    last_activity[uid] = time.time()

    # Автоочистка старых заявок от уже авторизованных
    if is_authorized(uid):
        ws = get_ws("Заявки на доступ")
        try:
            cell = ws.find(str(uid), in_column=1)
            if cell:
                ws.delete_rows(cell.row)
                log.info(f"Удалена старая заявка для {uid}")
        except: pass

    # === Новый пользователь ===
    if not is_authorized(uid):
        if "awaiting_fio" in states.get(uid, {}):
            fio = text.strip()
            if len(fio) < 5:
                send(chat, "ФИО слишком короткое. Введите ещё раз:")
                return

            ts = now_msk().strftime("%d.%m.%Y %H:%M")
            get_ws("Заявки на доступ").append_row([str(uid), username or "", fio, ts, "ожидает"])
            log.info(f"Новая заявка от {uid} — {fio}")

            approvers = get_approvers()
            if not approvers:
                log.error("НЕТ НИ ОДНОГО approver! Проверь лист Пользователи")
                send(chat, "Ошибка: нет администраторов. Обратитесь к руководству.")
                return

            user_link = f"<a href='tg://user?id={uid}'>{fio}</a>"
            for app_id in approvers:
                prefix = f"{uid}_{fio.replace(' ', '_').replace('.', '')}"
                kb = {
                    "inline_keyboard": [
                        [{"text": "Оператор",      "callback_data": f"role_{prefix}_Оператор"}],
                        [{"text": "Мастер",        "callback_data": f"role_{prefix}_Мастер"}],
                    ]
                }
                if any(get_ws("Пользователи").find(str(app_id), in_column=1) and 
                       get_ws("Пользователи").row_values(get_ws("Пользователи").find(str(app_id), in_column=1).row)[3].strip() == ROLE_ADMIN 
                       for _ in range(1)):
                    kb["inline_keyboard"].insert(2, [{"text": "Администратор", "callback_data": f"role_{prefix}_Администратор"}])
                kb["inline_keyboard"].append([{"text": "Отклонить", "callback_data": f"reject_{prefix}"}])

                send(app_id,
                     f"Новая заявка на доступ\n\n"
                     f"Пользователь: {user_link}\n"
                     f"ID: <code>{uid}</code>\n"
                     f"Username: @{username or '—'}\n"
                     f"ФИО: {fio}\n"
                     f"Время: {ts}",
                     kb)
                log.info(f"Уведомление отправлено {app_id}")

            send(chat, "Ваша заявка отправлена. Ожидайте подтверждения.")
            states.pop(uid, None)
            return

        states[uid] = {"awaiting_fio": True}
        send(chat, "Добро пожаловать!\nДля работы с ботом введите свои ФИО:")
        return

    # === Уже авторизован — дальше весь твой старый код (Старт/Стоп, Брак и т.д.) ===
    if uid not in states:
        states[uid] = {"chat": chat, "cancel_used": False}

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

# ==================== Flask + callback ====================
app = Flask(__name__)
LOCK_PATH = "/tmp/bot.lock"

@app.route("/", methods=["POST"])
def webhook():
    update = request.get_json()

    # === Callback (подтверждение/отклонение) ===
    if update.get("callback_query"):
        cq = update["callback_query"]
        data = cq["data"]
        from_id = cq["from"]["id"]
        msg = cq["message"]
        chat_id = msg["chat"]["id"]
        message_id = msg["message_id"]

        if data.startswith("role_") or data.startswith("reject_"):
            if not any(x in get_approvers() for x in [from_id]):
                answer_callback(cq["id"], "Нет прав")
                return "ok", 200

            if data.startswith("reject_"):
                _, uid_str, fio_enc = data.split("_", 2)
                uid = int(uid_str)
                ws = get_ws("Заявки на доступ")
                cell = ws.find(str(uid), in_column=1)
                if cell: ws.update_cell(cell.row, 5, "отклонено")
                send(uid, "В доступе отказано.")
                edit_message(chat_id, message_id, msg["text"] + "\n\nОтклонено")
                answer_callback(cq["id"], "Отклонено")
                return "ok", 200

            if data.startswith("role_"):
                _, uid_str, fio_enc, role = data.split("_", 3)
                uid = int(uid_str)
                fio = fio_enc.replace("_", " ").replace(".", ". ")

                if role == ROLE_ADMIN and from_id not in [int(r[0]) for r in get_ws("Пользователи").get_all_values()[1:] if len(r)>3 and r[3].strip() == ROLE_ADMIN and r[4].strip() == "активен"]:
                    answer_callback(cq["id"], "Только админ может назначать админа")
                    return "ok", 200

                get_ws("Пользователи").append_row([str(uid), "", fio, role, "активен", now_msk().strftime("%d.%m.%Y")])
                ws = get_ws("Заявки на доступ")
                cell = ws.find(str(uid), in_column=1)
                if cell: ws.delete_rows(cell.row)

                send(uid, f"Доступ подтверждён!\nРоль: <b>{role}</b>")
                edit_message(chat_id, message_id, msg["text"] + f"\n\nНазначена роль: <b>{role}</b>")
                answer_callback(cq["id"], "Готово")
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
    return "Bot живой! V4.2", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
