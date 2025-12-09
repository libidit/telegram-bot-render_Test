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

# ==================== ENV & GSPREAD INIT ====================
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

# ==================== Листы и Заголовки ====================
USERS_SHEET = "Пользователи"
PRODUCTION_RF_SHEET = "Продукция РФ"
PRODUCTION_PPI_SHEET = "Продукция ППИ"
OUTPUT_RF_SHEET = "Выпуск РФ"
OUTPUT_PPI_PPI = "Выпуск ППИ"

USERS_HEADERS = [
    "TelegramID", "ФИО", "Роль", "Статус",
    "Запросил у", "Дата создания",
    "Подтвердил", "Дата подтверждения"
]
OUTPUT_HEADERS = [
    "Дата", "Смена", "Продукция", "Количество",
    "Пользователь", "Время отправки", "Статус"
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

# Инициализация рабочих листов
ws_users = get_ws(USERS_SHEET, USERS_HEADERS)
ws_output_rf = get_ws(OUTPUT_RF_SHEET, OUTPUT_HEADERS)
ws_output_ppi = get_ws(OUTPUT_PPI_PPI, OUTPUT_HEADERS)
ws_prod_rf = get_ws(PRODUCTION_RF_SHEET, ["Код", "Название"])
ws_prod_ppi = get_ws(PRODUCTION_PPI_SHEET, ["Код", "Название"])

# Загрузка справочников продукции
def load_products(ws):
    try:
        # Предполагаем, что столбец 1 - Код, столбец 2 - Название
        values = ws.get_all_values()
        # Возвращаем {Название: Код} для удобства поиска
        return {row[1]: row[0] for row in values[1:] if len(row) >= 2 and row[1]}
    except Exception as e:
        log.error(f"Error loading products from {ws.title}: {e}")
        return {}

products_rf = load_products(ws_prod_rf)
products_ppi = load_products(ws_prod_ppi)

# ==================== Отправка сообщений & KB (Standard/Inline) ====================
def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception(f"send error: {e}")

def answer_callback(callback_id, text=None):
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery", json=payload, timeout=5)
    except:
        pass

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

# Стандартные клавиатуры
def keyboard(rows):
    return {
        "keyboard": [[{"text": t} for t in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }

# Главное меню
MAIN_KB = keyboard([
    ["Ротационное формование", "Полимерно-песчаное производство"],
    ["Отмена"]
])
# Клавиатура Отмены для текстового ввода
CANCEL_KB = keyboard([["Отмена"]])

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
                    send(states[uid]["chat"], "Диалог прерван — неактивность 10 минут.", MAIN_KB)
                except:
                    pass
                states.pop(uid, None)
                last_activity.pop(uid, None)

threading.Thread(target=timeout_worker, daemon=True).start()

# ==================== Авторизация (сохранена) ====================
# (get_user, add_user, find_user_row_index, update_user_status, update_user_role, get_approvers, can_approver_confirm, notify_approvers_new_user, pre_approve_process, confirm_process, reject_process, cancel_confirm_process)
# ... [Функции авторизации остаются без изменений, как в предыдущем ответе] ...
def get_user(uid):
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
    try:
        ws_users.append_row([
            str(uid), fio, "operator", "ожидает", requested_by or "",
            now_msk().strftime("%Y-%m-%d %H:%M:%S"), "", ""
        ], value_input_option="USER_ENTERED")
    except Exception as e:
        log.exception("add_user error: %s", e)

def find_user_row_index(uid):
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

def can_approver_confirm(approver_role, target_role):
    if approver_role == "admin":
        return True
    if approver_role == "master":
        return target_role in ("operator", "master")
    return False

def build_role_selection_kb(target_uid, approver_role):
    buttons = []
    buttons.append({"text": "Оператор (operator)", "callback_data": f"confirm_{target_uid}_operator"})
    buttons.append({"text": "Мастер (master)", "callback_data": f"confirm_{target_uid}_master"})
    if approver_role == "admin":
        buttons.append({"text": "Админ (admin)", "callback_data": f"confirm_{target_uid}_admin"})

    kb_rows = []
    row = []
    for b in buttons:
        row.append(b)
        if len(row) == 2:
            kb_rows.append(row)
            row = []
    if row:
        kb_rows.append(row)
    kb_rows.append([{"text": "Отмена выбора роли", "callback_data": f"cancel_confirm_{target_uid}"}])
    return {"inline_keyboard": kb_rows}

def ask_role_selection(approver_uid, target_uid, fio, approver_role, message_id):
    kb = build_role_selection_kb(target_uid, approver_role)
    msg = (f"Вы подтверждаете пользователя:\nФИО: {fio}\nTelegramID: {target_uid}\n\n"
           f"**Выберите роль для пользователя:**")
    edit_message(approver_uid, message_id, msg, kb)

def notify_approvers_new_user(uid, fio):
    approvers = get_approvers()
    if not approvers:
        log.info("No approvers found to notify for new user %s", uid)
        return

    approve_data = f"pre_approve_{uid}"
    reject_data = f"reject_{uid}"
    kb = {"inline_keyboard": [[{"text": "✅ Подтвердить и выбрать роль", "callback_data": approve_data},],
                              [{"text": "❌ Отклонить", "callback_data": reject_data},]]}
    text = (f"Новая заявка на доступ:\nФИО: {fio}\nTelegramID: {uid}\n\n"
            f"Нажмите кнопку для выбора роли или отклонения.")
    for a in approvers:
        try:
            send(a, text, kb)
        except Exception as e:
            log.exception("notify approver error: %s", e)

def pre_approve_process(approver_uid, target_uid, message_id):
    approver = get_user(approver_uid)
    target = get_user(target_uid)
    if not approver or approver["status"] != "подтвержден":
        edit_message(approver_uid, message_id, "Вы не зарегистрированы или не подтверждены — нет прав подтверждать.")
        return False
    if not target:
        edit_message(approver_uid, message_id, f"Пользователь {target_uid} не найден в списке заявок.")
        return False
    if target["status"] != "ожидает":
        edit_message(approver_uid, message_id, f"Заявка пользователя {target['fio']} уже обработана ({target['status']}).")
        return False
    target_role_to_check = target["role"] if target["role"] in ("operator", "master", "admin") else "operator"
    if not can_approver_confirm(approver["role"], target_role_to_check):
        edit_message(approver_uid, message_id, "У вас нет прав подтверждать этого пользователя.")
        return False
    ask_role_selection(approver_uid, target_uid, target["fio"], approver["role"], message_id)
    return True

def confirm_process(approver_uid, target_uid, new_role, message_id):
    approver = get_user(approver_uid)
    target = get_user(target_uid)
    if not approver or approver["status"] != "подтвержден":
        edit_message(approver_uid, message_id, "Вы не можете назначать роли.")
        return False
    if not target:
        edit_message(approver_uid, message_id, "Целевой пользователь не найден.")
        return False
    if target["status"] != "ожидает":
        edit_message(approver_uid, message_id, f"Заявка пользователя {target['fio']} уже обработана ({target['status']}).")
        return False
    if approver["role"] == "master" and new_role == "admin":
        edit_message(approver_uid, message_id, "Мастер не может назначать роль admin.")
        return False
    if not can_approver_confirm(approver["role"], new_role):
        edit_message(approver_uid, message_id, "У вас нет прав назначать такую роль.")
        return False

    update_user_role(target_uid, new_role)
    update_user_status(target_uid, "подтвержден")

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

def reject_process(approver_uid, target_uid, message_id):
    approver = get_user(approver_uid)
    target = get_user(target_uid)
    if not approver or approver["status"] != "подтвержден":
        edit_message(approver_uid, message_id, "У вас нет прав отклонять заявки.")
        return False
    if not target:
        edit_message(approver_uid, message_id, f"Пользователь {target_uid} не найден.")
        return False
    if target["status"] != "ожидает":
        edit_message(approver_uid, message_id, f"Заявка пользователя {target['fio']} уже обработана ({target['status']}).")
        return False
    target_role_to_check = target["role"] if target["role"] in ("operator", "master", "admin") else "operator"
    if not can_approver_confirm(approver["role"], target_role_to_check):
        edit_message(approver_uid, message_id, "У вас нет прав отклонять этого пользователя.")
        return False

    update_user_status(target_uid, "отклонен")

    idx = find_user_row_index(target_uid)
    if idx:
        try:
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

def cancel_confirm_process(approver_uid, target_uid, message_id):
    target = get_user(target_uid)
    if not target or target["status"] != "ожидает":
        edit_message(approver_uid, message_id, "Заявка уже обработана или не найдена.")
        return False

    fio = target["fio"]
    approve_data = f"pre_approve_{target_uid}"
    reject_data = f"reject_{target_uid}"
    kb = {"inline_keyboard": [[{"text": "✅ Подтвердить и выбрать роль", "callback_data": approve_data},],
                              [{"text": "❌ Отклонить", "callback_data": reject_data},]]}
    
    text = (f"Новая заявка на доступ:\nФИО: {fio}\nTelegramID: {target_uid}\n\n"
            f"Выбор роли отменён. Нажмите кнопку для выбора роли или отклонения.")
    edit_message(approver_uid, message_id, text, kb)
    return True
# ===================================================================================

# ==================== Функции производственных потоков ====================

def get_date_keyboard(flow):
    """Клавиатура для выбора даты (Сегодня, Вчера, Другая дата)"""
    today = now_msk().strftime("%Y-%m-%d")
    yesterday = (now_msk() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    return {
        "inline_keyboard": [
            [{"text": "Сегодня", "callback_data": f"prod_{flow}_date_{today}"}],
            [{"text": "Вчера", "callback_data": f"prod_{flow}_date_{yesterday}"}],
            [{"text": "Другая дата...", "callback_data": f"prod_{flow}_date_other"}],
            [{"text": "Отмена", "callback_data": f"prod_{flow}_cancel"}]
        ]
    }

def get_shift_keyboard(flow):
    """Клавиатура для выбора смены (День, Ночь)"""
    return {
        "inline_keyboard": [
            [{"text": "День", "callback_data": f"prod_{flow}_shift_День"}],
            [{"text": "Ночь", "callback_data": f"prod_{flow}_shift_Ночь"}],
            [{"text": "Отмена", "callback_data": f"prod_{flow}_cancel"}]
        ]
    }

def get_product_keyboard(flow):
    """Клавиатура для выбора продукции"""
    products = products_rf if flow == "rf" else products_ppi
    
    kb_rows = []
    # Сортируем названия для удобства
    sorted_names = sorted(products.keys()) 
    
    row = []
    for name in sorted_names:
        # data: prod_<flow>_product_<name>
        row.append({"text": name, "callback_data": f"prod_{flow}_product_{name}"})
        if len(row) == 2:
            kb_rows.append(row)
            row = []
    if row:
        kb_rows.append(row)
        
    kb_rows.append([{"text": "Отмена", "callback_data": f"prod_{flow}_cancel"}])
    return {"inline_keyboard": kb_rows}

def append_production_data(flow, data):
    """Записывает данные в соответствующий лист Google Sheets"""
    ws = ws_output_rf if flow == "rf" else ws_output_ppi
    products = products_rf if flow == "rf" else products_ppi
    
    product_code = products.get(data['product'], data['product']) # Использовать код или название
    
    user_data = get_user(data['uid'])
    user_fio = user_data.get('fio', str(data['uid']))
    
    try:
        ws.append_row([
            data['date'],
            data['shift'],
            product_code, # В таблице записываем код
            data['quantity'],
            user_fio,
            now_msk().strftime("%Y-%m-%d %H:%M:%S"),
            "Записано"
        ], value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        log.exception(f"Error appending production data for flow {flow}: {e}")
        return False


# ==================== Обработка шагов производственного потока ====================

def start_production_flow(uid, flow, chat_id, message_id=None):
    """Начало или перезапуск потока"""
    states[uid] = {"chat": chat_id, "flow": flow, "step": "date", "data": {"uid": uid}}
    
    flow_name = "Ротационное формование" if flow == "rf" else "Полимерно-песчаное производство"
    msg = f"Вы выбрали: **{flow_name}**.\n\nВыберите дату производства:"
    kb = get_date_keyboard(flow)
    
    if message_id:
        edit_message(chat_id, message_id, msg, kb)
    else:
        send(chat_id, msg, kb)
        
def process_date_selection(uid, flow, value, chat_id, message_id):
    """Обработка выбора даты"""
    state = states[uid]
    state['data']['date'] = value
    
    if value == 'other':
        # Переход к текстовому вводу даты
        state['step'] = 'date_input'
        msg = "Введите дату в формате **ГГГГ-ММ-ДД** (например, 2025-12-09):"
        edit_message(chat_id, message_id, msg) # Удаляем inline кнопки
        send(chat_id, msg, CANCEL_KB) # Добавляем Отмена
    else:
        # Переход к выбору смены
        state['step'] = 'shift'
        msg = f"Дата: **{value}**.\n\nВыберите смену:"
        kb = get_shift_keyboard(flow)
        edit_message(chat_id, message_id, msg, kb)

def process_shift_selection(uid, flow, value, chat_id, message_id):
    """Обработка выбора смены"""
    state = states[uid]
    state['data']['shift'] = value
    
    # Переход к выбору продукции
    state['step'] = 'product'
    msg = f"Дата: {state['data']['date']}, Смена: **{value}**.\n\nВыберите продукцию:"
    kb = get_product_keyboard(flow)
    edit_message(chat_id, message_id, msg, kb)

def process_product_selection(uid, flow, value, chat_id, message_id):
    """Обработка выбора продукции"""
    state = states[uid]
    state['data']['product'] = value
    
    # Переход к вводу количества
    state['step'] = 'quantity_input'
    msg = (f"Дата: {state['data']['date']}, Смена: {state['data']['shift']}.\n"
           f"Продукция: **{value}**.\n\n"
           f"Введите количество (только целое число):")
    edit_message(chat_id, message_id, msg) # Удаляем inline кнопки
    send(chat_id, msg, CANCEL_KB) # Добавляем Отмена
    
def display_buffered_data(uid, chat_id, message_id):
    """Отображает буферизированные данные и кнопки записи/отмены"""
    state = states[uid]
    flow = state['flow']
    data = state['data']
    
    # После ввода количества, переходим в состояние 'confirm'
    state['step'] = 'confirm'
    
    flow_name = "Ротационное формование" if flow == "rf" else "Полимерно-песчаное производство"
    
    msg = (f"**ПРОВЕРКА ДАННЫХ ({flow_name}):**\n"
           f"**Дата:** {data.get('date')}\n"
           f"**Смена:** {data.get('shift')}\n"
           f"**Продукция:** {data.get('product')}\n"
           f"**Количество:** {data.get('quantity')}\n\n"
           f"Нажмите 'Записать данные' для сохранения или 'Отмена записи' для сброса.")
           
    kb = {
        "inline_keyboard": [
            [{"text": "✅ Записать данные", "callback_data": f"prod_{flow}_write"}],
            [{"text": "❌ Отмена записи", "callback_data": f"prod_{flow}_cancel"}]
        ]
    }
    
    # Редактируем последнее сообщение, если есть, или отправляем новое
    if message_id:
        edit_message(chat_id, message_id, msg, kb)
    else:
        send(chat_id, msg, kb)
    
def process_write_data(uid, flow, chat_id, message_id):
    """Финальная запись данных и завершение потока"""
    state = states[uid]
    data = state['data']
    
    if append_production_data(flow, data):
        msg = f"✅ **Данные по выпуску {flow.upper()} записаны успешно!**"
    else:
        msg = f"❌ **Ошибка записи данных {flow.upper()}!** Попробуйте снова или обратитесь к администратору."
        
    edit_message(chat_id, message_id, msg)
    states.pop(uid, None)
    send(chat_id, "Выберите следующее действие:", MAIN_KB)

# ==================== Главный обработчик сообщений ====================

def process_message(uid, chat, text):
    last_activity[uid] = time.time()
    
    if uid not in states:
        states[uid] = {"chat": chat}

    state = states[uid]
    u = get_user(uid)
    
    # 1. Логика авторизации
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
            states[uid]["fio_wait"] = True
            send(chat, "Вы не зарегистрированы. Введите ваше ФИО:", CANCEL_KB)
            return
    
    if u["status"] != "подтвержден":
        send(chat, "Ваш доступ пока не подтверждён администратором.")
        return

    # 2. Обработка команд /start и Отмена
    if text == "/start":
        states.pop(uid, None)
        send(chat, "Добро пожаловать. Выберите действие:", MAIN_KB)
        return
        
    if text == "Отмена":
        if state.get('flow'):
             send(chat, f"Запись данных {state['flow'].upper()} отменена.", MAIN_KB)
        else:
             send(chat, "Отменено.", MAIN_KB)
        states.pop(uid, None)
        return
        
    # 3. Обработка выбора производственного потока (только если не в процессе)
    if text == "Ротационное формование":
        start_production_flow(uid, "rf", chat)
        return
        
    if text == "Полимерно-песчаное производство":
        start_production_flow(uid, "ppi", chat)
        return

    # 4. Обработка текстового ввода (Дата или Количество)
    if state.get('flow') and state.get('step') in ('date_input', 'quantity_input'):
        flow = state['flow']
        
        # Ввод даты
        if state['step'] == 'date_input':
            try:
                # Простая проверка формата
                datetime.strptime(text, "%Y-%m-%d") 
                process_date_selection(uid, flow, text, chat, None)
            except ValueError:
                send(chat, "Неверный формат даты. Пожалуйста, введите в формате **ГГГГ-ММ-ДД**:", CANCEL_KB)
            return
            
        # Ввод количества
        if state['step'] == 'quantity_input':
            try:
                quantity = int(text)
                if quantity <= 0:
                    raise ValueError
                state['data']['quantity'] = quantity
                display_buffered_data(uid, chat, None)
            except ValueError:
                send(chat, "Неверное количество. Введите **целое положительное число**:", CANCEL_KB)
            return

    # 5. Если подтвержден, но ввел не команду и не текст, нужный для шага
    send(chat, "Выберите действие:", MAIN_KB)


# ==================== Главный обработчик Inline-кнопок ====================

def process_callback_query(callback_query):
    uid = callback_query["from"]["id"]
    data = callback_query["data"]
    message = callback_query["message"]
    chat_id = message["chat"]["id"]
    message_id = message["message_id"]

    answer_callback(callback_query["id"])

    # 1. Логика авторизации
    if data.startswith(("pre_approve_", "reject_", "confirm_", "cancel_confirm_")):
        if data.startswith("pre_approve_"):
            target_uid = data.split("_")[-1]
            pre_approve_process(uid, target_uid, message_id)
        elif data.startswith("reject_"):
            target_uid = data.split("_")[-1]
            reject_process(uid, target_uid, message_id)
        elif data.startswith("confirm_"):
            parts = data.split("_")
            target_uid = parts[1]
            new_role = parts[2]
            confirm_process(uid, target_uid, new_role, message_id)
        elif data.startswith("cancel_confirm_"):
            target_uid = data.split("_")[-1]
            cancel_confirm_process(uid, target_uid, message_id)
        return

    # 2. Логика производственных потоков
    if data.startswith("prod_"):
        try:
            # prod_<flow>_<step>_<value>
            parts = data.split("_", 3)
            flow = parts[1]
            step = parts[2]
            value = parts[3] if len(parts) > 3 else None
        except IndexError:
            edit_message(chat_id, message_id, "Ошибка: неверный формат данных.")
            return

        state = states.get(uid)
        if not state or state.get('flow') != flow:
            send(chat_id, "Сессия устарела. Начните снова.", MAIN_KB)
            return

        if step == 'cancel':
            edit_message(chat_id, message_id, f"❌ Запись данных {flow.upper()} отменена.", None)
            states.pop(uid, None)
            send(chat_id, "Выберите следующее действие:", MAIN_KB)
            return

        if step == 'date':
            process_date_selection(uid, flow, value, chat_id, message_id)
        elif step == 'shift':
            process_shift_selection(uid, flow, value, chat_id, message_id)
        elif step == 'product':
            process_product_selection(uid, flow, value, chat_id, message_id)
        elif step == 'write' and state.get('step') == 'confirm':
            process_write_data(uid, flow, chat_id, message_id)
        return


# ==================== Flask Webhook ====================
app = Flask(__name__)
LOCK_PATH = "/tmp/bot.lock"

if os.getenv("RENDER"):
    # (Webhook setup is skipped for brevity, assumed to be running environment)
    pass

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
