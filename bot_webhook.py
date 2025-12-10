# bot_webhook.py
# Обновлённая версия под "Выпуск РФ" и "Выпуск ППИ" с мульти-циклом продукции
import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict, Any

from flask import Flask, request
import gspread
from google.oauth2 import service_account
from filelock import FileLock
import requests

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ========== ENV ==========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

if not all([TELEGRAM_TOKEN, SPREADSHEET_ID, GOOGLE_CREDS_JSON]):
    raise RuntimeError("Missing required env vars")

# ========== Google Sheets init ==========
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = service_account.Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

# ========== Time helpers ==========
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK)


def now_msk_str() -> str:
    return now_msk().strftime("%Y-%m-%d %H:%M:%S")


# ========== Sheet names & headers ==========
# New production sheets
RF_SHEET = "Выпуск РФ"       # Ротационное формование
PPI_SHEET = "Выпуск ППИ"     # Полимерно-песчаное производство

# Controllers lists (who should be notified for each flow)
CTRL_RF_SHEET = "Контр_Выпуск_РФ"
CTRL_PPI_SHEET = "Контр_Выпуск_ППИ"
USERS_SHEET = "Пользователи"

# Headers for the new sheets:
# Date | Shift | Product | Quantity | User | TS | Status
PROD_HEADERS = ["Дата", "Смена", "Продукция", "Количество", "Пользователь", "Время отправки", "Статус"]
USERS_HEADERS = ["TelegramID", "ФИО", "Роль", "Статус", "Запросил у", "Дата создания", "Подтвердил", "Дата подтверждения"]

# ========== Telegram send wrapper ==========
def tg_send(chat_id: int, text: str, markup: Optional[dict] = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception("tg_send error: %s", e)


# ========== SheetClient — encapsulate sheet ops + caching ==========
class SheetClient:
    def __init__(self, sh_obj, cache_ttl: int = 5):
        self.sh = sh_obj
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        # ensure sheets & headers
        self._ensure_sheet(RF_SHEET, PROD_HEADERS)
        self._ensure_sheet(PPI_SHEET, PROD_HEADERS)
        self._ensure_sheet(USERS_SHEET, USERS_HEADERS)
        # ensure controllers sheets exist (no headers required)
        self._ensure_sheet(CTRL_RF_SHEET, None)
        self._ensure_sheet(CTRL_PPI_SHEET, None)

    def _ensure_sheet(self, title: str, headers: Optional[List[str]]):
        try:
            ws = self.sh.worksheet(title)
        except gspread.exceptions.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=title, rows=3000, cols=20)
        if headers:
            current = ws.row_values(1)
            if current != headers:
                ws.clear()
                ws.insert_row(headers, 1)

    def _ws(self, title: str):
        return self.sh.worksheet(title)

    def _get_all_values_cached(self, title: str) -> List[List[str]]:
        now_ts = time.time()
        c = self._cache.get(title)
        if c and c.get("until", 0) > now_ts:
            return c["data"]
        try:
            data = self._ws(title).get_all_values()
        except Exception as e:
            log.exception("Error reading sheet %s: %s", title, e)
            data = []
        self._cache[title] = {"data": data, "until": now_ts + self.cache_ttl}
        return data

    def invalidate_cache(self, title: Optional[str] = None):
        if title:
            self._cache.pop(title, None)
        else:
            self._cache.clear()

    # Users operations
    def get_users_rows(self) -> List[List[str]]:
        return self._get_all_values_cached(USERS_SHEET)

    def find_user(self, uid: int) -> Optional[Dict[str, str]]:
        rows = self.get_users_rows()
        for row in rows[1:]:
            if row and row[0] == str(uid):
                fio = row[1] if len(row) > 1 else ""
                role = row[2] if len(row) > 2 and row[2].strip() else "operator"
                status = row[3] if len(row) > 3 and row[3].strip() else "ожидает"
                return {"id": str(uid), "fio": fio.strip(), "role": role.strip(), "status": status.strip()}
        return None

    def add_user(self, uid: int, fio: str, requested_by: str = ""):
        try:
            self._ws(USERS_SHEET).append_row(
                [str(uid), fio.strip(), "operator", "ожидает", requested_by or "", now_msk_str(), "", ""],
                value_input_option="USER_ENTERED"
            )
            self.invalidate_cache(USERS_SHEET)
        except Exception as e:
            log.exception("add_user error: %s", e)

    def find_user_row_index(self, uid: int) -> Optional[int]:
        rows = self.get_users_rows()
        for idx, row in enumerate(rows[1:], start=2):
            if row and row[0] == str(uid):
                return idx
        return None

    def update_user(self, uid: int, role: Optional[str] = None, status: Optional[str] = None, confirmed_by: Optional[int] = None):
        idx = self.find_user_row_index(uid)
        if not idx:
            return
        try:
            if role:
                self._ws(USERS_SHEET).update(f"C{idx}", [[role]])
            if status:
                self._ws(USERS_SHEET).update(f"D{idx}", [[status]])
            if confirmed_by:
                self._ws(USERS_SHEET).update(f"G{idx}", [[str(confirmed_by)]])
                self._ws(USERS_SHEET).update(f"H{idx}", [[now_msk_str()]])
            self.invalidate_cache(USERS_SHEET)
        except Exception as e:
            log.exception("update_user error: %s", e)

    def get_approvers(self) -> List[int]:
        res = []
        rows = self.get_users_rows()[1:]
        for row in rows:
            try:
                role = (row[2] if len(row) > 2 else "").strip()
                status = (row[3] if len(row) > 3 else "").strip()
                if role in ("admin", "master") and status == "подтвержден":
                    if row[0].strip().isdigit():
                        res.append(int(row[0].strip()))
            except Exception:
                continue
        return res

    # Controllers (plain read)
    def get_controllers(self, title: str) -> List[int]:
        try:
            ws = self._ws(title)
            ids = ws.col_values(1)[1:]
            return [int(i.strip()) for i in ids if i.strip().isdigit()]
        except Exception as e:
            log.exception("get_controllers(%s) error: %s", title, e)
            return []

    # Records
    def append_record(self, sheet_title: str, row: List[Any]):
        try:
            self._ws(sheet_title).append_row(row, value_input_option="USER_ENTERED")
            self.invalidate_cache(sheet_title)
        except Exception as e:
            log.exception("append_record error: %s", e)

    def get_last_records(self, sheet_title: str, n: int = 5) -> List[List[str]]:
        """
        Возвращает последние n АКТИВНЫХ записей (без ОТМЕНЕНО).
        Формат: [Дата, Смена, Продукция, Количество, Пользователь, Время, Статус]
        """
        vals = self._get_all_values_cached(sheet_title)
        if len(vals) <= 1:
            return []

        status_idx = 6  # Статус

        active = []
        # перебираем строки с конца вверх
        for row in reversed(vals[1:]):
            if len(row) > status_idx and row[status_idx].strip().upper() == "ОТМЕНЕНО":
                continue
            active.append(row)
            if len(active) >= n:
                break

        return active

    def update_cell(self, sheet_title: str, cell: str, value: Any):
        try:
            self._ws(sheet_title).update(cell, [[value]])
            self.invalidate_cache(sheet_title)
        except Exception as e:
            log.exception("update_cell error: %s", e)

    def find_last_session_records(self, sheet_title: str, uid: int) -> List[Tuple[List[str], int]]:
        """
        Находит последнюю активную сессию пользователя в листе по TS (Время отправки).
        Возвращает список кортежей (row, row_number) для всех строк, относящихся к этой сессии,
        где пользователь встречается как "(<uid>)" в столбце Пользователь и статус != "ОТМЕНЕНО".
        Формат листа: 0:Дата,1:Смена,2:Продукция,3:Количество,4:Пользователь,5:Время отправки,6:Статус
        """
        vals = self._get_all_values_cached(sheet_title)
        if not vals or len(vals) <= 1:
            return []
        user_idx = 4
        ts_idx = 5
        status_idx = 6

        # найдем индекс последней строки, принадлежащей пользователю и не отмененной
        last_row_index = None
        for i in range(len(vals) - 1, 0, -1):
            row = vals[i]
            if len(row) <= user_idx:
                continue
            try:
                cell_user = row[user_idx]
            except Exception:
                continue
            if f"({uid})" in cell_user:
                status = row[status_idx] if len(row) > status_idx else ""
                if status.strip() != "ОТМЕНЕНО":
                    last_row_index = i
                    break

        if last_row_index is None:
            return []

        # получить TS у этой строки
        last_row = vals[last_row_index]
        ts_value = last_row[ts_idx] if len(last_row) > ts_idx else None
        if not ts_value:
            # если TS нет, вернём только ту последнюю строку
            return [(last_row, last_row_index + 1)]

        # собрать все строки с тем же TS и тем же пользователем (и не отменённые)
        res: List[Tuple[List[str], int]] = []
        for i in range(1, len(vals)):  # пропускаем заголовок
            row = vals[i]
            if len(row) <= max(user_idx, ts_idx):
                continue
            try:
                if f"({uid})" in row[user_idx] and row[ts_idx] == ts_value:
                    status = row[status_idx] if len(row) > status_idx else ""
                    if status.strip() != "ОТМЕНЕНО":
                        res.append((row, i + 1))
            except Exception:
                continue
        return res



# instantiate
sheet_client = SheetClient(sh, cache_ttl=5)

# ========== Keyboards & helpers ==========
def kb_reply(rows: List[List[str]], one_time: bool = False, placeholder: Optional[str] = None, input_field_placeholder: Optional[str] = None) -> dict:
    kb = {"keyboard": [[{"text": t} for t in row] for row in rows],
          "resize_keyboard": True,
          "one_time_keyboard": one_time}
    if input_field_placeholder:
        kb["input_field_placeholder"] = input_field_placeholder
    return kb


MAIN_KB = kb_reply([["Ротационное формование", "Полимерно-песчаное производство"]])
FLOW_MENU_KB = kb_reply([["Новая запись"], ["Отменить последнюю запись"], ["Назад"]])
CANCEL_KB = kb_reply([["Отмена"]])
CONFIRM_KB = kb_reply([["Да, отменить"], ["Нет, оставить"]])

# Numeric input keyboard (opens numeric keypad on phone; user types digits manually)
NUMERIC_INPUT_KB = {
    "keyboard": [
        [{"text": "Отмена"}]
    ],
    "resize_keyboard": True,
    "one_time_keyboard": False,
    "input_field_placeholder": "Введите число"
}

# Product keyboards builder — reads first column of given sheet
def build_product_kb(sheet_name: str, extra: Optional[List[str]] = None) -> dict:
    if extra is None:
        extra = []
    try:
        vals = sheet_client._ws(sheet_name).col_values(1)[1:]
    except Exception:
        vals = []
    items = [v.strip() for v in vals if v and v.strip()] + extra
    # split into two columns per row for nicer layout
    rows = [items[i:i + 2] for i in range(0, len(items), 2)]
    rows.append(["Отмена"])
    return kb_reply(rows, one_time=False)


# Controllers list (cached)
_CONTROLLERS_CACHE: Dict[str, Any] = {"rf": {"until": 0, "data": []}, "ppi": {"until": 0, "data": []}}
def get_controllers_cached(sheet_name: str, ttl: int = 600) -> List[int]:
    key = "rf" if sheet_name == CTRL_RF_SHEET else "ppi"
    now_ts = time.time()
    if _CONTROLLERS_CACHE[key]["until"] > now_ts and _CONTROLLERS_CACHE[key]["data"]:
        return _CONTROLLERS_CACHE[key]["data"]
    try:
        data = sheet_client.get_controllers(sheet_name)
    except Exception:
        data = []
    _CONTROLLERS_CACHE[key]["data"] = data
    _CONTROLLERS_CACHE[key]["until"] = now_ts + ttl
    return data


# ========== AuthManager ==========
class AuthManager:
    def __init__(self, sc: SheetClient):
        self.sc = sc

    def get_user(self, uid: int) -> Optional[Dict[str, str]]:
        return self.sc.find_user(uid)

    def register_user(self, uid: int, fio: str, requested_by: str = ""):
        self.sc.add_user(uid, fio, requested_by)
        self.notify_approvers_new_user(uid, fio)

    def notify_approvers_new_user(self, uid: int, fio: str):
        approvers = self.sc.get_approvers()
        if not approvers:
            log.info("No approvers to notify for new user %s", uid)
            return
        kb = {
            "inline_keyboard": [
                [
                    {"text": "Подтвердить", "callback_data": f"approve_{uid}"},
                    {"text": "Отклонить", "callback_data": f"reject_{uid}"}
                ]
            ]
        }
        text = f"<b>Новая заявка на доступ</b>\nФИО: {fio}\nID: <code>{uid}</code>"
        for a in approvers:
            try:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                              json={"chat_id": a, "text": text, "parse_mode": "HTML", "reply_markup": json.dumps(kb, ensure_ascii=False)},
                              timeout=10)
            except Exception:
                log.exception("notify approver failed for %s", a)

    def process_callback(self, callback: dict):
        data = callback.get("data", "")
        uid = callback["from"]["id"]
        chat_id = callback["message"]["chat"]["id"]
        if data.startswith("approve_") or data.startswith("reject_"):
            target_id = int(data.split("_", 1)[1])
            approver = self.get_user(uid)
            if not approver or approver["status"] != "подтвержден" or approver["role"] not in ("admin", "master"):
                tg_send(chat_id, "У вас нет прав подтверждать/отклонять.")
                return
            target = self.get_user(target_id)
            if not target:
                tg_send(chat_id, "Целевой пользователь не найден.")
                return
            if data.startswith("approve_"):
                roles = ["operator", "master"]
                if approver["role"] == "admin":
                    roles.append("admin")
                kb = {"inline_keyboard": [[{"text": r, "callback_data": f"setrole_{target_id}_{r}"}] for r in roles]}
                tg_send(chat_id, f"Выберите роль для <b>{target['fio']}</b>:", kb)
            else:
                self.sc.update_user(target_id, status="отклонен", confirmed_by=uid)
                tg_send(chat_id, f"Заявка отклонена: {target['fio']}")
                try:
                    tg_send(int(target_id), "В доступе отказано.")
                except Exception:
                    pass
        elif data.startswith("setrole_"):
            parts = data.split("_")
            if len(parts) < 3:
                return
            target_id = int(parts[1])
            role = parts[2]
            approver = self.get_user(uid)
            if not approver or approver["status"] != "подтвержден":
                tg_send(chat_id, "Ваш аккаунт не подтверждён.")
                return
            if approver["role"] == "master" and role == "admin":
                tg_send(chat_id, "Мастер не может назначать роль admin.")
                return
            self.sc.update_user(target_id, role=role, status="подтвержден", confirmed_by=uid)
            target = self.get_user(target_id)
            tg_send(chat_id, f"Пользователь {target['fio']} подтверждён как <b>{role}</b>")
            try:
                tg_send(int(target_id), f"Ваша заявка подтверждена! Ваша роль: {role}\n\nВыберите действие:", MAIN_KB)
            except Exception:
                pass


auth = AuthManager(sheet_client)

# ========== FSM: управление состояниями и диалогами ==========
class FSM:
    def __init__(self, sc: SheetClient, authm: AuthManager):
        self.sc = sc
        self.auth = authm
        self.states: Dict[int, dict] = {}
        self.last_activity: Dict[int, float] = {}
        self.TIMEOUT = 600
        threading.Thread(target=self._timeout_worker, daemon=True).start()

    def _timeout_worker(self):
        while True:
            time.sleep(30)
            now_ts = time.time()
            for uid in list(self.states.keys()):
                if now_ts - self.last_activity.get(uid, now_ts) > self.TIMEOUT:
                    st = self.states.pop(uid, None)
                    if st:
                        try:
                            tg_send(st.get("chat"), "Диалог прерван — неактивность 10 минут.")
                        except Exception:
                            pass
                    self.last_activity.pop(uid, None)

    def touch(self, uid: int):
        self.last_activity[uid] = time.time()

    def ensure_state(self, uid: int, chat: int):
        if uid not in self.states:
            self.states[uid] = {"chat": chat, "cancel_used": False}
        else:
            self.states[uid]["chat"] = chat

    def clear_state(self, uid: int):
        self.states.pop(uid, None)
        self.last_activity.pop(uid, None)

    def handle_text(self, uid: int, chat: int, text: str, user_repr: str):
        self.touch(uid)
        self.ensure_state(uid, chat)
        st = self.states[uid]

        # navigation & cancel
        if text == "Назад":
            self.clear_state(uid)
            tg_send(chat, "Главное меню:", MAIN_KB)
            return
        if text == "Отмена":
            st.pop("pending_cancel", None)
            # keep the state cleared
            self.clear_state(uid)
            tg_send(chat, "Отменено.", MAIN_KB)
            return

        # auth
        user = self.auth.get_user(uid)
        if user is None:
            if st.get("waiting_fio"):
                fio = text.strip()
                if not fio:
                    tg_send(chat, "Введите корректное ФИО:")
                    return
                self.auth.register_user(uid, fio, requested_by="")
                tg_send(chat, "Спасибо! Ваша заявка отправлена на подтверждение.")
                self.clear_state(uid)
                return
            st["waiting_fio"] = True
            tg_send(chat, "Вы не зарегистрированы. Введите ваше ФИО:")
            return

        if user["status"] != "подтвержден":
            tg_send(chat, "Ваш доступ пока не подтверждён администратором.")
            return

        # flow selection
        if "flow" not in st:
            if text in ("/start", "Ротационное формование"):
                st["flow"] = "rf"
                tg_send(chat, "<b>Ротационное формование</b>\nВыберите действие:", FLOW_MENU_KB)
                return
            if text == "Полимерно-песчаное производство":
                st["flow"] = "ppi"
                tg_send(chat, "<b>Полимерно-песчаное производство</b>\nВыберите действие:", FLOW_MENU_KB)
                return
            tg_send(chat, f"Привет, {user['fio'].split()[0]}! Выберите действие:", MAIN_KB)
            return

        flow = st["flow"]

        # cancel last record
        if text == "Отменить последнюю запись":
            if st.get("cancel_used", False):
                tg_send(chat, "Вы уже отменили одну запись в этом сеансе. Сделайте новую запись, чтобы снова отменить.", FLOW_MENU_KB)
                return

            sheet = RF_SHEET if flow == "rf" else PPI_SHEET
            session_rows = self.sc.find_last_session_records(sheet, uid)
            if not session_rows:
                tg_send(chat, "У вас нет активных записей для отмены.", FLOW_MENU_KB)
                return

            # сохраняем все строки сессии в pending_cancel
            st["pending_cancel"] = {"ws": sheet, "rows": session_rows}

            # соберем превью сессии для подтверждения
            first_row = session_rows[0][0]
            date = first_row[0] if len(first_row) > 0 else ""
            shift = first_row[1] if len(first_row) > 1 else ""
            ts = first_row[5] if len(first_row) > 5 else ""
            msg = f"Вы собираетесь отменить последнюю сессию (все строки) пользователя.\n\nДата: {date}\nСмена: {shift}\nВремя отправки: {ts}\n\nПозиций в сессии: {len(session_rows)}\n\nСписок:\n"
            for r, _ in session_rows:
                prod = r[2] if len(r) > 2 else ""
                qty = r[3] if len(r) > 3 else ""
                msg += f"• {prod} — {qty}\n"

            tg_send(chat, msg, CONFIRM_KB)
            return


        # confirm cancel
        if "pending_cancel" in st:
            if text == "Да, отменить":
                pend = st["pending_cancel"]
                ws_title = pend["ws"]
                rows = pend["rows"]  # список (row, rownum)

                # пометим каждую строку статусом ОТМЕНЕНО (столбец G)
                for (_, rownum) in rows:
                    try:
                        self.sc.update_cell(ws_title, f"G{rownum}", "ОТМЕНЕНО")
                    except Exception:
                        log.exception("Failed to mark canceled row %s in %s", rownum, ws_title)

                # подготовим сообщение пользователю
                first_row = rows[0][0] if rows else None
                date = first_row[0] if first_row and len(first_row) > 0 else ""
                shift = first_row[1] if first_row and len(first_row) > 1 else ""
                ts = first_row[5] if first_row and len(first_row) > 5 else ""
                user_msg = (
                    f"❌ <b>Запись отменена</b>\n\n"
                    f"Дата: {date}\n"
                    f"Смена: {shift}\n"
                    f"Время отправки: {ts}\n"
                    f"Отменено позиций: {len(rows)}\n\n"
                    f"Отменил: {user['fio']}"
                )
                tg_send(chat, user_msg, FLOW_MENU_KB)

                # уведомим контролёров с деталями
                ctrl_sheet = CTRL_RF_SHEET if ws_title == RF_SHEET else CTRL_PPI_SHEET
                ctrl_msg = (
                    f"⚠️ <b>ОТМЕНЕНА ЗАПИСИ</b>\n\n"
                    f"Лист: {ws_title}\n"
                    f"Дата: {date}\n"
                    f"Смена: {shift}\n"
                    f"Время отправки: {ts}\n"
                    f"Отменено позиций: {len(rows)}\n\n"
                    "Позиции:\n"
                )
                for r, _ in rows:
                    prod = r[2] if len(r) > 2 else ""
                    qty = r[3] if len(r) > 3 else ""
                    ctrl_msg += f"• {prod} — {qty}\n"

                ctrl_msg += f"\nОтменил: {user['fio']}"

                for cid in get_controllers_cached(ctrl_sheet):
                    try:
                        tg_send(cid, ctrl_msg)
                    except Exception:
                        pass

                st["cancel_used"] = True
                st.pop("pending_cancel", None)
                return


            if text == "Нет, оставить":
                tg_send(chat, "Запись сохранена.", FLOW_MENU_KB)
                st.pop("pending_cancel", None)
                return

        # new record
        if text == "Новая запись":
            st["cancel_used"] = False
            # show recent records
            sheet = RF_SHEET if flow == "rf" else PPI_SHEET
            recent = self.sc.get_last_records(sheet, 5)
            msg = f"<b>Последние записи ({sheet}):</b>\n\n"
            if recent:
                # show up to 5
                lines = []
                for r in recent[-5:]:
                    date = r[0] if len(r) > 0 else ""
                    shift = r[1] if len(r) > 1 else ""
                    prod = r[2] if len(r) > 2 else ""
                    qty = r[3] if len(r) > 3 else ""
                    lines.append(f"• {date} | {shift} | {prod} | {qty}")
                msg += "\n".join(lines)
            else:
                msg += "Нет записей."
            tg_send(chat, msg)
            # start steps: date -> shift -> product -> quantity multi-cycle
            st.update({"step": "date", "data": {}})
            # ensure products_list cleared
            st.pop("products_list", None)
            today = now_msk().strftime("%d.%m.%Y")
            yest = (now_msk() - timedelta(days=1)).strftime("%d.%m.%Y")
            tg_send(chat, "Дата:", kb_reply([[today, yest], ["Другая дата", "Отмена"]]))
            return

        # step handling
        step = st.get("step")
        data = st.get("data", {})

        if not step:
            tg_send(chat, "Выберите действие:", FLOW_MENU_KB)
            return

        # date step
        if step == "date":
            if text == "Другая дата":
                st["step"] = "date_custom"
                tg_send(chat, "Введите дату (дд.мм.гггг):", CANCEL_KB)
                return
            try:
                datetime.strptime(text, "%d.%m.%Y")
                data["date"] = text
            except Exception:
                tg_send(chat, "Неверный формат даты.", CANCEL_KB)
                return
            st["step"] = "shift"
            tg_send(chat, "Выберите смену:", kb_reply([["День", "Ночь"], ["Отмена"]]))
            return

        if step == "date_custom":
            try:
                datetime.strptime(text, "%d.%m.%Y")
                data["date"] = text
                st["step"] = "shift"
                tg_send(chat, "Выберите смену:", kb_reply([["День", "Ночь"], ["Отмена"]]))
            except Exception:
                tg_send(chat, "Введите дату в формате дд.мм.гггг", CANCEL_KB)
            return

        # shift step
        if step == "shift":
            if text not in ("День", "Ночь"):
                tg_send(chat, "Выберите смену:", kb_reply([["День", "Ночь"], ["Отмена"]]))
                return
            data["shift"] = text
            # ask product from appropriate sheet
            prod_list_sheet = "Продукция РФ" if flow == "rf" else "Продукция ППИ"
            prod_kb = build_product_kb(prod_list_sheet, extra=["Другая продукция"])
            st["step"] = "product"
            st["data"] = data
            # initialize product list holder
            st["products_list"] = []
            tg_send(chat, "Выберите продукцию (или 'Другая продукция'):", prod_kb)
            return

        # === MULTI-PRODUCT CYCLE ===

        # select product
        if step == "product":
            if text == "Другая продукция":
                st["step"] = "product_custom"
                tg_send(chat, "Введите название продукции:", CANCEL_KB)
                return
            # user may press Отмена as keyboard button
            if text == "Отмена":
                tg_send(chat, "Отменено.", MAIN_KB)
                self.clear_state(uid)
                return
            data["product"] = text.strip()
            st["step"] = "quantity"
            tg_send(chat, "Введите количество:", NUMERIC_INPUT_KB)
            return

        if step == "product_custom":
            if text == "Отмена" or not text.strip():
                tg_send(chat, "Введите корректное название.", CANCEL_KB)
                return
            data["product"] = text.strip()
            st["step"] = "quantity"
            tg_send(chat, "Введите количество:", NUMERIC_INPUT_KB)
            return

        # quantity
        if step == "quantity":
            if text == "Отмена":
                tg_send(chat, "Отменено.", MAIN_KB)
                self.clear_state(uid)
                return
            if not (text.replace(",", ".").replace(".", "", 1).isdigit()):
                tg_send(chat, "Введите корректное число.", NUMERIC_INPUT_KB)
                return

            qty = text.replace(",", ".")
            product_name = data.get("product", "")

            # ensure products_list exists
            if "products_list" not in st:
                st["products_list"] = []

            st["products_list"].append({
                "product": product_name,
                "quantity": qty
            })

            # reset product field for next iteration
            data.pop("product", None)

            # Ask whether to continue cycle
            st["step"] = "add_more"
            tg_send(chat,
                    f"Добавлено:\n<b>{product_name}</b> — {qty}\n\nДобавить ещё продукцию?",
                    kb_reply([["Да, добавить"], ["Нет, завершить"], ["Отмена"]]))
            return

        # add_more step
        if step == "add_more":
            if text == "Да, добавить":
                # go back to product selection
                prod_list_sheet = "Продукция РФ" if flow == "rf" else "Продукция ППИ"
                prod_kb = build_product_kb(prod_list_sheet, extra=["Другая продукция"])
                st["step"] = "product"
                tg_send(chat, "Выберите продукцию:", prod_kb)
                return

            if text == "Нет, завершить":
                # Now write ALL items into the sheet
                target_sheet = RF_SHEET if flow == "rf" else PPI_SHEET
                plist = st.get("products_list", [])

                if not plist:
                    tg_send(chat, "Нет добавленных позиций.", MAIN_KB)
                    self.clear_state(uid)
                    return

                # Common fields
                user_field = f"{user['fio']} ({uid})"
                ts = now_msk_str()

                for item in plist:
                    row = [
                        data.get("date", ""),
                        data.get("shift", ""),
                        item.get("product", ""),
                        item.get("quantity", ""),
                        user_field,
                        ts,
                        ""
                    ]
                    try:
                        self.sc.append_record(target_sheet, row)
                    except Exception:
                        log.exception("append multiple failed")

                # confirmation
                msg = "✅ <b>Запись сохранена</b>\n\n"
                msg += f"{target_sheet}\n"
                msg += f"Дата: <b>{data.get('date','')}</b>\n"
                msg += f"Смена: <b>{data.get('shift','')}</b>\n\n"
                msg += "<b>Позиции:</b>\n"
                for item in plist:
                    msg += f"• {item['product']} — {item['quantity']}\n"

                tg_send(chat, msg, MAIN_KB)

                # notify controllers
                ctrl_sheet = CTRL_RF_SHEET if target_sheet == RF_SHEET else CTRL_PPI_SHEET
                notify = (
                    f"⚠️ <b>НОВАЯ ЗАПИСЬ ({target_sheet})</b>\n"
                    f"Дата: {data.get('date','')}\n"
                    f"Смена: {data.get('shift','')}\n"
                    f"Добавил: {user['fio']}\n\n"
                    "Позиции:\n"
                    + "\n".join([f"• {i['product']} — {i['quantity']}" for i in plist])
                )

                for cid in get_controllers_cached(ctrl_sheet):
                    try:
                        tg_send(cid, notify)
                    except Exception:
                        pass

                self.clear_state(uid)
                return

            # support user pressing Отмена or wrong input
            if text == "Отмена":
                tg_send(chat, "Отменено.", MAIN_KB)
                self.clear_state(uid)
                return

            tg_send(chat, "Нажмите Да или Завершить.", kb_reply([["Да"], ["Завершить"], ["Отмена"]]))
            return

        # fallback
        tg_send(chat, "Выберите действие:", FLOW_MENU_KB)


# ========== Flask webhook & callbacks ==========
app = Flask(__name__)
LOCK_PATH = "/tmp/bot.lock"

# start controllers refresher thread that warms cache once per day
def controllers_refresher_worker(interval_min: int = 1440):
    while True:
        try:
            sheet_client.invalidate_cache(CTRL_RF_SHEET)
            sheet_client.invalidate_cache(CTRL_PPI_SHEET)
            _ = sheet_client.get_controllers(CTRL_RF_SHEET)
            _ = sheet_client.get_controllers(CTRL_PPI_SHEET)
            log.info("Controllers cache refreshed")
        except Exception:
            log.exception("Error refreshing controllers cache")
        time.sleep(interval_min * 60)

threading.Thread(target=controllers_refresher_worker, daemon=True).start()

fsm = FSM(sheet_client, auth)

@app.route("/", methods=["POST"])
def webhook():
    update = request.get_json()
    if not update:
        return "ok", 200

    # handle callback_query (inline buttons for approvals)
    if "callback_query" in update:
        try:
            auth.process_callback(update["callback_query"])
            # answer callback
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                          json={"callback_query_id": update["callback_query"]["id"]})
        except Exception:
            log.exception("callback processing error")
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
        try:
            fsm.handle_text(user_id, chat_id, text, user_repr)
        except Exception:
            log.exception("Processing error")
    return "ok", 200

@app.route("/health")
def health():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
