# bot_webhook_optimized.py
# Vx — Оптимизированная версия (один файл): рефакторинг, кэш, цифровая клавиатура, расширенные уведомления

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
STARTSTOP_SHEET = "Старт-Стоп"
DEFECT_SHEET = "Брак"
CTRL_STARTSTOP_SHEET = "Контр_Старт-Стоп"
CTRL_DEFECT_SHEET = "Контр_Брак"
USERS_SHEET = "Пользователи"

HEADERS_STARTSTOP = ["Дата", "Время", "Номер линии", "Действие", "Причина", "ЗНП", "Метров брака", "Вид брака",
                     "Пользователь", "Время отправки", "Статус"]
USERS_HEADERS = ["TelegramID", "ФИО", "Роль", "Статус", "Запросил у", "Дата создания", "Подтвердил", "Дата подтверждения"]

# ========== Telegram send wrapper ==========
def tg_send(chat_id: int, text: str, markup: Optional[dict] = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        # ensure ascii=False for Cyrillic in reply_markup
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
        self._ensure_sheet(STARTSTOP_SHEET, HEADERS_STARTSTOP)
        self._ensure_sheet(DEFECT_SHEET, None)
        self._ensure_sheet(USERS_SHEET, USERS_HEADERS)

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

    def get_last_records(self, sheet_title: str, n: int = 2) -> List[List[str]]:
        vals = self._get_all_values_cached(sheet_title)
        if len(vals) <= 1:
            return []
        return vals[-n:]

    def update_cell(self, sheet_title: str, cell: str, value: Any):
        try:
            self._ws(sheet_title).update(cell, [[value]])
            self.invalidate_cache(sheet_title)
        except Exception as e:
            log.exception("update_cell error: %s", e)

    def find_last_active_record(self, sheet_title: str, uid: int) -> Tuple[Optional[List[str]], Optional[int]]:
        """
        Find last record for user (user ID appears as "(<id>)" in Пользователь column).
        For STARTSTOP sheet the user column is 9 (index 8). For DEFECT sheet user column is 8 (index 7).
        Returns (row, row_number) or (None, None).
        """
        vals = self._get_all_values_cached(sheet_title)
        if not vals or len(vals) <= 1:
            return None, None
        if sheet_title == DEFECT_SHEET:
            user_idx = 7  # 1-based column 8 → index 7
            status_idx = 9  # 1-based column 10 -> index 9
        else:
            user_idx = 8  # 1-based column 9 → index 8
            status_idx = 10  # 1-based column 11 → index 10

        for i in range(len(vals) - 1, 0, -1):
            row = vals[i]
            if len(row) <= user_idx:
                continue
            try:
                cell_user = row[user_idx]
            except Exception:
                continue
            if f"({uid})" in cell_user:
                # check status not ОТМЕНЕНО
                status = row[status_idx] if len(row) > status_idx else ""
                if status.strip() != "ОТМЕНЕНО":
                    return row, i + 1
        return None, None


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


MAIN_KB = kb_reply([["Старт/Стоп", "Брак"]])
FLOW_MENU_KB = kb_reply([["Новая запись"], ["Отменить последнюю запись"], ["Назад"]])
CANCEL_KB = kb_reply([["Отмена"]])
CONFIRM_KB = kb_reply([["Да, отменить"], ["Нет, оставить"]])

# NUM_LINE_KB — оставляем без one_time_keyboard, чтобы убрать мерцание
NUM_LINE_KB = {
    "keyboard": [
        [{"text": str(i)} for i in range(1, 6)],
        [{"text": str(i)} for i in range(6, 11)],
        [{"text": str(i)} for i in range(11, 16)],
        [{"text": "Отмена"}]
    ],
    "resize_keyboard": True,
    "one_time_keyboard": False,
    "input_field_placeholder": "Выберите номер линии"
}

# Numeric input keyboard (opens numeric keypad on phone; user types digits manually)
NUMERIC_INPUT_KB = {
    "keyboard": [
        [{"text": "Отмена"}]
    ],
    "resize_keyboard": True,
    "one_time_keyboard": False,
    "input_field_placeholder": "Введите число"
}

# reason/defect kb builders (with caching)
REASONS_CACHE: Dict[str, Any] = {"kb": None, "until": 0}
DEFECTS_CACHE: Dict[str, Any] = {"kb": None, "until": 0}


def build_sheet_kb(sheet_name: str, extra: Optional[List[str]] = None) -> dict:
    if extra is None:
        extra = []
    try:
        vals = sheet_client._ws(sheet_name).col_values(1)[1:]
    except Exception:
        vals = []
    items = [v.strip() for v in vals if v and v.strip()] + extra
    rows = [items[i:i + 2] for i in range(0, len(items), 2)]
    rows.append(["Отмена"])
    return kb_reply(rows, one_time=False)


def get_reasons_kb() -> dict:
    now_ts = time.time()
    if now_ts > REASONS_CACHE["until"]:
        REASONS_CACHE["kb"] = build_sheet_kb("Причина остановки", ["Другое"])
        REASONS_CACHE["until"] = now_ts + 300
    return REASONS_CACHE["kb"]


def get_defect_kb() -> dict:
    now_ts = time.time()
    if now_ts > DEFECTS_CACHE["until"]:
        DEFECTS_CACHE["kb"] = build_sheet_kb("Вид брака", ["Другое", "Без брака"])
        DEFECTS_CACHE["until"] = now_ts + 300
    return DEFECTS_CACHE["kb"]


# Controllers list (cached)
_CONTROLLERS_CACHE: Dict[str, Any] = {"startstop": {"until": 0, "data": []}, "defect": {"until": 0, "data": []}}
def get_controllers_cached(sheet_name: str, ttl: int = 600) -> List[int]:
    key = "startstop" if sheet_name == CTRL_STARTSTOP_SHEET else "defect"
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


# ZNP prefix helper
def znp_prefixes_now() -> Tuple[List[str], str, str]:
    now = now_msk()
    curr_month = now.month
    curr_year = now.year
    curr = f"{curr_month:02d}{str(curr_year)[2:]}"
    if curr_month == 1:
        prev_month = 12
        prev_year = curr_year - 1
    else:
        prev_month = curr_month - 1
        prev_year = curr_year
    prev = f"{prev_month:02d}{str(prev_year)[2:]}"
    valid = [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]
    return valid, curr, prev

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
        # Inline keyboard for approvers (they can pick approve/reject)
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
                # build inline kb to set role
                roles = ["operator", "master"]
                if approver["role"] == "admin":
                    roles.append("admin")
                kb = {"inline_keyboard": [[{"text": r, "callback_data": f"setrole_{target_id}_{r}"}] for r in roles]}
                tg_send(chat_id, f"Выберите роль для <b>{target['fio']}</b>:", kb)
            else:
                # reject
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
            # final assignment
            self.sc.update_user(target_id, role=role, status="подтвержден", confirmed_by=uid)
            target = self.get_user(target_id)
            tg_send(chat_id, f"Пользователь {target['fio']} подтверждён как <b>{role}</b>")
            # notify target
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
        # start timeout worker
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

        # --- command handlers (role approvals) ---
        if text.startswith("/setrole_") or text.startswith("/approve_") or text.startswith("/reject_"):
            # forward to auth manager style handlers if needed (we use inline callbacks for approvals)
            tg_send(chat, "Команда обработана через UI подтверждений (используйте инлайн-кнопки).")
            return

        # --- basic navigation ---
        if text == "Назад":
            self.clear_state(uid)
            tg_send(chat, "Главное меню:", MAIN_KB)
            return
        if text == "Отмена":
            st.pop("pending_cancel", None)
            self.clear_state(uid)
            tg_send(chat, "Отменено.", MAIN_KB)
            return

        # --- user auth check ---
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

        # --- flow selection ---
        if "flow" not in st:
            if text in ("/start", "Старт/Стоп"):
                st["flow"] = "startstop"
                tg_send(chat, "<b>Старт/Стоп</b>\nВыберите действие:", FLOW_MENU_KB)
                return
            if text == "Брак":
                st["flow"] = "defect"
                tg_send(chat, "<b>Брак</b>\nВыберите действие:", FLOW_MENU_KB)
                return
            # otherwise greet
            tg_send(chat, f"Привет, {user['fio'].split()[0]}! Выберите действие:", MAIN_KB)
            return

        flow = st["flow"]

        # --- Cancel last record ---
        if text == "Отменить последнюю запись":
            if st.get("cancel_used", False):
                tg_send(chat, "Вы уже отменили одну запись в этом сеансе. Сделайте новую запись, чтобы снова отменить.", FLOW_MENU_KB)
                return
            sheet = DEFECT_SHEET if flow == "defect" else STARTSTOP_SHEET
            row, rownum = self.sc.find_last_active_record(sheet, uid)
            if not row:
                tg_send(chat, "У вас нет активных записей для отмены.", FLOW_MENU_KB)
                return
            st["pending_cancel"] = {"ws": sheet, "row": row, "rownum": rownum}
            # build preview for confirmation
            if flow == "startstop":
                action = "Запуск" if len(row) > 3 and row[3] == "запуск" else "Остановка"
                msg = (f"Отменить эту запись?\n\n"
                       f"<b>Старт/Стоп</b>\n{row[0]} {row[1]} | Линия {row[2]}\nДействие: {action}\nПричина: {row[4] if len(row)>4 else '—'}")
            else:
                msg = (f"Отменить эту запись?\n\n"
                       f"<b>Брак</b>\n{row[0]} {row[1]} | Линия {row[2]}\nЗНП: <code>{row[4] if len(row)>4 else '—'}</code>\nМетров: {row[5] if len(row)>5 else '—'}\nВид: {row[6] if len(row)>6 else '—'}")
            tg_send(chat, msg, CONFIRM_KB)
            return

        # --- confirm cancel ---
        if "pending_cancel" in st:
            if text == "Да, отменить":
                pend = st["pending_cancel"]
                ws_title = pend["ws"]
                row = pend["row"]
                rownum = pend["rownum"]
                # choose status column depending on sheet
                status_col = "K" if ws_title == STARTSTOP_SHEET else "J"  # K=11, J=10 (1-based)
                try:
                    self.sc.update_cell(ws_title, f"{status_col}{rownum}", "ОТМЕНЕНО")
                except Exception:
                    log.exception("Failed to mark canceled")
                # notify controllers with full info + fio of original user
                # original user stored in row: in STARTSTOP user is column 9 (index 8), in DEFECT user is column 8 (index7)
                if ws_title == STARTSTOP_SHEET:
                    user_field = row[8] if len(row) > 8 else ""
                else:
                    user_field = row[7] if len(row) > 7 else ""
                # try extract user id from "(<id>)" pattern
                uid_in_row = None
                if "(" in user_field and ")" in user_field:
                    try:
                        uid_in_row = int(user_field.split("(")[-1].split(")")[0])
                    except Exception:
                        uid_in_row = None
                fio_of_orig = None
                if uid_in_row:
                    uobj = self.auth.get_user(uid_in_row)
                    fio_of_orig = uobj["fio"] if uobj else user_field
                else:
                    fio_of_orig = user_field
                # build cancel message
                if ws_title == STARTSTOP_SHEET:
                    action = "Запуск" if len(row) > 3 and row[3] == "запуск" else "Остановка"
                    reason = row[4] if len(row) > 4 else "—"
                    msg = (f"ОТМЕНЕНА ЗАПИСЬ СТАРТ/СТОП\nЛиния: {row[2]}\n{row[0]} {row[1]}\nДействие: {action}\nПричина: {reason}\nОтменил: {user['fio']}")
                    # notify controllers
                    for cid in get_controllers_cached(CTRL_STARTSTOP_SHEET):
                        try:
                            tg_send(cid, msg)
                        except Exception:
                            pass
                else:
                    znp = row[4] if len(row) > 4 else "—"
                    meters = row[5] if len(row) > 5 else "—"
                    defect_type = row[6] if len(row) > 6 else "—"
                    msg = (f"ОТМЕНЕНА ЗАПИСЬ БРАКА\nЛиния: {row[2]}\n{row[0]} {row[1]}\nЗНП: <code>{znp}</code>\nМетров: {meters}\nВид: {defect_type}\nОтменил: {user['fio']}")
                    for cid in get_controllers_cached(CTRL_DEFECT_SHEET):
                        try:
                            tg_send(cid, msg)
                        except Exception:
                            pass
                tg_send(chat, "Запись отменена!", FLOW_MENU_KB)
                st["cancel_used"] = True
                st.pop("pending_cancel", None)
                return
            if text == "Нет, оставить":
                tg_send(chat, "Отмена отменена. Запись сохранена.", FLOW_MENU_KB)
                st.pop("pending_cancel", None)
                return

        # --- New record ---
        if text == "Новая запись":
            st["cancel_used"] = False
            if flow == "defect":
                recent = self.sc.get_last_records(DEFECT_SHEET, 2)
                msg = "<b>Последние записи Брака:</b>\n\n"
                if recent:
                    msg += "\n".join(f"• {r[0]} {r[1]} | Линия {r[2]} | <code>{r[4] if len(r)>4 else '—'}</code> | {r[5] if len(r)>5 else '—'}м" for r in recent)
                else:
                    msg += "Нет записей."
                tg_send(chat, msg)
                st.update({"step": "line", "data": {"action": "брак"}})
            else:
                recent = self.sc.get_last_records(STARTSTOP_SHEET, 2)
                msg = "<b>Последние записи Старт/Стоп:</b>\n\n"
                if recent:
                    msg += "\n".join(f"• {r[0]} {r[1]} | Линия {r[2]} | {'Запуск' if (len(r)>3 and r[3]=='запуск') else 'Остановка'} | {r[4] if len(r)>4 else '—'}" for r in recent)
                else:
                    msg += "Нет записей."
                tg_send(chat, msg)
                st.update({"step": "line", "data": {}})
            tg_send(chat, "Введите номер линии (1–15):", NUM_LINE_KB)
            return

        # Step handling
        step = st.get("step")
        data = st.get("data", {})

        if not step:
            tg_send(chat, "Выберите действие:", FLOW_MENU_KB)
            return

        # --- step: line ---
        if step == "line":
            if not text.isdigit() or not (1 <= int(text) <= 15):
                tg_send(chat, "Номер линии должен быть от 1 до 15:", NUM_LINE_KB)
                return
            data["line"] = text
            st["step"] = "date"
            today = now_msk().strftime("%d.%m.%Y")
            yest = (now_msk() - timedelta(days=1)).strftime("%d.%m.%Y")
            tg_send(chat, "Дата:", kb_reply([[today, yest], ["Другая дата", "Отмена"]]))
            return

        # --- step: date ---
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
            st["step"] = "time"
            nowt = now_msk()
            t0 = nowt.strftime("%H:%M")
            t1 = (nowt - timedelta(minutes=10)).strftime("%H:%M")
            t2 = (nowt - timedelta(minutes=20)).strftime("%H:%M")
            t3 = (nowt - timedelta(minutes=30)).strftime("%H:%M")
            tg_send(chat, "Время:", kb_reply([[t0, t1, "Другое время"], [t2, t3, "Отмена"]]))
            return

        if step == "date_custom":
            try:
                datetime.strptime(text, "%d.%m.%Y")
                data["date"] = text
                st["step"] = "time"
                nowt = now_msk()
                t0 = nowt.strftime("%H:%M")
                t1 = (nowt - timedelta(minutes=10)).strftime("%H:%M")
                t2 = (nowt - timedelta(minutes=20)).strftime("%H:%M")
                t3 = (nowt - timedelta(minutes=30)).strftime("%H:%M")
                tg_send(chat, "Время:", kb_reply([[t0, t1, "Другое время"], [t2, t3, "Отмена"]]))
            except Exception:
                tg_send(chat, "Введите дату в формате дд.мм.гггг", CANCEL_KB)
            return

        # --- step: time ---
        if step in ("time", "time_custom"):
            if text == "Другое время":
                st["step"] = "time_custom"
                tg_send(chat, "Введите время (чч:мм):", CANCEL_KB)
                return
            if not (len(text) == 5 and text[2] == ":" and text[:2].isdigit() and text[3:].isdigit()):
                tg_send(chat, "Неверный формат времени.", CANCEL_KB)
                return
            data["time"] = text
            valid, curr, prev = znp_prefixes_now()
            if flow == "defect":
                st["step"] = "znp_prefix"
                kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
                tg_send(chat, "Префикс ЗНП:", kb_reply(kb))
            else:
                st["step"] = "action"
                tg_send(chat, "Действие:", kb_reply([["Запуск", "Остановка"], ["Отмена"]]))
            return

        # --- step: action ---
        if step == "action":
            if text not in ("Запуск", "Остановка"):
                tg_send(chat, "Выберите действие:", kb_reply([["Запуск", "Остановка"], ["Отмена"]]))
                return
            data["action"] = "запуск" if text == "Запуск" else "остановка"
            if data["action"] == "запуск":
                st["step"] = "znp_prefix"
                valid, curr, prev = znp_prefixes_now()
                kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
                tg_send(chat, "Префикс ЗНП:", kb_reply(kb))
            else:
                st["step"] = "reason"
                tg_send(chat, "Причина остановки:", get_reasons_kb())
            return

        # --- step: reason ---
        if step in ("reason", "reason_custom"):
            if text == "Другое" and step == "reason":
                st["step"] = "reason_custom"
                tg_send(chat, "Введите причину остановки:", CANCEL_KB)
                return
            data["reason"] = text
            st["step"] = "znp_prefix"
            valid, curr, prev = znp_prefixes_now()
            kb = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
            tg_send(chat, "Префикс ЗНП:", kb_reply(kb))
            return

        # --- step: znp_prefix / znp_manual ---
        if step in ("znp_prefix", "znp_manual"):
            valid, curr, prev = znp_prefixes_now()
            valid_set = [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]
            if step == "znp_prefix":
                if text in valid_set:
                    data["znp_prefix"] = text
                    # now ask for last 4 digits — show numeric keyboard
                    st["step"] = "znp_last4"
                    tg_send(chat, f"Последние 4 цифры ЗНП для <b>{text}</b>-XXXX:", NUMERIC_KB)
                    return
                if text == "Другое":
                    st["step"] = "znp_manual"
                    tg_send(chat, "Введите полный ЗНП (пример: D1225-1234):", CANCEL_KB)
                    return
                # maybe user typed 4 digits immediately but we didn't have prefix — disallow
                tg_send(chat, "Выберите префикс:", kb_reply([[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]))
                return
            else:  # znp_manual
                # expect format D1225-1234 (10 chars with '-')
                if len(text) == 10 and text[5] == "-" and text[:5].upper() in valid_set:
                    data["znp"] = text.upper()
                    st["step"] = "meters"
                    tg_send(chat, "Сколько метров брака?", NUMERIC_KB)
                    return
                tg_send(chat, "Неправильный формат ЗНП.\nПример: <code>D1225-1234</code>", CANCEL_KB)
                return

        # --- step: znp_last4 (new) ---
        if step == "znp_last4":
            if not (text.isdigit() and len(text) == 4):
                tg_send(chat, "Введите последние 4 цифры:", NUMERIC_INPUT_KB)
                return
            if "znp_prefix" in data:
                data["znp"] = f"{data['znp_prefix']}-{text}"
            else:
                data["znp"] = text
            st["step"] = "meters"
            tg_send(chat, "Сколько метров брака?", NUMERIC_INPUT_KB)
            return

        # --- step: meters ---
        if step == "meters":
            if not text.isdigit() or int(text) <= 0:
                tg_send(chat, "Укажите количество метров брака:", NUMERIC_INPUT_KB)
                return
            data["meters"] = text
            st["step"] = "defect_type"
            tg_send(chat, "Вид брака:", get_defect_kb())
            return

        # --- step: defect_type / defect_custom ---
        if step in ("defect_type", "defect_custom"):
            if text == "Другое" and step == "defect_type":
                st["step"] = "defect_custom"
                tg_send(chat, "Опишите вид брака:", CANCEL_KB)
                return
            data["defect_type"] = "" if text == "Без брака" else text

            # finalize record
            # attach user fio and id
            data["user"] = f"{user['fio']} ({uid})"
            data["flow"] = flow

            # build row according to sheet structure
            if flow == "defect":
                # DEFECT: Date, Time, Line, (no action), ZNP, Meters, DefectType, User, TS, Status
                row = [data["date"], data["time"], data["line"],
                       # For compatibility keep position 4 empty or "брак"? In original V3.9 they used "брак" in column 4,
                       # but user said "Брак" sheet doesn't have action; to keep compatibility we will NOT add action, we will shift:
                       # We'll use columns: Date, Time, Line, ZNP, Meters, DefectType, User, TS, Status
                       # To keep consistent column count, we will build row matching existing sheet expectation in original code:
                       # original append_row for defect used: [date, time, line, "брак", znp, meters, defect_type, user, ts, ""]
                       # but we've been told "Брак" doesn't have action; still many sheets historically had that 4th column; to be safe,
                       # we will keep the same append as original V3.9 but consumer code that reads will handle absence.
                       "брак",
                       data.get("znp", ""), data.get("meters", ""), data.get("defect_type", ""), data.get("user", ""), now_msk_str(), ""
                       ]
                target_sheet = DEFECT_SHEET
            else:
                # STARTSTOP: Date, Time, Line, Action, Reason, ZNP, Meters, DefectType, User, TS, Status
                row = [data["date"], data["time"], data["line"], data.get("action", ""),
                       data.get("reason", ""), data.get("znp", ""), data.get("meters", ""), data.get("defect_type", ""),
                       data.get("user", ""), now_msk_str(), ""]
                target_sheet = STARTSTOP_SHEET

            # append to sheet
            try:
                self.sc.append_record(target_sheet, row)
            except Exception:
                log.exception("Failed to append record")

            # prepare full confirmation for user
            dt = f"{data.get('date','') } {data.get('time','')}"
            line = data.get("line", "")
            if flow == "defect":
                znp = data.get("znp", "—")
                meters = data.get("meters", "—")
                defect_type = "Без брака" if data.get("defect_type","") == "" else data.get("defect_type","")
                confirm_text = (
                    f"✅ <b>Запись брака сохранена</b>\n\n"
                    f"Линия: <b>{line}</b>\n"
                    f"Дата и время: <b>{dt}</b>\n"
                    f"ЗНП: <code>{znp}</code>\n"
                    f"Метров брака: <b>{meters}</b>\n"
                    f"Вид брака: <b>{defect_type}</b>\n\n"
                    f"Добавил: {user['fio']}"
                )
                # notify controllers with full info + fio
                controllers = get_controllers_cached(CTRL_DEFECT_SHEET)
                notify_msg = (f"⚠️ <b>НОВАЯ ЗАПИСЬ БРАКА</b>\n\n"
                              f"Линия: <b>{line}</b>\n"
                              f"Дата и время: {dt}\n"
                              f"ЗНП: <code>{znp}</code>\n"
                              f"Метров брака: {meters}\n"
                              f"Вид брака: {defect_type}\n"
                              f"Добавил: {user['fio']}")
                for cid in controllers:
                    try:
                        tg_send(cid, notify_msg)
                    except Exception:
                        pass
            else:
                action_ru = "Запуск" if data.get("action") == "запуск" else "Остановка"
                reason = data.get("reason", "—")
                znp = data.get("znp", "—")
                meters = data.get("meters", "")
                defect_type = data.get("defect_type", "")
                confirm_text = (f"✅ <b>Запись Старт/Стоп сохранена</b>\n\n"
                                f"Линия: <b>{line}</b>\n"
                                f"Дата и время: <b>{dt}</b>\n"
                                f"Действие: <b>{action_ru}</b>\n"
                                f"Причина: <b>{reason}</b>\n")
                if znp and znp != "—":
                    confirm_text += f"ЗНП: <code>{znp}</code>\n"
                if meters:
                    confirm_text += f"Метров брака: <b>{meters}</b>\n"
                if defect_type:
                    confirm_text += f"Вид брака: <b>{defect_type}</b>\n"
                confirm_text += f"\nДобавил: {user['fio']}"

                # notify controllers
                controllers = get_controllers_cached(CTRL_STARTSTOP_SHEET)
                notify_msg = (f"⚠️ <b>НОВАЯ ЗАПИСЬ СТАРТ/СТОП</b>\n\n"
                              f"Линия: <b>{line}</b>\n"
                              f"Дата и время: {dt}\n"
                              f"Действие: {action_ru}\n"
                              f"Причина: {reason}\n")
                if znp and znp != "—":
                    notify_msg += f"ЗНП: <code>{znp}</code>\n"
                if meters:
                    notify_msg += f"Метров брака: {meters}\n"
                if defect_type:
                    notify_msg += f"Вид брака: {defect_type}\n"
                notify_msg += f"Добавил: {user['fio']}"
                for cid in controllers:
                    try:
                        tg_send(cid, notify_msg)
                    except Exception:
                        pass

            # send confirmation to user
            tg_send(chat, confirm_text, MAIN_KB)
            # finalize state
            self.clear_state(uid)
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
            # invalidate and prefetch
            sheet_client.invalidate_cache(CTRL_STARTSTOP_SHEET)
            sheet_client.invalidate_cache(CTRL_DEFECT_SHEET)
            _ = sheet_client.get_controllers(CTRL_STARTSTOP_SHEET)
            _ = sheet_client.get_controllers(CTRL_DEFECT_SHEET)
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
