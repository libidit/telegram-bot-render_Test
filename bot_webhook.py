# V4.2 — Optimized: Авторизация пользователей, подтверждение заявок, выбор роли, журнал подтверждений
# Реархитектура: SheetClient, Keyboards, AuthManager, FSM, минимальный топ-уровень process()
import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Dict, Any

from flask import Flask, request
import gspread
from google.oauth2 import service_account
from filelock import FileLock
import requests

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# ==================== ENV ====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")
if not all([TELEGRAM_TOKEN, SPREADSHEET_ID, GOOGLE_CREDS_JSON]):
    raise RuntimeError("Missing required env vars")

# ==================== Google Sheets setup ====================
creds_dict = json.loads(GOOGLE_CREDS_JSON)
creds = service_account.Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

# ==================== Timezone helpers ====================
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK)


def now_msk_str() -> str:
    return now_msk().strftime("%Y-%m-%d %H:%M:%S")


# ==================== Constants / Sheet names / Headers ====================
STARTSTOP_SHEET = "Старт-Стоп"
DEFECT_SHEET = "Брак"
CTRL_STARTSTOP_SHEET = "Контр_Старт-Стоп"
CTRL_DEFECT_SHEET = "Контр_Брак"
USERS_SHEET = "Пользователи"
HEADERS_STARTSTOP = ["Дата", "Время", "Номер линии", "Действие", "Причина", "ЗНП", "Метров брака", "Вид брака",
                     "Пользователь", "Время отправки", "Статус"]
USERS_HEADERS = ["TelegramID", "ФИО", "Роль", "Статус", "Запросил у", "Дата создания", "Подтвердил", "Дата подтверждения"]

# ==================== Simple Telegram send wrapper ====================
def tg_send(chat_id: int, text: str, markup: Optional[dict] = None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        log.exception("tg_send error: %s", e)


# ==================== SheetClient: encapsulates sheet access + caching ====================
class SheetClient:
    def __init__(self, sh_obj, cache_ttl=5):
        self.sh = sh_obj
        self.cache_ttl = cache_ttl
        # cache structure: {sheet_name: {"until": ts, "data": list_of_rows}}
        self._cache: Dict[str, Dict[str, Any]] = {}
        # Ensure expected sheets exist and headers in place
        self._ensure_sheet_with_headers(STARTSTOP_SHEET, HEADERS_STARTSTOP)
        self._ensure_sheet_with_headers(DEFECT_SHEET, None)
        self._ensure_sheet_with_headers(USERS_SHEET, USERS_HEADERS)

    def _ensure_sheet_with_headers(self, title: str, headers: Optional[List[str]]):
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
        now = time.time()
        c = self._cache.get(title)
        if c and c["until"] > now:
            return c["data"]
        try:
            data = self._ws(title).get_all_values()
        except Exception as e:
            log.exception("Google get_all_values error for %s: %s", title, e)
            data = []
        self._cache[title] = {"until": now + self.cache_ttl, "data": data}
        return data

    def invalidate_cache(self, title: Optional[str] = None):
        if title:
            self._cache.pop(title, None)
        else:
            self._cache.clear()

    # === Users-related ===
    def get_users_rows(self) -> List[List[str]]:
        return self._get_all_values_cached(USERS_SHEET)

    def find_user(self, uid: int) -> Optional[Dict[str, str]]:
        rows = self.get_users_rows()
        for row in rows[1:]:
            if row and row[0] == str(uid):
                return {
                    "id": str(uid),
                    "fio": row[1] if len(row) > 1 else "",
                    "role": row[2] if len(row) > 2 and row[2].strip() else "operator",
                    "status": row[3] if len(row) > 3 and row[3].strip() else "ожидает"
                }
        return None

    def add_user(self, uid: int, fio: str, requested_by: str = ""):
        row = [str(uid), fio, "operator", "ожидает", requested_by or "", now_msk_str(), "", ""]
        try:
            self._ws(USERS_SHEET).append_row(row, value_input_option="USER_ENTERED")
            self.invalidate_cache(USERS_SHEET)
        except Exception as e:
            log.exception("add_user error: %s", e)

    def find_user_row_index(self, uid: int) -> Optional[int]:
        rows = self.get_users_rows()
        for idx, row in enumerate(rows[1:], start=2):
            if row and row[0] == str(uid):
                return idx
        return None

    def update_user_status(self, uid: int, status: str):
        idx = self.find_user_row_index(uid)
        if idx:
            try:
                self._ws(USERS_SHEET).update(f"D{idx}", [[status]])
                self.invalidate_cache(USERS_SHEET)
            except Exception as e:
                log.exception("update_user_status error: %s", e)

    def update_user_role(self, uid: int, role: str):
        idx = self.find_user_row_index(uid)
        if idx:
            try:
                self._ws(USERS_SHEET).update(f"C{idx}", [[role]])
                self.invalidate_cache(USERS_SHEET)
            except Exception as e:
                log.exception("update_user_role error: %s", e)

    def set_user_approver_info(self, uid: int, approver_uid: int):
        idx = self.find_user_row_index(uid)
        if idx:
            try:
                self._ws(USERS_SHEET).update(f"G{idx}", [[str(approver_uid)]])
                self._ws(USERS_SHEET).update(f"H{idx}", [[now_msk_str()]])
                self.invalidate_cache(USERS_SHEET)
            except Exception as e:
                log.exception("set_user_approver_info error: %s", e)

    def get_approvers(self) -> List[int]:
        res = []
        rows = self.get_users_rows()[1:]
        for row in rows:
            if len(row) >= 4:
                role = (row[2] or "").strip()
                status = (row[3] or "").strip()
                if role in ("admin", "master") and status == "подтвержден":
                    if row[0].strip().isdigit():
                        res.append(int(row[0].strip()))
        return res

    # === Controllers (контролёры) ===
    def get_controllers(self, title: str) -> List[int]:
        try:
            ws = self._ws(title)
            ids = ws.col_values(1)[1:]
            return [int(i.strip()) for i in ids if i.strip().isdigit()]
        except Exception as e:
            log.exception("get_controllers(%s) error: %s", title, e)
            return []

    # === Records and rows ===
    def append_record(self, flow: str, row: List[Any]):
        ws_title = DEFECT_SHEET if flow == "defect" else STARTSTOP_SHEET
        try:
            self._ws(ws_title).append_row(row, value_input_option="USER_ENTERED")
            # Invalidate caches for that sheet (so subsequent reads are fresh)
            self.invalidate_cache(ws_title)
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

    def find_last_active_record(self, sheet_title: str, user_repr: str) -> Tuple[Optional[List[str]], Optional[int]]:
        """
        Returns (row, row_number_1based) or (None, None)
        Note: columns indexing follows original layout
        """
        vals = self._get_all_values_cached(sheet_title)
        if sheet_title == DEFECT_SHEET:
            user_col = 7  # zero-based index 6 -> but in code we used 7 meaning human readable; adapt carefully
            status_col = 9
        else:
            user_col = 8
            status_col = 10

        # convert to zero-based indices for Python lists:
        user_idx = user_col - 1
        status_idx = status_col - 1

        for i in range(len(vals) - 1, 0, -1):
            row = vals[i]
            if len(row) <= user_idx:
                continue
            cell_user = (row[user_idx] or "").strip()
            cell_status = (row[status_idx].strip() if len(row) > status_idx else "")
            if cell_user == user_repr and cell_status != "ОТМЕНЕНО":
                # row number in sheet is i+1 (1-based)
                return row, i + 1
        return None, None


# ==================== Keyboards factory (centralized) ====================
class Keyboards:
    @staticmethod
    def keyboard(rows: List[List[str]]) -> dict:
        return {
            "keyboard": [[{"text": t} for t in row] for row in rows],
            "resize_keyboard": True,
            "one_time_keyboard": False
        }

    @staticmethod
    def num_line_kb() -> dict:
        rows = [
            [str(i) for i in range(1, 6)],
            [str(i) for i in range(6, 11)],
            [str(i) for i in range(11, 16)],
            ["Отмена"]
        ]
        # convert to list of lists of strings
        return {"keyboard": [[{"text": t} for t in row] for row in rows],
                "resize_keyboard": True,
                "one_time_keyboard": True,
                "input_field_placeholder": "Выберите номер линии"}

    @staticmethod
    def cancel_kb() -> dict:
        return Keyboards.keyboard([["Отмена"]])

    @staticmethod
    def main_kb() -> dict:
        return Keyboards.keyboard([["Старт/Стоп", "Брак"]])

    @staticmethod
    def flow_menu_kb() -> dict:
        return Keyboards.keyboard([["Новая запись"], ["Отменить последнюю запись"], ["Назад"]])

    @staticmethod
    def confirm_kb() -> dict:
        return Keyboards.keyboard([["Да, отменить"], ["Нет, оставить"]])

    # Generic builders for reasons/defects based on sheet values (2 columns per row)
    @staticmethod
    def build_from_list(items: List[str], extra: Optional[List[str]] = None) -> dict:
        if extra is None:
            extra = []
        items = [i.strip() for i in items if i and i.strip()] + extra
        rows = [items[i:i + 2] for i in range(0, len(items), 2)]
        rows.append(["Отмена"])
        return Keyboards.keyboard(rows)


# ==================== AuthManager: registration / approval / roles ====================
class AuthManager:
    def __init__(self, sheet_client: SheetClient):
        self.sc = sheet_client

    def get_user(self, uid: int) -> Optional[Dict[str, str]]:
        return self.sc.find_user(uid)

    def register_user(self, uid: int, fio: str, requested_by: str = ""):
        self.sc.add_user(uid, fio, requested_by)
        # notify approvers immediately
        self.notify_approvers_new_user(uid, fio)

    def get_approvers(self) -> List[int]:
        return self.sc.get_approvers()

    def notify_approvers_new_user(self, uid: int, fio: str):
        approvers = self.get_approvers()
        if not approvers:
            log.info("No approvers to notify for new user %s", uid)
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
                tg_send(a, text, kb)
            except Exception as e:
                log.exception("notify approver error: %s", e)

    @staticmethod
    def can_approver_confirm(approver_role: str, target_role: str) -> bool:
        if approver_role == "admin":
            return True
        if approver_role == "master":
            # master confirms operator and master (but not admin)
            return target_role in ("operator", "master")
        return False

    def ask_role_selection(self, approver_uid: int, target_uid: int, fio: str, approver_role: str):
        buttons = ["operator", "master"]
        if approver_role == "admin":
            buttons.append("admin")
        kb_rows = []
        row = []
        for b in buttons:
            row.append(f"/setrole_{target_uid}_{b}")
            if len(row) == 2:
                kb_rows.append(row)
                row = []
        if row:
            kb_rows.append(row)
        kb_rows.append(["/cancel_setrole", "Отмена"])
        kb = {"keyboard": [[{"text": t} for t in r] for r in kb_rows], "resize_keyboard": True, "one_time_keyboard": False}
        msg = (f"Вы подтверждаете пользователя:\n"
               f"ФИО: {fio}\n"
               f"TelegramID: {target_uid}\n\n"
               f"Выберите роль для пользователя:")
        tg_send(approver_uid, msg, kb)

    def approve_by(self, approver_uid: int, target_uid: int):
        approver = self.get_user(approver_uid)
        target = self.get_user(target_uid)
        if not approver:
            tg_send(approver_uid, "Вы не зарегистрированы или не подтверждены — нет прав подтверждать.")
            return
        if approver["status"] != "подтвержден":
            tg_send(approver_uid, "Ваш аккаунт не подтверждён — нет прав подтверждать.")
            return
        if not target:
            tg_send(approver_uid, f"Пользователь {target_uid} не найден в списке заявок.")
            return
        # Проверка прав (по текущей роли target в таблице)
        if not self.can_approver_confirm(approver["role"], target["role"]):
            tg_send(approver_uid, "У вас нет прав подтверждать этого пользователя.")
            return
        self.ask_role_selection(approver_uid, target_uid, target["fio"], approver["role"])

    def reject_by(self, approver_uid: int, target_uid: int):
        approver = self.get_user(approver_uid)
        target = self.get_user(target_uid)
        if not approver:
            tg_send(approver_uid, "Вы не зарегистрированы или не подтверждены — нет прав отклонять.")
            return
        if approver["status"] != "подтвержден":
            tg_send(approver_uid, "Ваш аккаунт не подтверждён — нет прав отклонять.")
            return
        if not target:
            tg_send(approver_uid, f"Пользователь {target_uid} не найден.")
            return
        # Set status rejected and set approver info
        self.sc.update_user_status(target_uid, "отклонен")
        self.sc.set_user_approver_info(target_uid, approver_uid)
        tg_send(approver_uid, f"Заявка пользователя {target['fio']} отклонена.")
        try:
            tg_send(int(target_uid), "В доступе отказано.")
        except Exception:
            pass

    def set_role(self, approver_uid: int, target_uid: int, new_role: str):
        approver = self.get_user(approver_uid)
        target = self.get_user(target_uid)
        if not approver or approver["status"] != "подтвержден":
            tg_send(approver_uid, "Вы не можете назначать роли.")
            return
        if not target:
            tg_send(approver_uid, "Целевой пользователь не найден.")
            return
        if approver["role"] == "master" and new_role == "admin":
            tg_send(approver_uid, "Мастер не может назначать роль admin.")
            return
        if new_role not in ("operator", "master", "admin"):
            tg_send(approver_uid, "Неверная роль.")
            return
        if not self.can_approver_confirm(approver["role"], new_role):
            tg_send(approver_uid, "У вас нет прав назначать такую роль.")
            return
        self.sc.update_user_role(target_uid, new_role)
        self.sc.update_user_status(target_uid, "подтвержден")
        self.sc.set_user_approver_info(target_uid, approver_uid)
        tg_send(approver_uid, f"Пользователь {target['fio']} подтверждён и получил роль {new_role}.")
        try:
            tg_send(int(target_uid), f"Ваша заявка подтверждена! Ваша роль: {new_role}.")
        except Exception:
            pass


# ==================== Utility helpers ====================
def get_znp_prefixes_now() -> Tuple[str, str, List[str]]:
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
    return curr, prev, valid


# ==================== FSM: finite-state handling of flow steps ====================
class FSM:
    def __init__(self, sheet_client: SheetClient, auth: AuthManager):
        self.sc = sheet_client
        self.auth = auth
        self.states: Dict[int, dict] = {}  # uid -> state dict
        self.last_activity: Dict[int, float] = {}
        self.TIMEOUT = 600  # seconds
        # caches for reasons/defects keyboards (short TTL, uses sheet cache underneath)
        self.reasons_cache: Dict[str, Any] = {"kb": None, "until": 0}
        self.defects_cache: Dict[str, Any] = {"kb": None, "until": 0}
        # Start background timeout worker
        threading.Thread(target=self._timeout_worker, daemon=True).start()

    def _timeout_worker(self):
        while True:
            time.sleep(30)
            now = time.time()
            for uid in list(self.states.keys()):
                if now - self.last_activity.get(uid, now) > self.TIMEOUT:
                    try:
                        st = self.states.pop(uid, None)
                        if st:
                            tg_send(st.get("chat"), "Диалог прерван — неактивность 10 минут.")
                        self.last_activity.pop(uid, None)
                    except Exception:
                        pass

    def update_activity(self, uid: int):
        self.last_activity[uid] = time.time()

    def ensure_state(self, uid: int, chat: int):
        if uid not in self.states:
            self.states[uid] = {"chat": chat, "cancel_used": False}
        else:
            # keep chat up-to-date
            self.states[uid]["chat"] = chat

    def clear_state(self, uid: int):
        if uid in self.states:
            self.states.pop(uid, None)
        self.last_activity.pop(uid, None)

    # ===== keyboards for reasons/defects (use sheet values with caching) =====
    def get_reasons_kb(self):
        now = time.time()
        if now > self.reasons_cache["until"]:
            try:
                vals = self.sc._ws("Причина остановки").col_values(1)[1:]
            except Exception:
                vals = []
            self.reasons_cache["kb"] = Keyboards.build_from_list(vals, ["Другое"])
            self.reasons_cache["until"] = now + 300
        return self.reasons_cache["kb"]

    def get_defect_kb(self):
        now = time.time()
        if now > self.defects_cache["until"]:
            try:
                vals = self.sc._ws("Вид брака").col_values(1)[1:]
            except Exception:
                vals = []
            self.defects_cache["kb"] = Keyboards.build_from_list(vals, ["Другое", "Без брака"])
            self.defects_cache["until"] = now + 300
        return self.defects_cache["kb"]

    # ===== high-level public entry point =====
    def handle_text(self, uid: int, chat: int, text: str, user_repr: str):
        self.update_activity(uid)
        self.ensure_state(uid, chat)
        state = self.states[uid]

        # handle commands first
        # setrole_/approve_/reject_ handled by AuthManager (delegation)
        if text.startswith("/setrole_"):
            # format /setrole_<target>_<role>
            payload = text[len("/setrole_"):]
            parts = payload.split("_", 1)
            if len(parts) != 2:
                tg_send(chat, "Неверная команда выбора роли.")
                return
            try:
                target_uid = int(parts[0])
                new_role = parts[1]
            except Exception:
                tg_send(chat, "Неверная команда выбора роли.")
                return
            self.auth.set_role(uid, target_uid, new_role)
            return

        if text.startswith("/approve_"):
            target_part = text.split("_", 1)[1]
            target_id = "".join(ch for ch in target_part if ch.isdigit())
            if not target_id:
                tg_send(chat, "Неверный формат команды.")
                return
            self.auth.approve_by(uid, int(target_id))
            return

        if text.startswith("/reject_"):
            target_part = text.split("_", 1)[1]
            target_id = "".join(ch for ch in target_part if ch.isdigit())
            if not target_id:
                tg_send(chat, "Неверный формат команды.")
                return
            self.auth.reject_by(uid, int(target_id))
            return

        # Basic built-in commands
        if text == "Назад":
            self.clear_state(uid)
            tg_send(chat, "Главное меню:", Keyboards.main_kb())
            return

        if text == "Отмена":
            state.pop("pending_cancel", None)
            tg_send(chat, "Отменено.", Keyboards.main_kb())
            return

        # Now ensure user is registered and confirmed
        u = self.auth.get_user(uid)
        if u is None:
            # awaiting fio?
            if state.get("fio_wait"):
                fio = text.strip()
                if not fio:
                    tg_send(chat, "Введите корректное ФИО:", Keyboards.cancel_kb())
                    return
                self.auth.register_user(uid, fio, requested_by="")
                tg_send(chat, "Спасибо! Ваша заявка отправлена на подтверждение.")
                self.clear_state(uid)
                return
            else:
                state["fio_wait"] = True
                tg_send(chat, "Вы не зарегистрированы. Введите ваше ФИО:")
                return

        if u["status"] != "подтвержден":
            tg_send(chat, "Ваш доступ пока не подтверждён администратором.")
            return

        # Main menu / flow selection
        if "flow" not in state:
            if text in ("/start", "Старт/Стоп"):
                tg_send(chat, "<b>Старт/Стоп</b>\nВыберите действие:", Keyboards.flow_menu_kb())
                state.update({"flow": "startstop"})
                return
            elif text == "Брак":
                tg_send(chat, "<b>Брак</b>\nВыберите действие:", Keyboards.flow_menu_kb())
                state.update({"flow": "defect"})
                return
            else:
                tg_send(chat, "Выберите действие:", Keyboards.main_kb())
                return

        # Flow established
        flow = state["flow"]

        # Cancel last entry flow
        if text == "Отменить последнюю запись":
            if state.get("cancel_used", False):
                tg_send(chat, "Вы уже отменили одну запись в этом сеансе. Сделайте новую запись, чтобы снова отменить.",
                        Keyboards.flow_menu_kb())
                return
            sheet = DEFECT_SHEET if flow == "defect" else STARTSTOP_SHEET
            row, row_num = self.sc.find_last_active_record(sheet, user_repr)
            if not row:
                tg_send(chat, "У вас нет активных записей для отмены.", Keyboards.flow_menu_kb())
                return
            state["pending_cancel"] = {"ws": sheet, "row": row, "row_num": row_num}
            # prepare confirmation message
            if flow == "startstop":
                action = "Запуск" if len(row) > 3 and row[3] == "запуск" else "Остановка"
                msg = (f"Отменить эту запись?\n\n"
                       f"<b>Старт/Стоп</b>\n"
                       f"{row[0]} {row[1]} | Линия {row[2]}\n"
                       f"Действие: {action}\n"
                       f"Причина: {row[4] if len(row) > 4 else '—'}")
            else:
                msg = (f"Отменить эту запись?\n\n"
                       f"<b>Брак</b>\n"
                       f"{row[0]} {row[1]} | Линия {row[2]}\n"
                       f"ЗНП: <code>{row[4] if len(row) > 4 else ''}</code>\n"
                       f"Метров: {row[5] if len(row) > 5 else ''}")
            tg_send(chat, msg, Keyboards.confirm_kb())
            return

        # confirm cancellation
        if "pending_cancel" in state:
            pend = state["pending_cancel"]
            if text == "Да, отменить":
                ws_title = pend["ws"]
                row = pend["row"]
                row_num = pend["row_num"]
                status_col = "K" if flow == "startstop" else "J"  # as in original
                try:
                    self.sc.update_cell(ws_title, f"{status_col}{row_num}", "ОТМЕНЕНО")
                except Exception as e:
                    log.exception("Error marking cancelled: %s", e)
                # notify controllers
                if flow == "startstop":
                    action = "Запуск" if len(row) > 3 and row[3] == "запуск" else "Остановка"
                    # notify controllers of startstop (fetch controllers)
                    controllers = self.sc.get_controllers(CTRL_STARTSTOP_SHEET)
                    for cid in controllers:
                        try:
                            tg_send(cid,
                                    f"ОТМЕНЕНА ЗАПИСЬ СТАРТ/СТОП\nПользователь: {user_repr}\nЛиния: {row[2]}\n{row[0]} {row[1]}\nДействие: {action}")
                        except Exception:
                            pass
                    tg_send(chat, f"Запись отменена:\n{row[0]} {row[1]} | Линия {row[2]} | {action}", Keyboards.flow_menu_kb())
                else:
                    controllers = self.sc.get_controllers(CTRL_DEFECT_SHEET)
                    for cid in controllers:
                        try:
                            tg_send(cid,
                                    f"ОТМЕНЕНА ЗАПИСЬ БРАКА\nПользователь: {user_repr}\nЛиния: {row[2]}\n{row[0]} {row[1]}\nЗНП: <code>{row[4]}</code>\nМетров: {row[5]}")
                        except Exception:
                            pass
                    tg_send(chat, f"Запись брака отменена:\n{row[0]} {row[1]} | Линия {row[2]}", Keyboards.flow_menu_kb())
                state["cancel_used"] = True
                state.pop("pending_cancel", None)
                return
            if text == "Нет, оставить":
                tg_send(chat, "Отмена отменена. Запись сохранена.", Keyboards.flow_menu_kb())
                state.pop("pending_cancel", None)
                return

        # New record
        if text == "Новая запись":
            state["cancel_used"] = False
            if flow == "defect":
                records = self.sc.get_last_records(DEFECT_SHEET, 2)
                msg = "<b>Последние записи Брака:</b>\n\n"
                if records:
                    msg += "\n".join(
                        f"• {r[0]} {r[1]} | Линия {r[2]} | <code>{r[4] if len(r) > 4 else '—'}</code> | {r[5] if len(r) > 5 else '—'}м"
                        for r in records)
                else:
                    msg += "Нет записей."
                tg_send(chat, msg)
                state.update({"step": "line", "data": {"action": "брак"}})
            else:
                records = self.sc.get_last_records(STARTSTOP_SHEET, 2)
                msg = "<b>Последние записи Старт/Стоп:</b>\n\n"
                if records:
                    msg += "\n".join(
                        f"• {r[0]} {r[1]} | Линия {r[2]} | {'Запуск' if (len(r) > 3 and r[3] == 'запуск') else 'Остановка'} | {r[4] if len(r) > 4 else '—'}"
                        for r in records)
                else:
                    msg += "Нет записей."
                tg_send(chat, msg)
                state.update({"step": "line", "data": {}})
            tg_send(chat, "Введите номер линии (1–15):", Keyboards.num_line_kb())
            return

        # Step processing: delegated to per-step handlers
        step = state.get("step")
        data = state.get("data", {})

        if not step:
            tg_send(chat, "Выберите действие:", Keyboards.flow_menu_kb())
            return

        # --- handler: line ---
        if step == "line":
            if not (text.isdigit() and 1 <= int(text) <= 15):
                tg_send(chat, "Номер линии должен быть от 1 до 15:", Keyboards.num_line_kb())
                return
            data["line"] = text
            state["step"] = "date"
            today = now_msk().strftime("%d.%m.%Y")
            yest = (now_msk() - timedelta(days=1)).strftime("%d.%m.%Y")
            tg_send(chat, "Дата:", Keyboards.keyboard([[today, yest], ["Другая дата", "Отмена"]]))
            return

        # --- handler: date ---
        if step == "date":
            if text == "Другая дата":
                state["step"] = "date_custom"
                tg_send(chat, "Введите дату (дд.мм.гггг):", Keyboards.cancel_kb())
                return
            try:
                datetime.strptime(text, "%d.%m.%Y")
                data["date"] = text
            except Exception:
                tg_send(chat, "Неверный формат даты.", Keyboards.cancel_kb())
                return
            state["step"] = "time"
            now = now_msk()
            t0 = now.strftime("%H:%M")
            t1 = (now - timedelta(minutes=10)).strftime("%H:%M")
            t2 = (now - timedelta(minutes=20)).strftime("%H:%M")
            t3 = (now - timedelta(minutes=30)).strftime("%H:%M")
            tg_send(chat, "Время:", Keyboards.keyboard([[t0, t1, "Другое время"], [t2, t3, "Отмена"]]))
            return

        if step == "date_custom":
            try:
                datetime.strptime(text, "%d.%m.%Y")
                data["date"] = text
                state["step"] = "time"
                now = now_msk()
                t0 = now.strftime("%H:%M")
                t1 = (now - timedelta(minutes=10)).strftime("%H:%M")
                t2 = (now - timedelta(minutes=20)).strftime("%H:%M")
                t3 = (now - timedelta(minutes=30)).strftime("%H:%M")
                tg_send(chat, "Время:", Keyboards.keyboard([[t0, t1, "Другое время"], [t2, t3, "Отмена"]]))
            except Exception:
                tg_send(chat, "Введите дату в формате дд.мм.гггг", Keyboards.cancel_kb())
            return

        # --- handler: time ---
        if step in ("time", "time_custom"):
            if text == "Другое время":
                state["step"] = "time_custom"
                tg_send(chat, "Введите время (чч:мм):", Keyboards.cancel_kb())
                return
            if not (len(text) == 5 and text[2] == ":" and text[:2].isdigit() and text[3:].isdigit()):
                tg_send(chat, "Неверный формат времени.", Keyboards.cancel_kb())
                return
            data["time"] = text
            if flow == "defect":
                state["step"] = "znp_prefix"
                curr, prev, valid = get_znp_prefixes_now()
                kb_rows = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
                tg_send(chat, "Префикс ЗНП:", Keyboards.keyboard(kb_rows))
                return
            else:
                state["step"] = "action"
                tg_send(chat, "Действие:", Keyboards.keyboard([["Запуск", "Остановка"], ["Отмена"]]))
                return

        # --- handler: action ---
        if step == "action":
            if text not in ("Запуск", "Остановка"):
                tg_send(chat, "Выберите действие:", Keyboards.keyboard([["Запуск", "Остановка"], ["Отмена"]]))
                return
            data["action"] = "запуск" if text == "Запуск" else "остановка"
            if data["action"] == "запуск":
                state["step"] = "znp_prefix"
                curr, prev, valid = get_znp_prefixes_now()
                kb_rows = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
                tg_send(chat, "Префикс ЗНП:", Keyboards.keyboard(kb_rows))
            else:
                state["step"] = "reason"
                tg_send(chat, "Причина остановки:", self.get_reasons_kb())
            return

        # --- handler: reason ---
        if step in ("reason", "reason_custom"):
            if text == "Другое" and step == "reason":
                state["step"] = "reason_custom"
                tg_send(chat, "Введите причину остановки:", Keyboards.cancel_kb())
                return
            data["reason"] = text
            state["step"] = "znp_prefix"
            curr, prev, valid = get_znp_prefixes_now()
            kb_rows = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
            tg_send(chat, "Префикс ЗНП:", Keyboards.keyboard(kb_rows))
            return

        # --- handler: znp_prefix / znp_manual ---
        if step in ("znp_prefix", "znp_manual"):
            curr, prev, valid = get_znp_prefixes_now()
            valid_prefixes = [f"D{curr}", f"L{curr}", f"D{prev}", f"L{prev}"]
            if step == "znp_prefix":
                if text in valid_prefixes:
                    data["znp_prefix"] = text
                    tg_send(chat, f"Последние 4 цифры ЗНП для <b>{text}</b>-XXXX:", Keyboards.cancel_kb())
                    return
                if text == "Другое":
                    state["step"] = "znp_manual"
                    tg_send(chat, "Введите полный ЗНП (например, D1225-1234):", Keyboards.cancel_kb())
                    return
                # maybe user typed 4 digits immediately if prefix already present
                if text.isdigit() and len(text) == 4 and "znp_prefix" in data:
                    data["znp"] = f"{data['znp_prefix']}-{text}"
                    state["step"] = "meters"
                    tg_send(chat, "Сколько метров брака?", Keyboards.cancel_kb())
                    return
                kb_rows = [[f"D{curr}", f"L{curr}"], [f"D{prev}", f"L{prev}"], ["Другое", "Отмена"]]
                tg_send(chat, "Выберите префикс:", Keyboards.keyboard(kb_rows))
                return
            else:  # znp_manual
                # Expected format like D1225-1234 (len 10 with '-' at pos 5)
                if len(text) == 10 and text[5] == "-" and text[:5].upper() in valid_prefixes:
                    data["znp"] = text.upper()
                    state["step"] = "meters"
                    tg_send(chat, "Сколько метров брака?", Keyboards.cancel_kb())
                    return
                tg_send(chat, "Неправильный формат ЗНП.\nПример: <code>D1225-1234</code>", Keyboards.cancel_kb())
                return

        # --- handler: meters ---
        if step == "meters":
            if not (text.isdigit() and int(text) > 0):
                tg_send(chat, "Укажите количество метров брака (число > 0):", Keyboards.cancel_kb())
                return
            data["meters"] = text
            state["step"] = "defect_type"
            tg_send(chat, "Вид брака:", self.get_defect_kb())
            return

        # --- handler: defect_type / defect_custom ---
        if step in ("defect_type", "defect_custom"):
            if text == "Другое" and step == "defect_type":
                state["step"] = "defect_custom"
                tg_send(chat, "Опишите вид брака:", Keyboards.cancel_kb())
                return
            data["defect_type"] = "" if text == "Без брака" else text
            data["user"] = user_repr
            data["flow"] = flow
            # build row for sheet and append
            if flow == "defect":
                row = [data["date"], data["time"], data["line"], "брак",
                       data.get("znp", ""), data["meters"], data.get("defect_type", ""), data["user"], now_msk_str(), ""]
                # Note: keep columns consistent with HEADERS_STARTSTOP etc. If original used other column positions, adapt accordingly.
            else:
                row = [data["date"], data["time"], data["line"], data.get("action", ""),
                       data.get("reason", ""), data.get("znp", ""), data.get("meters", ""),
                       data.get("defect_type", ""), data["user"], now_msk_str(), ""]
            # Append and notify controllers
            self.sc.append_record(flow, row)
            if flow == "defect":
                controllers = self.sc.get_controllers(CTRL_DEFECT_SHEET)
                msg = (f"НОВАЯ ЗАПИСЬ БРАКА\nЛиния: {data['line']}\n{data['date']} {data['time']}\n"
                       f"ЗНП: <code>{data.get('znp','—')}</code>\nМетров брака: {data.get('meters','—')}\n"
                       f"Вид брака: {data.get('defect_type','—')}")
                for cid in controllers:
                    try:
                        tg_send(cid, msg)
                    except Exception:
                        pass
            else:
                controllers = self.sc.get_controllers(CTRL_STARTSTOP_SHEET)
                action_ru = "Запуск" if data.get("action") == "запуск" else "Остановка"
                msg = (f"НОВАЯ ЗАПИСЬ СТАРТ/СТОП\nЛиния: {data['line']}\n{data['date']} {data['time']}\n"
                       f"Действие: {action_ru}\nПричина: {data.get('reason','—')}")
                for cid in controllers:
                    try:
                        tg_send(cid, msg)
                    except Exception:
                        pass
            tg_send(chat, f"<b>Записано!</b>\nЛиния {data['line']} • {data['date']} {data['time']}", Keyboards.main_kb())
            self.clear_state(uid)
            return

        tg_send(chat, "Выберите действие:", Keyboards.flow_menu_kb())


# ==================== Background controllers refresh worker ====================
class ControllersRefresher:
    def __init__(self, sheet_client: SheetClient, interval_min: int = 1440):
        self.sc = sheet_client
        self.interval_min = interval_min
        self._stop = False
        threading.Thread(target=self._worker, daemon=True).start()

    def _worker(self):
        while not self._stop:
            try:
                # simply invalidate controllers-related caches by invalidating sheet caches
                self.sc.invalidate_cache(CTRL_STARTSTOP_SHEET)
                self.sc.invalidate_cache(CTRL_DEFECT_SHEET)
                # optionally, you can pre-fetch to warm cache:
                _ = self.sc.get_controllers(CTRL_STARTSTOP_SHEET)
                _ = self.sc.get_controllers(CTRL_DEFECT_SHEET)
                log.info("Controllers refreshed")
            except Exception:
                log.exception("Error refreshing controllers")
            time.sleep(self.interval_min * 60)

    def stop(self):
        self._stop = True


# ==================== Application wiring ====================
sheet_client = SheetClient(sh, cache_ttl=5)
auth_manager = AuthManager(sheet_client)
fsm = FSM(sheet_client, auth_manager)
controllers_refresher = ControllersRefresher(sheet_client, interval_min=1440)  # once per day

# Pre-create commonly used keyboards
MAIN_KB = Keyboards.main_kb()
FLOW_MENU_KB = Keyboards.flow_menu_kb()
CANCEL_KB = Keyboards.cancel_kb()
CONFIRM_KB = Keyboards.confirm_kb()
NUM_LINE_KB = Keyboards.num_line_kb()

# ==================== Flask webhook ====================
app = Flask(__name__)
LOCK_PATH = "/tmp/bot.lock"

if os.getenv("RENDER"):
    token = os.getenv("TELEGRAM_TOKEN")
    domain = os.getenv("RENDER_EXTERNAL_HOSTNAME")
    if token and domain:
        url = f"https://{domain}/"
        try:
            requests.get(f"https://api.telegram.org/bot{token}/setWebhook?url={url}")
            print(f"Вебхук установлен: {url}")
        except Exception:
            log.exception("Failed to set webhook")

@app.route("/", methods=["POST"])
def webhook():
    update = request.get_json()
    if not update:
        return "ok", 200
    if "message" not in update:
        return "ok", 200
    m = update["message"]
    chat_id = m["chat"]["id"]
    user_id = m["from"]["id"]
    text = (m.get("text") or "").strip()
    username = m["from"].get("username", "")
    user_repr = f"{user_id} (@{username or 'no_user'})"

    # Use file lock to avoid concurrent writes when multiple webhook workers hit same process
    with FileLock(LOCK_PATH):
        try:
            fsm.handle_text(user_id, chat_id, text, user_repr)
        except Exception as e:
            log.exception("processing error: %s", e)
    return "ok", 200

@app.route("/ping")
def index():
    return "Bot is running!", 200

# ==================== Run server ====================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
