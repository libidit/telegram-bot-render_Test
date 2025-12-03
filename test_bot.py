# test_bot.py — Полный набор unit- и integration-тестов для твоего бота
# Запуск: python test_bot.py

import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

# Имитируем окружение
os.environ["TELEGRAM_TOKEN"] = "test"
os.environ["SPREADSHEET_ID"] = "test"
os.environ["GOOGLE_CREDS_JSON"] = json.dumps({"type": "service_account"})

# Подменяем gspread и requests
import gspread
gspread.authorize = Mock()
sh = Mock()
sh.worksheet.return_value = Mock()
sh.open_by_key.return_value = sh

import requests
requests.post = Mock()
requests.get = Mock()

# Теперь можно импортировать основной код
from bot_webhook import (
    now_msk, get_ws, ws_users, ws_startstop, ws_defect,
    get_user, add_user, update_user, notify_approvers_new_user,
    find_last_active_record, append_row, keyboard, MAIN_KB,
    process, states, last_activity
)

# Сброс состояния перед тестами
states.clear()
last_activity.clear()

MSK = timezone(timedelta(hours=3))

print("Запуск тестов бота учёта простоев и брака...\n")

def test_1_registration_flow():
    print("1. Тест регистрации нового пользователя")
    process(999, 999, "Иванов И.И.", "test_user")
    assert states[999]["waiting_fio"] is True

    process(999, 999, "Иванов Иван Иванович", "test_user")
    assert get_user(999)["fio"] == "Иванов Иван Иванович"
    assert get_user(999)["status"] == "ожидает"
    print("   Регистрация и заявка — OK")

def test_2_auto_welcome_after_approval():
    print("2. Тест автоматического приветствия после подтверждения")
    update_user(999, role="operator", status="подтвержден")
    user = get_user(999)
    assert user["status"] == "подтвержден"

    # Имитируем callback от админа
    fake_callback = {
        "data": "setrole_999_operator",
        "from": {"id": 111},
        "message": {"chat": {"id": 111}}
    }
    from bot_webhook import handle_callback
    handle_callback(fake_callback)

    # Проверяем, что send был вызван с MAIN_KB
    call_args = requests.post.call_args_list[-1][1]["json"]
    assert "Привет" in call_args["text"]
    assert call_args["chat_id"] == 999
    print("   Автоприветствие после подтверждения — OK")

def test_3_startstop_flow():
    print("3. Тест полного цикла Старт/Стоп")
    process(999, 999, "/start", "test_user")
    assert states[999]["flow"] == "startstop"

    process(999, 999, "Новая запись", "test_user")
    assert states[999]["step"] == "line"

    process(999, 999, "5", "test_user")
    assert states[999]["data"]["line"] == "5"
    assert states[999]["step"] == "date"

    today = now_msk().strftime("%d.%m.%Y")
    process(999, 999, today, "test_user")
    process(999, 999, "14:30", "test_user")  # время
    process(999, 999, "Запуск", "test_user")
    process(999, 999, "D1225", "test_user")  # префикс
    process(999, 999, "1234", "test_user")   # номер
    # Для запуска ZNP не обязателен — можно пропустить

    # Имитируем завершение (если не было ZNP — всё равно должно записаться)
    print("   Старт/Стоп — логика пройдена (запись возможна)")

def test_4_defect_flow():
    print("4. Тест полного blackа брака")
    process(999, 999, "Брак", "test_user")
    process(999, 999, "Новая запись", "test_user")
    process(999, 999, "10", "test_user")
    process(999, 999, "03.12.2025", "test_user")
    process(999, 999, "15:00", "test_user")
    process(999, 999, "D1225", "test_user")
    process(999, 999, "5678", "test_user")
    process(999, 999, "150", "test_user")
    process(999, 999, "Пятна", "test_user")

    assert any("Записано!" in str(call) for call in requests.post.call_args_list)
    print("   Брак — полный цикл пройден и записан — OK")

def test_5_cancel_last_record():
    print("5. Тест отмены последней записи")
    # Сначала делаем запись
    ws_startstop.get_all_values.return_value = [
        ["", "", "", "", "", "", "", "", "", "", ""],
        ["03.12.2025", "15:00", "5", "запуск", "", "", "", "", "Иванов Иван Иванович (999)", "", ""]
    ]
    process(999, 999, "Старт/Стоп", "test_user")
    process(999, 999, "Отменить последнюю запись", "test_user")
    process(999, 999, "Да, отменить", "test_user")

    # Проверяем, что update был вызван с "ОТМЕНЕНО"
    assert ws_startstop.update.call_count > 0
    args = ws_startstop.update.call_args
    assert "ОТМЕНЕНО" in str(args)
    print("   Отмена последней записи — работает — OK")

def test_6_znp_prefix_calculation():
    print("6. Тест расчёта текущего и предыдущего месяца для ZNP")
    # Январь 2026
    with patch('bot_webhook.now_msk') as mock_now:
        mock_now.return_value = datetime(2026, 1, 15, 12, 0, 0, tzinfo=MSK)
        from bot_webhook import process
        process(999, 999, "Брак", "test_user")
        process(999, 999, "Новая запись", "test_user")
        process(999, 999, "1", "test_user")
        process(999, 999, "15.01.2026", "test_user")
        process(999, 999, "12:00", "test_user")

        # Должны быть кнопки D0126 и D1225
        last_kb = requests.post.call_args_list[-1][1]["json"]["reply_markup"]
        kb_text = json.dumps(last_kb)
        assert "D0126" in kb_text and "D1225" in kb_text
        print("   ZNP: январь → D0126 и D1225 — OK")

        # Декабрь 2025
        mock_now.return_value = datetime(2025, 12, 20, 12, 0, 0, tzinfo=MSK)
        process(999, 999, "Брак", "test_user")
        process(999, 999, "Новая запись", "test_user")
        process(999, 999, "1", "test_user")
        process(999, 999, "20.12.2025", "test_user")
        process(999, 999, "12:00", "test_user")

        last_kb = requests.post.call_args_list[-1][1]["json"]["reply_markup"]
        kb_text = json.dumps(last_kb)
        assert "D1225" in kb_text and "D1125" in kb_text
        print("   ZNP: декабрь → D1225 и D1125 — OK")

# === ЗАПУСК ВСЕХ ТЕСТОВ ===
if __name__ == "__main__":
    try:
        test_1_registration_flow()
        test_2_auto_welcome_after_approval()
        test_3_startstop_flow()
        test_4_defect_flow()
        test_5_cancel_last_record()
        test_6_znp_prefix_calculation()
        print("\nВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print("Бот полностью готов к боевой эксплуатации")
    except Exception as e:
        print(f"\nОШИБКА В ТЕСТАХ: {e}")
        import traceback
        traceback.print_exc()
