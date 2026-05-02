import json
import os
import re
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Dict, Any, Tuple

import aiohttp
from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, Message, CallbackQuery
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = int(os.getenv("API_ID", ""))
API_HASH = os.getenv("API_HASH", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "123")
DADATA_API_KEY = os.getenv("DADATA_API_KEY", "")
DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY", "")
DATA_FILE = "data.json"
HASH_FILE = DATA_FILE + ".hash"
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", "1095"))

app = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_states: Dict[str, Dict[str, Any]] = {}


def compute_file_hash(filepath: str) -> Optional[str]:
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except Exception:
        return None


def verify_db_integrity(data_file: str, hash_file: str) -> bool:
    current_hash = compute_file_hash(data_file)
    if current_hash is None:
        return True
    if not os.path.exists(hash_file):
        return True
    try:
        with open(hash_file, "r") as f:
            stored_hash = f.read().strip()
        return current_hash == stored_hash
    except Exception:
        return False


def save_hash(data_file: str, hash_file: str):
    h = compute_file_hash(data_file)
    if h:
        with open(hash_file, "w") as f:
            f.write(h)


def get_db() -> Dict[str, Any]:
    if not os.path.exists(DATA_FILE):
        return {"users": {}, "orders": {}, "consents": {}}
    if not verify_db_integrity(DATA_FILE, HASH_FILE):
        return {"users": {}, "orders": {}, "consents": {}}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for key in ["users", "orders", "consents"]:
            data.setdefault(key, {})
        return data
    except Exception:
        return {"users": {}, "orders": {}, "consents": {}}


def save_db(data: Dict[str, Any]) -> bool:
    try:
        temp = f"{DATA_FILE}.tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(temp, DATA_FILE)
        save_hash(DATA_FILE, HASH_FILE)
        return True
    except Exception:
        return False


def hash_sensitive(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def is_likely_city(text: str) -> bool:
    return bool(text and text.strip() and re.search(r'[а-яё]', text, re.IGNORECASE))


async def validate_city(city_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not is_likely_city(city_name):
        return None, "Название города должно содержать русские буквы."
    if not DADATA_API_KEY or DADATA_API_KEY == "ВАШ_API_KEY":
        return city_name.strip().title(), None
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address"
    headers = {"Authorization": f"Token {DADATA_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "query": city_name, "count": 1, "locations": [{"country_iso_code": "RU"}],
        "from_bound": {"value": "city"}, "to_bound": {"value": "city"}
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("suggestions"):
                        city_data = data["suggestions"][0].get("data", {})
                        normalized = city_data.get("city") or city_data.get("settlement")
                        if normalized:
                            return normalized, None
                    return None, f"Город '{city_name}' не найден."
                return None, f"Ошибка DaData ({resp.status})."
    except Exception:
        return None, "Сервис проверки городов временно недоступен."


def get_role_keyboard(role: str) -> Optional[ReplyKeyboardMarkup]:
    keyboards = {
        "Клиент": [["Заказать вынос", "Мои заказы"], ["Мои данные", "Сменить город", "Сменить роль"],
                   ["Удалить аккаунт"]],
        "Выноситель": [["Найти заказ", "Завершить заказ"], ["Мои данные", "Сменить город", "Сменить роль"],
                       ["Удалить аккаунт"]],
        "Админ": [["Статистика", "Управление пользователями"], ["Журнал аудита"]]
    }
    kb = keyboards.get(role)
    return ReplyKeyboardMarkup(kb, resize_keyboard=True) if kb else None


def consent_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Я согласен на обработку ПДн", callback_data="consent_accept")],
        [InlineKeyboardButton("📄 Политика конфиденциальности", url="https://yoursite.ru/privacy")]
    ])


def reg_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Клиент", callback_data="reg_Клиент")],
        [InlineKeyboardButton("Выноситель", callback_data="reg_Выноситель")],
        [InlineKeyboardButton("Админ", callback_data="reg_Админ_needpass")]
    ])


def role_switch_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Клиент", callback_data="switch_Клиент")],
        [InlineKeyboardButton("Выноситель", callback_data="switch_Выноситель")],
        [InlineKeyboardButton("Админ", callback_data="switch_Админ_needpass")]
    ])


def require_registered(func):
    @wraps(func)
    async def wrapper(client, message):
        uid = str(message.from_user.id)
        user = get_db()["users"].get(uid)
        if not user or not user.get("consent_given"):
            await message.reply(
                "Для работы с ботом необходимо принять условия обработки персональных данных.\n"
                "Нажмите /start для начала.",
                reply_markup=reg_kb()
            )
            return
        return await func(client, message)

    return wrapper


async def ask_city(client: Client, uid: str, role: str, is_role_change: bool = False):
    user_states[uid] = {"state": "waiting_for_city", "role": role, "is_role_change": is_role_change}
    await client.send_message(uid, "Введите ваш город (например: Москва, Новосибирск):")


async def ask_order_details(client: Client, uid: str, city: str, address: str):
    user_states[uid] = {"state": "waiting_for_details", "city": city, "address": address}
    await client.send_message(
        uid,
        "Введите описание заказа (этаж, код, вес) или нажмите 'Пропустить':",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="skip_details")]])
    )


async def create_order_final(client: Client, uid: str, city: str, address: str, details: Optional[str] = None):
    db = get_db()
    order_id = f"ORD-{int(datetime.now().timestamp())}"
    db["orders"][order_id] = {
        "client_id": uid, "city": city, "address": address, "details": details,
        "status": "pending", "remover_id": None,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "created_at": datetime.now().isoformat()
    }
    if save_db(db):
        await client.send_message(uid, f"Заказ {order_id} создан! Ищем выносителя...",
                                  reply_markup=get_role_keyboard("Клиент"))
    else:
        await client.send_message(uid, "Ошибка создания заказа.", reply_markup=get_role_keyboard("Клиент"))


@app.on_message(filters.command("start") & filters.private)
async def start(client: Client, message: Message):
    uid = str(message.from_user.id)
    # Сбрасываем состояние пользователя, чтобы избежать конфликтов
    user_states.pop(uid, None)

    db = get_db()
    user = db["users"].get(uid)

    if user and user.get("consent_given"):
        role = user.get("role", "Клиент")
        if role not in ["Клиент", "Выноситель", "Админ"]:
            role = "Клиент"
            db["users"][uid]["role"] = role
            save_db(db)
        await message.reply(f"С возвращением! Вы вошли как {role}.", reply_markup=get_role_keyboard(role))
    else:
        await message.reply(
            "Привет! Для использования сервиса необходимо согласие на обработку персональных данных.\n"
            "Мы собираем: имя, ID Telegram, город, историю заказов.\n"
            "Данные хранятся на серверах в РФ и **не передаются** третьим лицам, кроме сервиса проверки адресов (DaData).\n"
            "Нажимая «Согласен», вы подтверждаете, что ознакомлены и согласны с передачей вашего города в DaData.\n"
            "Вы можете отозвать согласие в любой момент командой /withdraw_consent.",
            reply_markup=consent_kb()
        )


@app.on_callback_query(filters.regex(r"^consent_accept$"))
async def handle_consent(client: Client, callback_query: CallbackQuery):
    uid = str(callback_query.from_user.id)
    db = get_db()
    timestamp = datetime.now().isoformat()

    if uid not in db["users"]:
        db["users"][uid] = {
            "name": callback_query.from_user.first_name,
            "username": callback_query.from_user.username,
            "registered_at": timestamp
        }
    db["users"][uid]["consent_given"] = True
    db["users"][uid]["consent_timestamp"] = timestamp
    db["consents"][uid] = {"given_at": timestamp, "ip_hash": hash_sensitive(str(callback_query.from_user.id)),
                           "version": "1.0"}
    save_db(db)
    await callback_query.message.edit_text("Спасибо! Теперь выберите вашу роль:", reply_markup=reg_kb())
    await callback_query.answer()


@app.on_callback_query(filters.regex(r"^reg_"))
async def handle_registration(client: Client, callback_query: CallbackQuery):
    uid = str(callback_query.from_user.id)
    db = get_db()
    user = db["users"].get(uid)

    if not user or not user.get("consent_given"):
        await callback_query.answer("Сначала примите условия соглашения", show_alert=True)
        return

    data = callback_query.data
    if data == "reg_Админ_needpass":
        user_states[uid] = {"state": "waiting_admin_pass"}
        await callback_query.message.edit_text("Введите пароль администратора:")
        await callback_query.answer()
        return

    role = data.split("_", 1)[1]
    if user.get("role") == role and user.get("city"):
        await callback_query.message.edit_text(f"Вы уже зарегистрированы как {role}.",
                                               reply_markup=get_role_keyboard(role))
        return

    await callback_query.message.delete()
    if user.get("city"):
        db["users"][uid]["role"] = role
        save_db(db)
        await client.send_message(uid, f"Роль изменена на {role}", reply_markup=get_role_keyboard(role))
    else:
        await ask_city(client, uid, role, is_role_change=bool(user.get("role")))
    await callback_query.answer()


@app.on_message(filters.text & filters.private & ~filters.command(
    ["start", "cancel", "mydata", "delete_account", "withdraw_consent"]))
@require_registered
async def handle_text(client: Client, message: Message):
    uid = str(message.from_user.id)
    if uid in user_states:
        await handle_state_input(client, message)
        return

    db = get_db()
    role = db["users"][uid].get("role", "Клиент")
    text = message.text.strip()

    if role == "Клиент":
        if text == "Заказать вынос":
            city = db["users"][uid].get("city")
            if city:
                await message.reply(
                    f"Ваш город: {city}. Использовать?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"{city} (оставить)", callback_data="order_city_keep")],
                        [InlineKeyboardButton("Ввести другой", callback_data="order_city_change")]
                    ])
                )
            else:
                await ask_city(client, uid, role, is_role_change=False)
        elif text == "Мои заказы":
            await show_my_orders(client, message)
        elif text == "Сменить город":
            await ask_city(client, uid, role, is_role_change=False)
        elif text == "Сменить роль":
            await message.reply("Выберите новую роль:", reply_markup=role_switch_kb())
        elif text == "Мои данные":
            await show_my_data(client, message)
        elif text == "Удалить аккаунт":
            await request_delete_account(client, message)
        else:
            await message.reply("Используйте кнопки меню.", reply_markup=get_role_keyboard(role))

    elif role == "Выноситель":
        if text == "Найти заказ":
            await find_orders(client, message)
        elif text == "Завершить заказ":
            await finish_order(client, message)
        elif text == "Сменить город":
            await ask_city(client, uid, role, is_role_change=False)
        elif text == "Сменить роль":
            await message.reply("Выберите новую роль:", reply_markup=role_switch_kb())
        elif text == "Мои данные":
            await show_my_data(client, message)
        elif text == "Удалить аккаунт":
            await request_delete_account(client, message)
        else:
            await message.reply("Используйте кнопки меню.", reply_markup=get_role_keyboard(role))

    elif role == "Админ":
        if text == "Статистика":
            await admin_stats(client, message)
        elif text == "Управление пользователями":
            await list_users_for_admin(client, message)
        elif text == "Журнал аудита":
            await show_audit_log(client, message)
        else:
            await message.reply("Используйте кнопки меню.", reply_markup=get_role_keyboard(role))


async def handle_state_input(client: Client, message: Message):
    uid = str(message.from_user.id)
    state = user_states.get(uid)
    if not state:
        return

    if state["state"] == "waiting_admin_pass":
        if message.text.strip() == ADMIN_PASSWORD:
            db = get_db()
            db["users"][uid]["role"] = "Админ"
            save_db(db)
            await message.reply("✅ Пароль верен. Вы назначены администратором.",
                                reply_markup=get_role_keyboard("Админ"))
            user_states.pop(uid, None)
        else:
            await message.reply("❌ Неверный пароль. Попробуйте ещё раз или нажмите /cancel.")
        return

    if state["state"] == "waiting_for_city":
        city_input = message.text.strip()
        msg = await message.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await msg.delete()

        if error:
            await message.reply(error)
            return

        db = get_db()
        new_role = state["role"]
        is_change = state.get("is_role_change", False)

        if uid not in db["users"]:
            db["users"][uid] = {
                "name": message.from_user.first_name,
                "role": new_role,
                "city": city,
                "consent_given": True,
                "consent_timestamp": datetime.now().isoformat()
            }
            save_db(db)
            await message.reply(f"Регистрация завершена! Роль: {new_role}, Город: {city}",
                                reply_markup=get_role_keyboard(new_role))
        else:
            db["users"][uid]["city"] = city
            # Если у пользователя ещё нет роли, устанавливаем её
            if "role" not in db["users"][uid] or db["users"][uid]["role"] is None:
                db["users"][uid]["role"] = new_role
            elif is_change:
                db["users"][uid]["role"] = new_role
            save_db(db)
            current_role = db["users"][uid].get("role", "Клиент")
            await message.reply(f"Город изменён на {city}", reply_markup=get_role_keyboard(current_role))
        user_states.pop(uid, None)
        return

    if state["state"] == "order_city_new":
        city_input = message.text.strip()
        msg = await message.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await msg.delete()
        if error:
            await message.reply(error)
            return
        user_states[uid] = {"state": "waiting_for_address", "city": city}
        await message.reply(f"Город: {city}\nВведите адрес (улица, дом):")
        return

    if state["state"] == "waiting_for_address":
        addr = message.text.strip()
        if not addr:
            return await message.reply("Адрес не может быть пустым.")
        city = state.get("city")
        if city:
            await ask_order_details(client, uid, city, addr)
        else:
            await message.reply("Ошибка: не указан город. Начните заказ заново.",
                                reply_markup=get_role_keyboard("Клиент"))
        user_states.pop(uid, None)
        return

    if state["state"] == "waiting_for_details":
        details = message.text.strip() or None
        city = state.get("city")
        address = state.get("address")
        await create_order_final(client, uid, city, address, details)
        user_states.pop(uid, None)
        return

    user_states.pop(uid, None)
    await message.reply("Сбой состояния. Начните с /start")


async def show_my_orders(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    text = "Ваши заказы:\n\n"
    has_orders = False
    status_map = {
        "pending": "Ожидает", "awaiting_confirm": "Ожидает подтверждения",
        "active": "В работе", "waiting_complete": "Ожидает подтверждения выполнения", "done": "Выполнен"
    }
    for oid, order in db["orders"].items():
        if order["client_id"] == uid:
            has_orders = True
            details = f"\nОписание: {order.get('details', 'Нет')}" if order.get('details') else ""
            text += f"Заказ {oid}\nГород: {order.get('city')}\nАдрес: {order.get('address')}{details}\nСтатус: {status_map.get(order['status'], 'Неизвестно')}\n---\n"
    await message.reply(text if has_orders else "У вас пока нет заказов.", reply_markup=get_role_keyboard("Клиент"))


async def show_my_data(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    user = db["users"].get(uid, {})
    orders_count = sum(1 for o in db["orders"].values() if o.get("client_id") == uid or o.get("remover_id") == uid)

    text = (
        f"📋 Ваши данные:\n"
        f"ID: {uid}\n"
        f"Имя: {user.get('name')}\n"
        f"Роль: {user.get('role')}\n"
        f"Город: {user.get('city', 'Не указан')}\n"
        f"Согласие дано: {user.get('consent_timestamp', 'Нет')}\n"
        f"Кол-во заказов: {orders_count}\n"
    )
    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton("🗑 Запросить удаление всех данных", callback_data="req_delete_all")]])
    await message.reply(text, reply_markup=kb)


async def request_delete_account(client: Client, message: Message):
    uid = str(message.from_user.id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ Да, удалить безвозвратно", callback_data="confirm_delete_account")],
        [InlineKeyboardButton("Отмена", callback_data="cancel_action")]
    ])
    await message.reply("Вы уверены? Это действие необратимо. Все ваши данные и история заказов будут удалены.",
                        reply_markup=kb)


async def find_orders(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    user_city = db["users"][uid].get("city")
    if not user_city:
        return await message.reply("Сначала укажите город через 'Сменить город'.",
                                   reply_markup=get_role_keyboard("Выноситель"))

    for order in db["orders"].values():
        if order.get("remover_id") == uid and order.get("status") == "active":
            return await message.reply("Сначала завершите текущий заказ!", reply_markup=get_role_keyboard("Выноситель"))

    kb = InlineKeyboardMarkup([])
    found = False
    for oid, order in db["orders"].items():
        if order.get("status") == "pending" and order.get("city") == user_city:
            found = True
            label = f"{oid} | {order.get('address', 'без адреса')}"
            if order.get("details"):
                label += f" | {order['details'][:20]}..."
            kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"take_{oid}")])

    if not found:
        await message.reply(f"Свободных заказов в {user_city} нет.", reply_markup=get_role_keyboard("Выноситель"))
    else:
        kb.inline_keyboard.append([InlineKeyboardButton("Назад", callback_data="back_remover")])
        await message.reply(f"Доступные заказы в {user_city}:", reply_markup=kb)


async def finish_order(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    active_oid = None
    for oid, order in db["orders"].items():
        if order.get("remover_id") == uid and order.get("status") == "active":
            active_oid = oid
            break
    if not active_oid:
        return await message.reply("Нет активных заказов.", reply_markup=get_role_keyboard("Выноситель"))

    db["orders"][active_oid]["status"] = "waiting_complete"
    save_db(db)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Подтвердить выполнение", callback_data=f"complete_yes_{active_oid}")],
        [InlineKeyboardButton("❌ Отклонить (заказ продолжается)", callback_data=f"complete_no_{active_oid}")]
    ])
    try:
        remover_name = db["users"][uid]["name"]
        await client.send_message(
            db["orders"][active_oid]["client_id"],
            f"Выноситель {remover_name} отметил заказ {active_oid} как выполненный.\nПодтверждаете?",
            reply_markup=kb
        )
        await message.reply(f"Запрос на подтверждение выполнения заказа {active_oid} отправлен клиенту.",
                            reply_markup=get_role_keyboard("Выноситель"))
    except Exception:
        db["orders"][active_oid]["status"] = "active"
        save_db(db)
        await message.reply("Не удалось связаться с клиентом. Заказ остаётся активным.",
                            reply_markup=get_role_keyboard("Выноситель"))


async def admin_stats(client: Client, message: Message):
    db = get_db()
    statuses = ["pending", "awaiting_confirm", "active", "waiting_complete", "done"]
    stats = {s: sum(1 for o in db["orders"].values() if o["status"] == s) for s in statuses}
    text = (
        f"📊 Статистика\n"
        f"Пользователей: {len(db['users'])}\n"
        f"Ожидают: {stats['pending']}\n"
        f"Ждут подтверждения: {stats['awaiting_confirm']}\n"
        f"В работе: {stats['active']}\n"
        f"Ждут подтверждения выполнения: {stats['waiting_complete']}\n"
        f"Выполнено: {stats['done']}"
    )
    await message.reply(text, reply_markup=get_role_keyboard("Админ"))


async def list_users_for_admin(client: Client, message: Message):
    db = get_db()
    admin_uid = str(message.from_user.id)
    users = {uid: data for uid, data in db["users"].items() if uid != admin_uid}
    if not users:
        await message.reply("Нет других пользователей.", reply_markup=get_role_keyboard("Админ"))
        return
    kb = InlineKeyboardMarkup([])
    for uid, user in users.items():
        name = user.get("name", "Без имени")
        username = f" @{user.get('username')}" if user.get('username') else ""
        label = f"{name}{username} [{user.get('role', '')}]"
        kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"admin_user_{uid}")])
    await message.reply("Управление пользователями:", reply_markup=kb)


async def show_audit_log(client: Client, message: Message):
    await message.reply("Журнал аудита отключён.", reply_markup=get_role_keyboard("Админ"))


@app.on_callback_query()
async def handle_callbacks(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    uid = str(callback_query.from_user.id)
    db = get_db()

    if data.startswith("switch_"):
        role_target = data.split("_", 1)[1]
        if role_target == "Админ_needpass":
            user_states[uid] = {"state": "waiting_admin_pass"}
            await callback_query.message.edit_text("Введите пароль администратора:")
            await callback_query.answer()
            return
        if db["users"].get(uid, {}).get("role") == role_target:
            await callback_query.answer(f"Вы уже {role_target}", show_alert=True)
            return
        db["users"][uid]["role"] = role_target
        save_db(db)
        await callback_query.message.edit_text(f"Ваша роль изменена на {role_target}.")
        await callback_query.message.reply(f"Новое меню для {role_target}:",
                                           reply_markup=get_role_keyboard(role_target))
        await callback_query.answer()
        return

    if data == "order_city_keep":
        city = db["users"][uid].get("city")
        if city:
            user_states[uid] = {"state": "waiting_for_address", "city": city}
            await callback_query.message.edit_text("Введите адрес (улица, дом):")
        await callback_query.answer()

    elif data == "order_city_change":
        user_states[uid] = {"state": "order_city_new"}
        await callback_query.message.edit_text("Введите город для заказа:")
        await callback_query.answer()

    elif data.startswith("take_"):
        oid = data.split("_", 1)[1]
        order = db["orders"].get(oid)
        if not order or order["status"] != "pending":
            return await callback_query.answer("Заказ недоступен", show_alert=True)
        if db["users"][uid].get("city") != order.get("city"):
            return await callback_query.answer("Не ваш город", show_alert=True)

        order["status"] = "awaiting_confirm"
        order["remover_id"] = uid
        save_db(db)
        details_text = f"\nОписание: {order['details']}" if order.get('details') else ""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_yes_{oid}")],
            [InlineKeyboardButton("❌ Отказать", callback_data=f"confirm_no_{oid}")]
        ])
        try:
            remover_name = db["users"][uid]["name"]
            await client.send_message(
                order["client_id"],
                f"Выноситель {remover_name} хочет взять ваш заказ {oid}.\nАдрес: {order['address']}{details_text}\n\nПодтверждаете?",
                reply_markup=kb
            )
            await callback_query.message.delete()
            await callback_query.message.reply("Запрос отправлен клиенту.",
                                               reply_markup=get_role_keyboard("Выноситель"))
        except Exception:
            await callback_query.answer("Ошибка отправки уведомления", show_alert=True)

    elif data.startswith("confirm_yes_") or data.startswith("confirm_no_"):
        oid = data.split("_", 2)[2]
        approved = data.startswith("confirm_yes_")
        await process_confirmation(client, callback_query, uid, oid, approved)

    elif data.startswith("complete_yes_") or data.startswith("complete_no_"):
        oid = data.split("_", 2)[2]
        approved = data.startswith("complete_yes_")
        await process_completion_confirmation(client, callback_query, uid, oid, approved)

    elif data == "skip_details":
        state = user_states.get(uid)
        if state and state.get("state") == "waiting_for_details":
            await create_order_final(client, uid, state.get("city"), state.get("address"), details=None)
            user_states.pop(uid, None)
            try:
                await callback_query.message.delete()
            except:
                pass
        await callback_query.answer()

    elif data == "back_remover":
        try:
            await callback_query.message.delete()
        except:
            pass
        await callback_query.message.reply("Меню выносителя:", reply_markup=get_role_keyboard("Выноситель"))
        await callback_query.answer()

    elif data == "req_delete_all":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚠️ Да, удалить всё", callback_data="confirm_delete_account")],
            [InlineKeyboardButton("Отмена", callback_data="cancel_action")]
        ])
        await callback_query.message.reply("Подтвердите удаление всех ваших данных:", reply_markup=kb)
        await callback_query.answer()

    elif data == "confirm_delete_account":
        await process_delete_account(client, callback_query)

    elif data == "cancel_action":
        await callback_query.message.edit_text("Действие отменено.")
        await callback_query.answer()

    elif data.startswith("admin_user_"):
        target_uid = data.split("_", 2)[2]
        target_user = db["users"].get(target_uid)
        if not target_user:
            return await callback_query.answer("Пользователь не найден", show_alert=True)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Сменить роль", callback_data=f"admin_change_role_{target_uid}")],
            [InlineKeyboardButton("🗑 Удалить пользователя", callback_data=f"admin_del_{target_uid}")]
        ])
        info = (
            f"Пользователь: {target_user.get('name')}\n"
            f"Роль: {target_user.get('role')}\n"
            f"Город: {target_user.get('city')}\n"
            f"Согласие: {target_user.get('consent_given')}"
        )
        await callback_query.message.edit_text(info, reply_markup=kb)
        await callback_query.answer()

    elif data.startswith("admin_change_role_"):
        target_uid = data.split("_", 3)[3]
        target_user = db["users"].get(target_uid)
        if not target_user:
            return await callback_query.answer("Пользователь не найден", show_alert=True)
        role_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👤 Клиент", callback_data=f"admin_set_role_{target_uid}_Клиент")],
            [InlineKeyboardButton("🛠 Выноситель", callback_data=f"admin_set_role_{target_uid}_Выноситель")],
            [InlineKeyboardButton("👑 Админ", callback_data=f"admin_set_role_{target_uid}_Админ")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"admin_user_{target_uid}")]
        ])
        await callback_query.message.edit_text(f"Выберите новую роль для {target_user.get('name')}:",
                                               reply_markup=role_kb)
        await callback_query.answer()

    elif data.startswith("admin_set_role_"):
        parts = data.split("_")
        if len(parts) < 5:
            return await callback_query.answer("Ошибка формата", show_alert=True)
        target_uid = parts[3]
        new_role = "_".join(parts[4:])
        if new_role not in ["Клиент", "Выноситель", "Админ"]:
            return await callback_query.answer("Недопустимая роль", show_alert=True)
        db = get_db()
        if target_uid not in db["users"]:
            return await callback_query.answer("Пользователь не найден", show_alert=True)
        db["users"][target_uid]["role"] = new_role
        save_db(db)
        await callback_query.message.edit_text(f"Роль пользователя изменена на {new_role}.")
        target_user = db["users"].get(target_uid)
        if target_user:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Сменить роль", callback_data=f"admin_change_role_{target_uid}")],
                [InlineKeyboardButton("🗑 Удалить пользователя", callback_data=f"admin_del_{target_uid}")]
            ])
            info = (
                f"Пользователь: {target_user.get('name')}\n"
                f"Роль: {target_user.get('role')}\n"
                f"Город: {target_user.get('city')}\n"
                f"Согласие: {target_user.get('consent_given')}"
            )
            await callback_query.message.edit_text(info, reply_markup=kb)
        await callback_query.answer()

    elif data.startswith("admin_del_"):
        target_uid = data.split("_", 2)[2]
        if target_uid in db["users"]:
            for oid in list(db["orders"].keys()):
                if db["orders"][oid].get("client_id") == target_uid or db["orders"][oid].get(
                        "remover_id") == target_uid:
                    del db["orders"][oid]
            del db["users"][target_uid]
            if target_uid in db["consents"]:
                del db["consents"][target_uid]
            save_db(db)
            await callback_query.message.edit_text("Пользователь удалён.")
            await list_users_for_admin(client, callback_query.message)
        await callback_query.answer()


async def process_confirmation(client: Client, c: CallbackQuery, uid: str, oid: str, approved: bool):
    db = get_db()
    order = db["orders"].get(oid)
    if not order or order["client_id"] != uid or order["status"] != "awaiting_confirm":
        return await c.answer("Заказ уже обработан", show_alert=True)

    if approved:
        order["status"] = "active"
        save_db(db)
        await client.send_message(order["remover_id"], f"Заказ {oid} подтверждён. Работайте!")
        await c.message.edit_text(f"Заказ {oid} подтверждён. Выноситель приступит к выполнению.",
                                  reply_markup=get_role_keyboard("Клиент"))
    else:
        order["status"] = "pending"
        rem_id = order.pop("remover_id", None)
        save_db(db)
        if rem_id:
            await client.send_message(rem_id, f"Заказ {oid} отклонён клиентом.")
        await c.message.edit_text(f"Заказ {oid} отклонён. Поиск нового выносителя.",
                                  reply_markup=get_role_keyboard("Клиент"))
    await c.answer()


async def process_completion_confirmation(client: Client, c: CallbackQuery, uid: str, oid: str, approved: bool):
    db = get_db()
    order = db["orders"].get(oid)
    if not order or order["client_id"] != uid or order["status"] != "waiting_complete":
        return await c.answer("Заказ уже обработан", show_alert=True)

    if approved:
        order["status"] = "done"
        save_db(db)
        remover_id = order.get("remover_id")
        if remover_id:
            await client.send_message(remover_id, f"Клиент подтвердил выполнение заказа {oid}! Заказ завершён.",
                                      reply_markup=get_role_keyboard("Выноситель"))
        await c.message.edit_text(f"Заказ {oid} успешно выполнен! Спасибо за использование сервиса.",
                                  reply_markup=get_role_keyboard("Клиент"))
    else:
        order["status"] = "active"
        save_db(db)
        remover_id = order.get("remover_id")
        if remover_id:
            await client.send_message(remover_id,
                                      f"Клиент не подтвердил выполнение заказа {oid}. Продолжите выполнение.",
                                      reply_markup=get_role_keyboard("Выноситель"))
        await c.message.edit_text(f"Вы отказались подтвердить выполнение заказа {oid}. Заказ остаётся в работе.",
                                  reply_markup=get_role_keyboard("Клиент"))
    await c.answer()


async def process_delete_account(client: Client, callback_query: CallbackQuery):
    uid = str(callback_query.from_user.id)
    db = get_db()

    for oid in list(db["orders"].keys()):
        if db["orders"][oid].get("client_id") == uid or db["orders"][oid].get("remover_id") == uid:
            del db["orders"][oid]

    if uid in db["users"]:
        del db["users"][uid]
    if uid in db["consents"]:
        del db["consents"][uid]
    if uid in user_states:
        del user_states[uid]

    save_db(db)
    await callback_query.message.edit_text("Ваш аккаунт и данные успешно удалены.")
    await callback_query.answer()


@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    if uid in user_states:
        user_states.pop(uid, None)
        db = get_db()
        role = db["users"][uid]["role"] if uid in db["users"] else None
        await message.reply("Отменено.", reply_markup=get_role_keyboard(role) if role else reg_kb())
    else:
        await message.reply("Нет активных действий.")


@app.on_message(filters.command("withdraw_consent") & filters.private)
@require_registered
async def withdraw_consent(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    if uid in db["users"]:
        db["users"][uid]["consent_given"] = False
        save_db(db)
    await message.reply(
        "Согласие на обработку ПДн отозвано. Доступ к персонализированным функциям ограничен.\n"
        "Для повторной активации используйте /start."
    )


@app.on_message(filters.command("mydata") & filters.private)
@require_registered
async def mydata_command(client: Client, message: Message):
    await show_my_data(client, message)


@app.on_message(filters.command("delete_account") & filters.private)
@require_registered
async def delete_account_command(client: Client, message: Message):
    await request_delete_account(client, message)


async def cleanup_expired_data():
    db = get_db()
    now = datetime.now()
    cutoff = now - timedelta(days=DATA_RETENTION_DAYS)
    deleted = 0
    for oid, order in list(db["orders"].items()):
        try:
            created = datetime.fromisoformat(order.get("created_at", now.isoformat()))
            if created < cutoff and order.get("status") == "done":
                del db["orders"][oid]
                deleted += 1
        except:
            continue
    if deleted:
        save_db(db)


app.run()
