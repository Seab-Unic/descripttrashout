import json
import os
import re
import logging
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

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
DADATA_API_KEY = os.getenv("DADATA_API_KEY")
DADATA_SECRET_KEY = os.getenv("DADATA_SECRET_KEY")

FILE = "data.json"
LOG_FILE = "audit.log"
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", 1095))

# 🔒 Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

audit_logger = logging.getLogger("audit")
audit_logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
audit_logger.addHandler(fh)

app = Client("descript_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_states: Dict[str, Dict[str, Any]] = {}

# 🛡️ Утилиты
def log_audit(action: str, user_id: str, details: Optional[str] = None):
    msg = f"ACTION:{action} | UID:{user_id}"
    if details: msg += f" | DETAILS:{details}"
    audit_logger.info(msg)

def hash_sensitive(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()

def is_likely_city(text: str) -> bool:
    if not text or not text.strip(): return False
    cleaned = text.strip()
    if re.fullmatch(r'[.,\-_\s]+', cleaned): return False
    return bool(re.search(r'[а-яё]', cleaned, re.IGNORECASE))

# 🗄️ Работа с БД
def get_db() -> Dict[str, Any]:
    if not os.path.exists(FILE):
        return {"users": {}, "orders": {}, "consents": {}}
    try:
        with open(FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for key in ["users", "orders", "consents"]:
            if key not in data: data[key] = {}
        return data
    except Exception as e:
        logger.error(f"DB read error: {e}")
        return {"users": {}, "orders": {}, "consents": {}}

def save_db(data: Dict[str, Any]) -> bool:
    try:
        temp_file = f"{FILE}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, FILE)
        if os.name != "nt": os.chmod(FILE, 0o600)
        return True
    except Exception as e:
        logger.error(f"DB save error: {e}")
        return False

def migrate_data():
    """Автоматически исправляет старые записи в data.json"""
    db = get_db()
    updated = False
    for uid, user in db.get("users", {}).items():
        if "role" not in user or user["role"] not in ["Клиент", "Выноситель", "Админ"]:
            user["role"] = "Клиент"
            updated = True
        if "consent_given" not in user:
            user["consent_given"] = False
            updated = True
        if "consent_timestamp" not in user:
            user["consent_timestamp"] = None
            updated = True
    if updated:
        save_db(db)
        logger.info("✅ Данные успешно мигрированы. Отсутствующие поля восстановлены.")

def ensure_user_role(db: Dict, uid: str) -> str:
    """Безопасно получает роль пользователя. Если отсутствует, ставит 'Клиент' и сохраняет."""
    user = db.get("users", {}).get(uid, {})
    role = user.get("role")
    if role not in ["Клиент", "Выноситель", "Админ"]:
        role = "Клиент"
        if uid in db.get("users", {}):
            db["users"][uid]["role"] = role
            save_db(db)
    return role

# ⌨️ Клавиатуры
def get_reply_keyboard(role: str) -> ReplyKeyboardMarkup:
    safe_role = role if role in ["Клиент", "Выноситель", "Админ"] else "Клиент"
    if safe_role == "Клиент":
        return ReplyKeyboardMarkup([[KeyboardButton("Заказать вынос"), KeyboardButton("Мои заказы")],
                                    [KeyboardButton("Мои данные"), KeyboardButton("Сменить город")],
                                    [KeyboardButton("Удалить аккаунт")]], resize_keyboard=True)
    elif safe_role == "Выноситель":
        return ReplyKeyboardMarkup([[KeyboardButton("Найти заказ"), KeyboardButton("Завершить заказ")],
                                    [KeyboardButton("Мои данные"), KeyboardButton("Сменить город")],
                                    [KeyboardButton("Удалить аккаунт")]], resize_keyboard=True)
    elif safe_role == "Админ":
        return ReplyKeyboardMarkup([[KeyboardButton("Статистика")],
                                    [KeyboardButton("Управление пользователями")],
                                    [KeyboardButton("Журнал аудита")]], resize_keyboard=True)
    return ReplyKeyboardMarkup([[KeyboardButton("Меню")]], resize_keyboard=True)

def reg_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Клиент", callback_data="reg_Клиент")],
        [InlineKeyboardButton("🚚 Выноситель", callback_data="reg_Выноситель")],
        [InlineKeyboardButton("🔑 Админ", callback_data="reg_Админ")]
    ])

def consent_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Я согласен на обработку ПДн", callback_data="consent_accept")],
        [InlineKeyboardButton("📄 Читать политику конфиденциальности", url="https://yoursite.ru/privacy")]
    ])

# 🔄 Декоратор проверки регистрации
def require_registered(func):
    @wraps(func)
    async def wrapper(client, message):
        uid = str(message.from_user.id)
        db = get_db()
        user = db["users"].get(uid)
        if not user or not user.get("consent_given"):
            await message.reply("Для работы с ботом необходимо принять условия обработки персональных данных.\nНажмите /start.", reply_markup=reg_kb())
            return
        return await func(client, message)
    return wrapper

# 🌐 Валидация города
async def validate_city(city_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not is_likely_city(city_name):
        return None, "Это не похоже на название города."
    if not DADATA_API_KEY or DADATA_API_KEY == "ВАШ_API_KEY":
        return city_name.strip().title(), None
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address"
    headers = {"Authorization": f"Token {DADATA_API_KEY}", "Content-Type": "application/json"}
    payload = {"query": city_name, "count": 1, "locations": [{"country_iso_code": "RU"}], "from_bound": {"value": "city"}, "to_bound": {"value": "city"}}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("suggestions"):
                        city_data = data["suggestions"][0].get("data", {})
                        normalized = city_data.get("city") or city_data.get("settlement")
                        return normalized, None
                    return None, f"Город '{city_name}' не найден."
                return None, f"Ошибка DaData ({resp.status})"
    except Exception as e:
        logger.error(f"DaData error: {e}")
        return None, "Ошибка подключения к сервису адресов."

# 📦 Логика заказов
async def ask_city(client: Client, uid: str, role: str, is_role_change: bool = False):
    user_states[uid] = {"state": "waiting_for_city", "role": role, "is_role_change": is_role_change}
    await client.send_message(uid, "Введите ваш город (например: Москва, Новосибирск):")

async def ask_order_details(client: Client, uid: str, city: str, address: str):
    user_states[uid] = {"state": "waiting_for_details", "city": city, "address": address}
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="skip_details")]])
    await client.send_message(uid, "Введите описание заказа (этаж, код домофона, вес) или нажмите 'Пропустить':", reply_markup=kb)

async def create_order_final(client: Client, uid: str, city: str, address: str, details: Optional[str] = None):
    db = get_db()
    order_id = f"ORD-{int(datetime.now().timestamp())}"
    db["orders"][order_id] = {
        "client_id": uid, "city": city, "address": address, "details": details,
        "status": "pending", "remover_id": None, "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "created_at": datetime.now().isoformat()
    }
    if save_db(db):
        log_audit("ORDER_CREATE", uid, f"OID:{order_id}")
        await client.send_message(uid, f"Заказ {order_id} создан! Ищем выносителя...", reply_markup=get_reply_keyboard("Клиент"))
    else:
        await client.send_message(uid, "Ошибка создания заказа.", reply_markup=get_reply_keyboard("Клиент"))

# 📥 Обработчики
@app.on_message(filters.command("start") & filters.private)
async def start(client: Client, message: Message):
    db = get_db()
    uid = str(message.from_user.id)
    user = db["users"].get(uid)
    if user and user.get("consent_given"):
        role = ensure_user_role(db, uid)
        log_audit("LOGIN", uid, f"Role:{role}")
        await message.reply(f"С возвращением! Вы вошли как {role}.", reply_markup=get_reply_keyboard(role))
    else:
        log_audit("START_CMD", uid, "Consent required")
        await message.reply(
            "Привет! Для использования сервиса необходимо согласие на обработку персональных данных.\n"
            "Мы собираем: имя, ID Telegram, город, историю заказов.\n"
            "Данные хранятся на серверах в РФ и не передаются третьим лицам, кроме сервиса валидации адресов (DaData) при вашем согласии.",
            reply_markup=consent_kb()
        )

@app.on_callback_query(filters.regex(r"^consent_accept$"))
async def handle_consent_accept(client: Client, callback_query: CallbackQuery):
    uid = str(callback_query.from_user.id)
    db = get_db()
    timestamp = datetime.now().isoformat()
    if uid not in db["users"]:
        db["users"][uid] = {"name": callback_query.from_user.first_name, "username": callback_query.from_user.username, "registered_at": timestamp}
    db["users"][uid].update({"consent_given": True, "consent_timestamp": timestamp})
    db["consents"][uid] = {"given_at": timestamp, "ip_hash": hash_sensitive(str(uid)), "version": "1.0"}
    save_db(db)
    log_audit("CONSENT_GIVEN", uid)
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

    role = callback_query.data.split("_", 1)[1]
    # 🔒 Проверка пароля для Админа
    if role == "Админ":
        if not ADMIN_PASSWORD:
            await callback_query.answer("Пароль админа не настроен в .env", show_alert=True)
            return
        user_states[uid] = {"state": "admin_password_check", "target_role": "Админ"}
        await callback_query.message.edit_text("Введите пароль администратора:")
        await callback_query.answer()
        return

    if user.get("role") == role and user.get("city"):
        await callback_query.message.edit_text(f"Вы уже зарегистрированы как {role}.", reply_markup=get_reply_keyboard(role))
        return

    await callback_query.message.delete()
    if user.get("city"):
        db["users"][uid]["role"] = role
        save_db(db)
        log_audit("ROLE_UPDATE", uid, f"New role: {role}")
        await client.send_message(uid, f"Роль изменена на {role}", reply_markup=get_reply_keyboard(role))
    else:
        await ask_city(client, uid, role, is_role_change=bool(user.get("role")))
    await callback_query.answer()

@app.on_message(filters.command("mydata") & filters.private)
@require_registered
async def show_my_data(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    role = ensure_user_role(db, uid)
    user = db["users"].get(uid, {})
    my_orders = [o for oid, o in db["orders"].items() if o.get("client_id") == uid or o.get("remover_id") == uid]
    text = f"📋 Ваши персональные данные:\nID: {uid}\nИмя: {user.get('name')}\nРоль: {role}\nГород: {user.get('city', 'Не указан')}\nСогласие дано: {user.get('consent_timestamp', 'Нет')}\n\n📦 Заказы ({len(my_orders)}):\n"
    for order in my_orders[:5]:
        text += f"- {order.get('id', 'N/A')}: {order.get('status')} ({order.get('date')})\n"
    if len(my_orders) > 5: text += f"... и еще {len(my_orders) - 5} заказов"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Запросить удаление всех данных", callback_data="req_delete_all")]])
    await message.reply(text, reply_markup=kb)
    log_audit("DATA_ACCESS", uid)

@app.on_message(filters.command("delete_account") & filters.private)
@require_registered
async def request_delete_account(client: Client, message: Message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ Да, удалить безвозвратно", callback_data="confirm_delete_account")],
        [InlineKeyboardButton("Отмена", callback_data="cancel_action")]
    ])
    await message.reply("Вы уверены? Это действие необратимо. Все ваши персональные данные и история заказов будут удалены.", reply_markup=kb)
    log_audit("DELETE_REQUEST", str(message.from_user.id))

@app.on_message(filters.command("withdraw_consent") & filters.private)
@require_registered
async def withdraw_consent(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    if uid in db["users"]:
        db["users"][uid]["consent_given"] = False
        save_db(db)
    log_audit("CONSENT_WITHDRAWN", uid)
    await message.reply("Согласие на обработку ПДн отозвано. Доступ к функциям ограничен. Используйте /start для повторной активации.")

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    if uid in user_states:
        user_states.pop(uid, None)
        db = get_db()
        role = ensure_user_role(db, uid) if uid in db.get("users", {}) else None
        await message.reply("Действие отменено.", reply_markup=get_reply_keyboard(role) if role else reg_kb())
    else:
        await message.reply("Нет активных действий для отмены.")

@app.on_message(filters.text & filters.private & ~filters.command(["start", "cancel", "mydata", "delete_account", "withdraw_consent"]))
@require_registered
async def handle_reply_buttons(client: Client, message: Message):
    uid = str(message.from_user.id)
    if uid in user_states:
        await text_handler(client, message)
        return

    db = get_db()
    role = ensure_user_role(db, uid)
    text = message.text.strip()

    if role == "Клиент":
        if "Заказать вынос" in text:
            city = db["users"][uid].get("city")
            if city:
                kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"{city} (оставить)", callback_data="order_city_keep")], [InlineKeyboardButton("Ввести другой", callback_data="order_city_change")]])
                await message.reply(f"Ваш город: {city}. Использовать?", reply_markup=kb)
            else: await ask_city(client, uid, "Клиент", is_role_change=False)
        elif "Мои заказы" in text: await my_orders_command(client, message)
        elif "Сменить город" in text: await ask_city(client, uid, role, is_role_change=False)
        elif "Мои данные" in text: await show_my_data(client, message)
        else: await message.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    elif role == "Выноситель":
        if "Найти заказ" in text: await find_orders_command(client, message)
        elif "Завершить заказ" in text: await finish_order_command(client, message)
        elif "Сменить город" in text: await ask_city(client, uid, role, is_role_change=False)
        elif "Мои данные" in text: await show_my_data(client, message)
        else: await message.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    elif role == "Админ":
        if "Статистика" in text: await admin_stats_command(client, message)
        elif "Управление пользователями" in text: await list_users_for_admin(client, message)
        elif "Журнал аудита" in text: await show_audit_log(client, message)
        else: await message.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))

async def text_handler(client: Client, message: Message):
    if message.text.startswith("/"): return
    uid = str(message.from_user.id)
    state = user_states.get(uid)
    if not state: return

    state_name = state.get("state")

    if state_name == "admin_password_check":
        if message.text.strip() == ADMIN_PASSWORD:
            target_role = state.get("target_role", "Админ")
            db = get_db()
            db["users"][uid]["role"] = target_role
            save_db(db)
            log_audit("ADMIN_REGISTRATION", uid)
            await message.reply(f"Доступ подтверждён. Роль: {target_role}", reply_markup=get_reply_keyboard(target_role))
        else:
            await message.reply("❌ Неверный пароль. Регистрация отменена.", reply_markup=reg_kb())
        user_states.pop(uid, None)
        return

    if state_name == "waiting_for_city":
        city_input = message.text.strip()
        status_msg = await message.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await status_msg.delete()
        if error:
            await message.reply(error)
            return

        db = get_db()
        new_role = state.get("role")
        is_change = state.get("is_role_change", False)
        if uid not in db["users"]:
            db["users"][uid] = {"name": message.from_user.first_name, "role": new_role, "city": city, "consent_given": True, "consent_timestamp": datetime.now().isoformat()}
            save_db(db)
            log_audit("REGISTRATION_COMPLETE", uid, f"City:{city}")
            await message.reply(f"Регистрация завершена! Роль: {new_role}, Город: {city}", reply_markup=get_reply_keyboard(new_role))
        elif is_change:
            db["users"][uid].update({"role": new_role, "city": city})
            save_db(db)
            log_audit("PROFILE_UPDATE", uid, f"City:{city}, Role:{new_role}")
            await message.reply(f"Роль изменена на {new_role}. Город: {city}", reply_markup=get_reply_keyboard(new_role))
        else:
            db["users"][uid]["city"] = city
            save_db(db)
            log_audit("PROFILE_UPDATE", uid, f"City:{city}")
            current_role = ensure_user_role(db, uid)
            await message.reply(f"Город изменён на {city}", reply_markup=get_reply_keyboard(current_role))
        user_states.pop(uid, None)
        return

    if state_name == "order_city_new":
        city_input = message.text.strip()
        status_msg = await message.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await status_msg.delete()
        if error: await message.reply(error); return
        user_states[uid] = {"state": "waiting_for_address", "city": city}
        await message.reply(f"Выбран город: {city}\nВведите адрес (улица, дом):")
        return

    if state_name == "waiting_for_address":
        addr = message.text.strip()
        if not addr: return await message.reply("Адрес не может быть пустым.")
        city = state.get("city")
        if city:
            log_audit("ORDER_ADDRESS_INPUT", uid, f"City:{city}")
            await ask_order_details(client, uid, city, addr)
        else:
            await message.reply("Ошибка: не указан город. Начните заказ заново.", reply_markup=get_reply_keyboard("Клиент"))
        user_states.pop(uid, None)
        return

    if state_name == "waiting_for_details":
        details = message.text.strip()
        city = state.get("city")
        address = state.get("address")
        await create_order_final(client, uid, city, address, details=details if details else None)
        user_states.pop(uid, None)
        return

    user_states.pop(uid, None)
    await message.reply("Сбой состояния. Начните с /start")

# 📋 Команды и колбэки
async def my_orders_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    text = "Ваши заказы:\n\n"
    has_orders = False
    for oid, order in db["orders"].items():
        if order.get("client_id") == uid:
            has_orders = True
            text += f"Заказ {oid}\nГород: {order.get('city', '')}\nАдрес: {order.get('address', '')}\nСтатус: {order.get('status', 'Неизвестно')}\n---\n"
    await message.reply(text if has_orders else "У вас пока нет заказов.", reply_markup=get_reply_keyboard("Клиент"))

async def find_orders_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    user_city = db["users"].get(uid, {}).get("city")
    if not user_city: return await message.reply("Сначала укажите город.", reply_markup=get_reply_keyboard("Выноситель"))
    for oid, order in db["orders"].items():
        if order.get("remover_id") == uid and order.get("status") == "active":
            return await message.reply("Сначала завершите текущий заказ!", reply_markup=get_reply_keyboard("Выноситель"))
    kb = InlineKeyboardMarkup([])
    has_orders = False
    for oid, order in db["orders"].items():
        if order.get("status") == "pending" and order.get("city") == user_city:
            has_orders = True
            kb.inline_keyboard.append([InlineKeyboardButton(f"{oid} | {order.get('address', 'адрес')}", callback_data=f"take_{oid}")])
    if not has_orders:
        await message.reply(f"Свободных заказов в {user_city} нет.", reply_markup=get_reply_keyboard("Выноситель"))
    else:
        kb.inline_keyboard.append([InlineKeyboardButton("Назад", callback_data="back_remover")])
        await message.reply(f"Доступные заказы в {user_city}:", reply_markup=kb)

async def finish_order_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    active = next((oid for oid, o in db["orders"].items() if o.get("remover_id") == uid and o.get("status") == "active"), None)
    if not active: return await message.reply("Нет активных заказов.", reply_markup=get_reply_keyboard("Выноситель"))
    db["orders"][active]["status"] = "waiting_complete"
    save_db(db)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Подтвердить выполнение", callback_data=f"complete_yes_{active}")], [InlineKeyboardButton("Отклонить", callback_data=f"complete_no_{active}")]])
    try:
        await client.send_message(db["orders"][active]["client_id"], f"Выноситель отметил заказ {active} как выполненный. Подтверждаете?", reply_markup=kb)
        await message.reply(f"Запрос на подтверждение выполнения заказа {active} отправлен клиенту.", reply_markup=get_reply_keyboard("Выноситель"))
    except Exception as e:
        logger.error(f"Notify client error: {e}")
        db["orders"][active]["status"] = "active"
        save_db(db)
        await message.reply("Не удалось связаться с клиентом. Заказ остаётся активным.", reply_markup=get_reply_keyboard("Выноситель"))

async def admin_stats_command(client: Client, message: Message):
    db = get_db()
    stats = {s: sum(1 for o in db["orders"].values() if o.get("status") == s) for s in ["pending", "awaiting_confirm", "active", "waiting_complete", "done"]}
    text = f"📊 Статистика\nПользователей: {len(db['users'])}\nОжидают: {stats['pending']}\nВ работе: {stats['active']}\nВыполнено: {stats['done']}"
    await message.reply(text, reply_markup=get_reply_keyboard("Админ"))

async def list_users_for_admin(client: Client, message: Message):
    db = get_db()
    uid_admin = str(message.from_user.id)
    other_users = {uid: data for uid, data in db["users"].items() if uid != uid_admin}
    if not other_users: return await message.reply("Нет других пользователей.", reply_markup=get_reply_keyboard("Админ"))
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for uid, user in other_users.items():
        label = f"{user.get('name', 'Без имени')} [{user.get('role', '?')}]"
        kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"admin_user_{uid}")])
    await message.reply("Управление пользователями:", reply_markup=kb)

async def show_audit_log(client: Client, message: Message):
    if not os.path.exists(LOG_FILE): return await message.reply("Журнал аудита пуст.", reply_markup=get_reply_keyboard("Админ"))
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-20:]
        await message.reply("🔒 Последние записи аудита:\n" + "".join(lines), reply_markup=get_reply_keyboard("Админ"))
    except Exception as e:
        logger.error(f"Read audit log error: {e}")
        await message.reply("Ошибка чтения журнала.", reply_markup=get_reply_keyboard("Админ"))

@app.on_callback_query()
async def handle_callbacks(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    uid = str(callback_query.from_user.id)
    db = get_db()
    role = ensure_user_role(db, uid)

    if data == "order_city_keep":
        city = db["users"].get(uid, {}).get("city")
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
        if not order or order.get("status") != "pending": return await callback_query.answer("Заказ недоступен", show_alert=True)
        if db["users"].get(uid, {}).get("city") != order.get("city"): return await callback_query.answer("Не ваш город", show_alert=True)
        order["status"] = "awaiting_confirm"
        order["remover_id"] = uid
        save_db(db)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Подтвердить", callback_data=f"confirm_yes_{oid}")], [InlineKeyboardButton("Отказать", callback_data=f"confirm_no_{oid}")]])
        try:
            await client.send_message(order["client_id"], f"Выноситель хочет взять заказ {oid}.\nАдрес: {order['address']}\nПодтверждаете?", reply_markup=kb)
            await callback_query.message.delete()
            await callback_query.message.reply("Запрос отправлен клиенту.", reply_markup=get_reply_keyboard("Выноситель"))
        except: await callback_query.answer("Ошибка отправки", show_alert=True)
    elif data.startswith(("confirm_yes_", "confirm_no_", "complete_yes_", "complete_no_")):
        action, status, oid = data.split("_", 2)
        approved = status == "yes"
        order = db["orders"].get(oid)
        if not order: return await callback_query.answer("Заказ не найден", show_alert=True)
        
        if "confirm" in action:
            if order.get("client_id") != uid or order.get("status") != "awaiting_confirm": return await callback_query.answer("Уже обработан", show_alert=True)
            order["status"] = "active" if approved else "pending"
            if not approved: order.pop("remover_id", None)
            save_db(db)
            msg = "Подтверждён" if approved else "Отклонён. Поиск нового выносителя."
            try: await callback_query.message.edit_text(f"Заказ {oid}: {msg}")
            except: pass
        else:
            if order.get("client_id") != uid or order.get("status") != "waiting_complete": return await callback_query.answer("Уже обработан", show_alert=True)
            order["status"] = "done" if approved else "active"
            save_db(db)
            msg = "Выполнен" if approved else "Продолжайте выполнение"
            try: await callback_query.message.edit_text(f"Заказ {oid}: {msg}")
            except: pass
        await callback_query.answer()
    elif data == "skip_details":
        state = user_states.get(uid)
        if state and state.get("state") == "waiting_for_details":
            await create_order_final(client, uid, state.get("city"), state.get("address"))
            user_states.pop(uid, None)
            try: await callback_query.message.delete()
            except: pass
        await callback_query.answer()
    elif data == "back_remover":
        try: await callback_query.message.delete()
        except: pass
        await callback_query.message.reply("Меню выносителя:", reply_markup=get_reply_keyboard("Выноситель"))
        await callback_query.answer()
    elif data == "req_delete_all":
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚠️ Да, удалить всё", callback_data="confirm_delete_account")], [InlineKeyboardButton("Отмена", callback_data="cancel_action")]])
        await callback_query.message.reply("Подтвердите удаление всех ваших данных:", reply_markup=kb)
        await callback_query.answer()
    elif data == "cancel_action":
        await callback_query.message.edit_text("Действие отменено.")
        await callback_query.answer()
    elif data.startswith("admin_user_"):
        target_uid = data.split("_", 2)[2]
        target_user = db["users"].get(target_uid)
        if not target_user: return await callback_query.answer("Не найден", show_alert=True)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🗑 Удалить пользователя", callback_data=f"admin_del_{target_uid}")]])
        await callback_query.message.edit_text(f"ID: {target_uid}\nИмя: {target_user.get('name')}\nРоль: {target_user.get('role')}\nГород: {target_user.get('city')}", reply_markup=kb)
        await callback_query.answer()
    elif data.startswith("admin_del_"):
        target_uid = data.split("_", 2)[2]
        if target_uid in db["users"]:
            for oid in list(db["orders"].keys()):
                if db["orders"][oid].get("client_id") == target_uid or db["orders"][oid].get("remover_id") == target_uid:
                    del db["orders"][oid]
            name = db["users"][target_uid].get("name", "Пользователь")
            del db["users"][target_uid]
            db["consents"].pop(target_uid, None)
            save_db(db)
            log_audit("ADMIN_USER_DELETE", uid, f"Deleted UID:{target_uid}")
            await callback_query.message.edit_text(f"{name} удалён.")
            await list_users_for_admin(client, callback_query.message)
        await callback_query.answer()
    elif data == "confirm_delete_account":
        db["orders"] = {k: v for k, v in db["orders"].items() if v.get("client_id") != uid and v.get("remover_id") != uid}
        db["users"].pop(uid, None)
        db["consents"].pop(uid, None)
        user_states.pop(uid, None)
        save_db(db)
        log_audit("ACCOUNT_DELETED", uid)
        await callback_query.message.edit_text("✅ Аккаунт и данные успешно удалены.")
        await callback_query.answer()

async def cleanup_expired_data():
    db = get_db()
    now = datetime.now()
    cutoff = now - timedelta(days=DATA_RETENTION_DAYS)
    deleted = 0
    for oid in list(db["orders"].keys()):
        try:
            order_date = datetime.fromisoformat(db["orders"][oid].get("created_at", now.isoformat()))
            if order_date < cutoff and db["orders"][oid].get("status") == "done":
                del db["orders"][oid]
                deleted += 1
        except: continue
    if deleted: save_db(db); logger.info(f"Cleanup: removed {deleted} expired orders")
app.run()
