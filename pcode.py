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

BOT_TOKEN = ""
API_ID = 
API_HASH = ""
FILE = "data.json"
ADMIN_PASSWORD = "123"  

DADATA_API_KEY = "cf7b21165e788a558a05449046ddeab78dbf153f"
DADATA_SECRET_KEY = "d6ddb27fb2a980853186f620c416b461800464e3"
app = Client("bot_session_name", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_states = {}
FILE = "data.json"
LOG_FILE = "audit.log"
DATA_RETENTION_DAYS = int(os.getenv("DATA_RETENTION_DAYS", 1095))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

audit_logger = logging.getLogger("audit")
audit_logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
audit_logger.addHandler(fh)

app = Client("bot_session_name", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_states: Dict[str, Dict[str, Any]] = {}


def log_audit(action: str, user_id: str, details: Optional[str] = None):
    msg = f"ACTION:{action} | UID:{user_id}"
    if details:
        msg += f" | DETAILS:{details}"
    audit_logger.info(msg)


def require_registered(func):
    @wraps(func)
    async def wrapper(client, message):
        uid = str(message.from_user.id)
        db = get_db()
        user = db["users"].get(uid)
        if not user or not user.get("consent_given"):
            await message.reply(
                "Для работы с ботом необходимо принять условия обработки персональных данных.\n"
                "Нажмите /start для начала.",
                reply_markup=reg_kb()
            )
            return
        return await func(client, message)
    return wrapper


def get_db() -> Dict[str, Any]:
    if not os.path.exists(FILE):
        return {"users": {}, "orders": {}, "consents": {}}
    try:
        with open(FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for key in ["users", "orders", "consents"]:
            if key not in data:
                data[key] = {}
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
        if os.name != "nt":
            os.chmod(FILE, 0o600)
        return True
    except Exception as e:
        logger.error(f"DB save error: {e}")
        return False


def hash_sensitive(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def is_likely_city(text: str) -> bool:
    if not text or not text.strip():
        return False
    cleaned = text.strip()
    if re.fullmatch(r'[.,\-_\s]+', cleaned):
        return False
    if not re.search(r'[а-яё]', cleaned, re.IGNORECASE):
        return False
    return True


async def validate_city(city_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not is_likely_city(city_name):
        return None, "Это не похоже на название города. Пожалуйста, введите реальный город."
    if not DADATA_API_KEY or DADATA_API_KEY == "ВАШ_API_KEY":
        return city_name.strip().title(), None
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address"
    headers = {
        "Authorization": f"Token {DADATA_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "query": city_name,
        "count": 1,
        "locations": [{"country_iso_code": "RU"}],
        "from_bound": {"value": "city"},
        "to_bound": {"value": "city"}
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("suggestions"):
                        city_data = data["suggestions"][0].get("data", {})
                        normalized = city_data.get("city") or city_data.get("settlement")
                        if normalized:
                            return normalized, None
                    return None, f"Не удалось определить город для '{city_name}'."
                else:
                    error_text = await response.text()
                    return None, f"Ошибка DaData ({response.status}): {error_text}"
    except Exception as e:
        logger.error(f"DaData connection error: {e}")
        return None, f"Ошибка подключения к сервису валидации адресов."


def get_reply_keyboard(role: str) -> Optional[ReplyKeyboardMarkup]:
    if role == "Клиент":
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Заказать вынос"), KeyboardButton("Мои заказы")],
             [KeyboardButton("Мои данные"), KeyboardButton("Сменить город")],
             [KeyboardButton("Удалить аккаунт")]],
            resize_keyboard=True
        )
    elif role == "Выноситель":
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Найти заказ"), KeyboardButton("Завершить заказ")],
             [KeyboardButton("Мои данные"), KeyboardButton("Сменить город")],
             [KeyboardButton("Удалить аккаунт")]],
            resize_keyboard=True
        )
    elif role == "Админ":
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Статистика")],
             [KeyboardButton("Управление пользователями")],
             [KeyboardButton("Журнал аудита")]],
            resize_keyboard=True
        )
    return None


def reg_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Клиент", callback_data="reg_Клиент")],
        [InlineKeyboardButton("Выноситель", callback_data="reg_Выноситель")]
    ])


def consent_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "✅ Я согласен на обработку ПДн",
            callback_data="consent_accept"
        )],
        [InlineKeyboardButton(
            "📄 Читать политику конфиденциальности",
            url="https://yoursite.ru/privacy"
        )]
    ])


async def ask_city(client: Client, uid: str, role: str, is_role_change: bool = False):
    user_states[uid] = {"state": "waiting_for_city", "role": role, "is_role_change": is_role_change}
    await client.send_message(uid, "Введите ваш город (например: Москва, Новосибирск):")


async def ask_order_details(client: Client, uid: str, city: str, address: str):
    user_states[uid] = {"state": "waiting_for_details", "city": city, "address": address}
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="skip_details")]])
    await client.send_message(
        uid,
        "Введите дополнительное описание заказа (этаж, код домофона, вес).\nИли нажмите 'Пропустить':",
        reply_markup=kb
    )


async def create_order_final(client: Client, uid: str, city: str, address: str, details: Optional[str] = None):
    db = get_db()
    order_id = f"ORD-{int(datetime.now().timestamp())}"
    db["orders"][order_id] = {
        "client_id": uid,
        "city": city,
        "address": address,
        "details": details,
        "status": "pending",
        "remover_id": None,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "created_at": datetime.now().isoformat()
    }
    if save_db(db):
        log_audit("ORDER_CREATE", uid, f"OID:{order_id}")
        await client.send_message(uid, f"Заказ {order_id} создан! Ищем выносителя...", reply_markup=get_reply_keyboard("Клиент"))
    else:
        await client.send_message(uid, "Ошибка создания заказа.", reply_markup=get_reply_keyboard("Клиент"))


async def list_users_for_admin(client: Client, message: Message):
    db = get_db()
    uid_admin = str(message.from_user.id)
    other_users = {uid: data for uid, data in db["users"].items() if uid != uid_admin}
    if not other_users:
        await message.reply("Нет других пользователей.", reply_markup=get_reply_keyboard("Админ"))
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for uid, user in other_users.items():
        name = user.get("name", "Без имени")
        username = f" @{user.get('username')}" if user.get('username') else ""
        label = f"{name}{username} [{user.get('role', '')}]"
        kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"admin_user_{uid}")])
    await message.reply("Управление пользователями:", reply_markup=kb)


@app.on_message(filters.command("start") & filters.private)
async def start(client: Client, message: Message):
    db = get_db()
    uid = str(message.from_user.id)
    user = db["users"].get(uid)

    if user and user.get("consent_given"):
        # 🔒 FIX: безопасное получение + авто-миграция отсутствующей роли
        role = user.get("role")
        if not role or role not in ["Клиент", "Выноситель", "Админ"]:
            role = "Клиент"
            db["users"][uid]["role"] = role
            save_db(db)  # Сохраняем исправление в БД

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
        db["users"][uid] = {
            "name": callback_query.from_user.first_name,
            "username": callback_query.from_user.username,
            "registered_at": timestamp
        }
    db["users"][uid]["consent_given"] = True
    db["users"][uid]["consent_timestamp"] = timestamp
    db["consents"][uid] = {
        "given_at": timestamp,
        "ip_hash": hash_sensitive(str(callback_query.from_user.id)),
        "version": "1.0"
    }
    save_db(db)
    log_audit("CONSENT_GIVEN", uid)
    await callback_query.message.edit_text(
        "Спасибо! Теперь выберите вашу роль:",
        reply_markup=reg_kb()
    )
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
    user = db["users"].get(uid, {})
    my_orders = [o for oid, o in db["orders"].items() if o.get("client_id") == uid or o.get("remover_id") == uid]

    text = (
        f"📋 Ваши персональные данные:\n"
        f"ID: {uid}\n"
        f"Имя: {user.get('name')}\n"
        f"Роль: {user.get('role')}\n"
        f"Город: {user.get('city', 'Не указан')}\n"
        f"Согласие дано: {user.get('consent_timestamp', 'Нет')}\n\n"
        f"📦 Заказы ({len(my_orders)}):\n"
    )
    for order in my_orders[:5]:
        text += f"- {order.get('id', 'N/A')}: {order.get('status')} ({order.get('date')})\n"
    if len(my_orders) > 5:
        text += f"... и еще {len(my_orders) - 5} заказов"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🗑 Запросить удаление всех данных", callback_data="req_delete_all")]
    ])
    await message.reply(text, reply_markup=kb)
    log_audit("DATA_ACCESS", uid)


@app.on_message(filters.command("delete_account") & filters.private)
@require_registered
async def request_delete_account(client: Client, message: Message):
    uid = str(message.from_user.id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ Да, удалить безвозвратно", callback_data="confirm_delete_account")],
        [InlineKeyboardButton("Отмена", callback_data="cancel_action")]
    ])
    await message.reply(
        "Вы уверены? Это действие необратимо. Все ваши персональные данные и история заказов будут удалены.",
        reply_markup=kb
    )
    log_audit("DELETE_REQUEST", uid)


@app.on_callback_query(filters.regex(r"^confirm_delete_account$"))
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
    log_audit("ACCOUNT_DELETED", uid)
    await callback_query.message.edit_text("Ваш аккаунт и данные успешно удалены.")
    await callback_query.answer()


@app.on_message(filters.command("withdraw_consent") & filters.private)
@require_registered
async def withdraw_consent(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    if uid in db["users"]:
        db["users"][uid]["consent_given"] = False
        save_db(db)
    log_audit("CONSENT_WITHDRAWN", uid)
    await message.reply(
        "Согласие на обработку ПДн отозвано. Доступ к персонализированным функциям ограничен.\n"
        "Для повторной активации используйте /start."
    )


@app.on_message(filters.text & filters.private & ~filters.command(["start", "cancel", "mydata", "delete_account", "withdraw_consent"]))
@require_registered
async def handle_reply_buttons(client: Client, message: Message):
    uid = str(message.from_user.id)
    if uid in user_states:
        await text_handler(client, message)
        return

    db = get_db()
    role = db["users"][uid]["role"]
    text = message.text.strip()

    if role == "Клиент":
        if "Заказать вынос" in text:
            city = db["users"][uid].get("city")
            if city:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{city} (оставить)", callback_data="order_city_keep")],
                    [InlineKeyboardButton("Ввести другой", callback_data="order_city_change")]
                ])
                await message.reply(f"Ваш город: {city}. Использовать?", reply_markup=kb)
            else:
                await ask_city(client, uid, "Клиент", is_role_change=False)
        elif "Мои заказы" in text:
            await my_orders_command(client, message)
        elif "Сменить город" in text:
            await ask_city(client, uid, role, is_role_change=False)
        elif "Мои данные" in text:
            await show_my_data(client, message)
        else:
            await message.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    elif role == "Выноситель":
        if "Найти заказ" in text:
            await find_orders_command(client, message)
        elif "Завершить заказ" in text:
            await finish_order_command(client, message)
        elif "Сменить город" in text:
            await ask_city(client, uid, role, is_role_change=False)
        elif "Мои данные" in text:
            await show_my_data(client, message)
        else:
            await message.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    elif role == "Админ":
        if "Статистика" in text:
            await admin_stats_command(client, message)
        elif "Управление пользователями" in text:
            await list_users_for_admin(client, message)
        elif "Журнал аудита" in text:
            await show_audit_log(client, message)
        else:
            await message.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))


async def my_orders_command(client: Client, message: Message):
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
            text += f"Заказ {oid}\nГород: {order.get('city', '')}\nАдрес: {order.get('address', '')}{details}\nСтатус: {status_map.get(order['status'], 'Неизвестно')}\n---\n"
    await message.reply(text if has_orders else "У вас пока нет заказов.", reply_markup=get_reply_keyboard("Клиент"))


async def find_orders_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    user_city = db["users"][uid].get("city")
    if not user_city:
        return await message.reply("Сначала укажите город через 'Сменить город'.", reply_markup=get_reply_keyboard("Выноситель"))

    for oid, order in db["orders"].items():
        if order.get("remover_id") == uid and order.get("status") == "active":
            return await message.reply("Сначала завершите текущий заказ!", reply_markup=get_reply_keyboard("Выноситель"))

    kb = InlineKeyboardMarkup([])
    has_orders = False
    for oid, order in db["orders"].items():
        if order.get("status") == "pending" and order.get("city") == user_city:
            has_orders = True
            address = order.get('address', 'без адреса')
            details = order.get('details')
            label = f"{oid} | {address}"
            if details:
                label += f" | {details[:20]}..."
            kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"take_{oid}")])

    if not has_orders:
        await message.reply(f"Свободных заказов в {user_city} нет.", reply_markup=get_reply_keyboard("Выноситель"))
    else:
        kb.inline_keyboard.append([InlineKeyboardButton("Назад", callback_data="back_remover")])
        await message.reply(f"Доступные заказы в {user_city}:", reply_markup=kb)


async def finish_order_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    db = get_db()
    active = next((oid for oid, o in db["orders"].items() if o.get("remover_id") == uid and o.get("status") == "active"), None)
    if not active:
        return await message.reply("Нет активных заказов.", reply_markup=get_reply_keyboard("Выноситель"))

    db["orders"][active]["status"] = "waiting_complete"
    save_db(db)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Подтвердить выполнение", callback_data=f"complete_yes_{active}")],
        [InlineKeyboardButton("Отклонить (заказ продолжается)", callback_data=f"complete_no_{active}")]
    ])
    try:
        remover_name = db['users'][uid]['name']
        await client.send_message(
            db["orders"][active]["client_id"],
            f"Выноситель {remover_name} отметил заказ {active} как выполненный.\nПодтверждаете?",
            reply_markup=kb
        )
        await message.reply(f"Запрос на подтверждение выполнения заказа {active} отправлен клиенту.", reply_markup=get_reply_keyboard("Выноситель"))
    except Exception as e:
        logger.error(f"Notify client error: {e}")
        db["orders"][active]["status"] = "active"
        save_db(db)
        await message.reply("Не удалось связаться с клиентом. Заказ остаётся активным.", reply_markup=get_reply_keyboard("Выноситель"))


async def admin_stats_command(client: Client, message: Message):
    db = get_db()
    stats = {s: sum(1 for o in db["orders"].values() if o["status"] == s) for s in ["pending", "awaiting_confirm", "active", "waiting_complete", "done"]}
    text = (
        f"📊 Статистика\n"
        f"Пользователей: {len(db['users'])}\n"
        f"Ожидают: {stats['pending']}\n"
        f"Ждут подтверждения: {stats['awaiting_confirm']}\n"
        f"В работе: {stats['active']}\n"
        f"Ждут подтверждения выполнения: {stats['waiting_complete']}\n"
        f"Выполнено: {stats['done']}"
    )
    await message.reply(text, reply_markup=get_reply_keyboard("Админ"))


async def show_audit_log(client: Client, message: Message):
    if not os.path.exists(LOG_FILE):
        return await message.reply("Журнал аудита пуст.", reply_markup=get_reply_keyboard("Админ"))
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-20:]
        text = "🔒 Последние записи аудита:\n" + "".join(lines)
        await message.reply(text, reply_markup=get_reply_keyboard("Админ"))
    except Exception as e:
        logger.error(f"Read audit log error: {e}")
        await message.reply("Ошибка чтения журнала.", reply_markup=get_reply_keyboard("Админ"))


@app.on_callback_query()
async def handle_callbacks(client: Client, callback_query: CallbackQuery):
    data = callback_query.data
    uid = str(callback_query.from_user.id)
    db = get_db()

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
            [InlineKeyboardButton("Подтвердить", callback_data=f"confirm_yes_{oid}")],
            [InlineKeyboardButton("Отказать", callback_data=f"confirm_no_{oid}")]
        ])
        try:
            remover_name = db['users'][uid]['name']
            await client.send_message(
                order["client_id"],
                f"Выноситель {remover_name} хочет взять ваш заказ {oid}.\nАдрес: {order['address']}{details_text}\n\nПодтверждаете?",
                reply_markup=kb
            )
            await callback_query.message.delete()
            await callback_query.message.reply("Запрос отправлен клиенту.", reply_markup=get_reply_keyboard("Выноситель"))
        except Exception as e:
            logger.error(f"Notify client error: {e}")
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
        await callback_query.message.reply("Меню выносителя:", reply_markup=get_reply_keyboard("Выноситель"))
        await callback_query.answer()
    elif data == "req_delete_all":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚠️ Да, удалить всё", callback_data="confirm_delete_account")],
            [InlineKeyboardButton("Отмена", callback_data="cancel_action")]
        ])
        await callback_query.message.reply("Подтвердите удаление всех ваших данных:", reply_markup=kb)
        await callback_query.answer()
    elif data == "cancel_action":
        await callback_query.message.edit_text("Действие отменено.")
        await callback_query.answer()
    elif data.startswith("admin_user_"):
        target_uid = data.split("_", 2)[2]
        target_user = db["users"].get(target_uid)
        if not target_user:
            return await callback_query.answer("Пользователь не найден", show_alert=True)
        kb = InlineKeyboardMarkup([
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
                if db["orders"][oid].get("client_id") == target_uid or db["orders"][oid].get("remover_id") == target_uid:
                    del db["orders"][oid]
            name = db["users"][target_uid].get("name", "Пользователь")
            del db["users"][target_uid]
            if target_uid in db["consents"]:
                del db["consents"][target_uid]
            save_db(db)
            log_audit("ADMIN_USER_DELETE", uid, f"Deleted UID:{target_uid}")
            await callback_query.message.edit_text(f"{name} удалён.")
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
        try:
            await c.message.edit_text(f"Заказ {oid} подтверждён. Выноситель приступит к выполнению.", reply_markup=get_reply_keyboard("Клиент"))
        except:
            pass
    else:
        order["status"] = "pending"
        rem_id = order.pop("remover_id", None)
        save_db(db)
        if rem_id:
            await client.send_message(rem_id, f"Заказ {oid} отклонён клиентом.")
        try:
            await c.message.edit_text(f"Заказ {oid} отклонён. Поиск нового выносителя.", reply_markup=get_reply_keyboard("Клиент"))
        except:
            pass
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
            await client.send_message(remover_id, f"Клиент подтвердил выполнение заказа {oid}! Заказ завершён.", reply_markup=get_reply_keyboard("Выноситель"))
        await c.message.edit_text(f"Заказ {oid} успешно выполнен! Спасибо за использование сервиса.", reply_markup=get_reply_keyboard("Клиент"))
    else:
        order["status"] = "active"
        save_db(db)
        remover_id = order.get("remover_id")
        if remover_id:
            await client.send_message(remover_id, f"Клиент не подтвердил выполнение заказа {oid}. Продолжите выполнение.", reply_markup=get_reply_keyboard("Выноситель"))
        await c.message.edit_text(f"Вы отказались подтвердить выполнение заказа {oid}. Заказ остаётся в работе.", reply_markup=get_reply_keyboard("Клиент"))
    await c.answer()


@app.on_message(filters.command("cancel") & filters.private)
async def cancel_command(client: Client, message: Message):
    uid = str(message.from_user.id)
    if uid in user_states:
        user_states.pop(uid, None)
        db = get_db()
        role = db["users"][uid]["role"] if uid in db["users"] else None
        await message.reply("Отменено.", reply_markup=get_reply_keyboard(role) if role else reg_kb())
    else:
        await message.reply("Нет активных действий.")


async def text_handler(client: Client, message: Message):
    if message.text.startswith("/"):
        return
    uid = str(message.from_user.id)
    state = user_states.get(uid)
    if not state:
        return

    if state["state"] == "waiting_for_city":
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
            db["users"][uid] = {
                "name": message.from_user.first_name,
                "role": new_role,
                "city": city,
                "consent_given": True,
                "consent_timestamp": datetime.now().isoformat()
            }
            save_db(db)
            log_audit("REGISTRATION_COMPLETE", uid, f"City:{city}")
            await message.reply(f"Регистрация завершена! Роль: {new_role}, Город: {city}", reply_markup=get_reply_keyboard(new_role))
        elif is_change:
            db["users"][uid]["role"] = new_role
            db["users"][uid]["city"] = city
            save_db(db)
            log_audit("PROFILE_UPDATE", uid, f"City:{city}, Role:{new_role}")
            await message.reply(f"Роль изменена на {new_role}. Город: {city}", reply_markup=get_reply_keyboard(new_role))
        else:
            db["users"][uid]["city"] = city
            save_db(db)
            log_audit("PROFILE_UPDATE", uid, f"City:{city}")
            # FIX: безопасное получение роли с дефолтом
            current_role = db["users"][uid].get("role", "Клиент")
            if current_role not in ["Клиент", "Выноситель", "Админ"]:
                current_role = "Клиент"
            await message.reply(f"Город изменён на {city}", reply_markup=get_reply_keyboard(current_role))
        user_states.pop(uid, None)
        return

    if state["state"] == "order_city_new":
        city_input = message.text.strip()
        status_msg = await message.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await status_msg.delete()
        if error:
            await message.reply(error)
            return
        user_states[uid] = {"state": "waiting_for_address", "city": city}
        await message.reply(f"Выбран город: {city}\nВведите адрес (улица, дом):")
        return

    if state["state"] == "waiting_for_address":
        addr = message.text.strip()
        if not addr:
            return await message.reply("Адрес не может быть пустым.")
        city = state.get("city")
        if city:
            log_audit("ORDER_ADDRESS_INPUT", uid, f"City:{city}")
            await ask_order_details(client, uid, city, addr)
        else:
            await message.reply("Ошибка: не указан город. Начните заказ заново.", reply_markup=get_reply_keyboard("Клиент"))
        user_states.pop(uid, None)
        return

    if state["state"] == "waiting_for_details":
        details = message.text.strip()
        city = state.get("city")
        address = state.get("address")
        await create_order_final(client, uid, city, address, details=details if details else None)
        user_states.pop(uid, None)
        return

    user_states.pop(uid, None)
    await message.reply("Сбой состояния. Начните с /start")

async def cleanup_expired_data():
    db = get_db()
    now = datetime.now()
    cutoff = now - timedelta(days=DATA_RETENTION_DAYS)
    deleted_count = 0

    for oid in list(db["orders"].keys()):
        try:
            order_date = datetime.fromisoformat(db["orders"][oid].get("created_at", now.isoformat()))
            if order_date < cutoff and db["orders"][oid].get("status") == "done":
                del db["orders"][oid]
                deleted_count += 1
        except:
            continue

    if deleted_count > 0:
        save_db(db)
        logger.info(f"Cleanup: removed {deleted_count} expired orders")

app.run()
