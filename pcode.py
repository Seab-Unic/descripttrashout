import json
import os
import re
from datetime import datetime
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

BOT_TOKEN = ""
API_ID = 0
API_HASH = ""
FILE = "data.json"
ADMIN_PASSWORD = "123"

DADATA_API_KEY = "ВАШ_API_KEY"
DADATA_SECRET_KEY = "ВАШ_SECRET_KEY"

app = Client("bot_session_name", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_states = {}

def get_db():
    if not os.path.exists(FILE):
        return {"users": {}, "orders": {}}
    try:
        with open(FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "users" not in data:
                data["users"] = {}
            if "orders" not in data:
                data["orders"] = {}
            return data
    except:
        return {"users": {}, "orders": {}}

def save_db(data):
    try:
        with open(FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except:
        return False

def is_likely_city(text: str) -> bool:
    if not text or not text.strip():
        return False
    cleaned = text.strip()
    if re.fullmatch(r'[\d\s\.,\-_]+', cleaned):
        return False
    if not re.search(r'[а-яё]', cleaned, re.IGNORECASE):
        return False
    return True

async def validate_city(city_name: str):
    if not is_likely_city(city_name):
        return None, "Это не похоже на название города. Пожалуйста, введите реальный город (только буквы)."
    if not DADATA_API_KEY or DADATA_API_KEY == "ВАШ_API_KEY":
        return city_name.strip().title(), None
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/address"
    headers = {
        "Authorization": f"Token {DADATA_API_KEY}",
        "X-Secret": DADATA_SECRET_KEY,
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
        json_payload = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=json_payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("suggestions"):
                        city_data = data["suggestions"][0]["data"]
                        normalized = city_data.get("city") or city_data.get("settlement")
                        if normalized:
                            return normalized, None
                        else:
                            return None, f"Не удалось определить город для '{city_name}'."
                    else:
                        return None, f"Город '{city_name}' не найден. Проверьте название."
                else:
                    error_text = await response.text()
                    return None, f"Ошибка DaData ({response.status}): {error_text}"
    except Exception as e:
        return None, f"Ошибка подключения к DaData: {str(e)}"

def get_reply_keyboard(role):
    if role == "Клиент":
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Заказать вынос"), KeyboardButton("Мои заказы")],
             [KeyboardButton("Сменить роль"), KeyboardButton("Сменить город")]],
            resize_keyboard=True
        )
    elif role == "Выноситель":
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Найти заказ"), KeyboardButton("Завершить заказ")],
             [KeyboardButton("Сменить роль"), KeyboardButton("Сменить город")]],
            resize_keyboard=True
        )
    elif role == "Админ":
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Статистика")],
             [KeyboardButton("Удалить пользователя")],
             [KeyboardButton("Сменить роль")]],
            resize_keyboard=True
        )
    return None

def reg_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Клиент", callback_data="reg_Клиент")],
        [InlineKeyboardButton("Выноситель", callback_data="reg_Выноситель")],
        [InlineKeyboardButton("Админ", callback_data="reg_admin")]
    ])

async def list_users_for_deletion(client, m):
    db = get_db()
    uid_admin = str(m.from_user.id)
    other_users = {uid: data for uid, data in db["users"].items() if uid != uid_admin}
    if not other_users:
        await m.reply("Нет других пользователей.", reply_markup=get_reply_keyboard("Админ"))
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for uid, user in other_users.items():
        name = user.get("name", "Без имени")
        username = f" @{user.get('username')}" if user.get('username') else ""
        label = f"{name}{username} [{user.get('role', '')}]"
        kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"del_confirm_{uid}")])
    await m.reply("Выберите пользователя для удаления:", reply_markup=kb)

@app.on_callback_query(filters.regex(r"^del_confirm_"))
async def confirm_delete_user(client, c):
    uid = c.data.split("_", 2)[2]
    db = get_db()
    if uid not in db["users"]:
        return await c.answer("Уже удалён", show_alert=True)
    user = db["users"][uid]
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Да", callback_data=f"del_final_{uid}")],
        [InlineKeyboardButton("Отмена", callback_data="cancel_delete")]
    ])
    try:
        await c.message.edit_text(f"Удалить {user.get('name')} (роль: {user.get('role')})?", reply_markup=kb)
    except:
        pass
    await c.answer()

@app.on_callback_query(filters.regex(r"^del_final_"))
async def delete_user_final(client, c):
    uid = c.data.split("_", 2)[2]
    db = get_db()
    if uid not in db["users"]:
        return await c.message.edit_text("Уже удалён.")
    for oid in list(db["orders"].keys()):
        if db["orders"][oid].get("client_id") == uid or db["orders"][oid].get("remover_id") == uid:
            if db["orders"][oid].get("client_id") == uid and db["orders"][oid].get("remover_id"):
                try:
                    await client.send_message(db["orders"][oid]["remover_id"], f"Пользователь удалён, заказ {oid} аннулирован.")
                except:
                    pass
            elif db["orders"][oid].get("remover_id") == uid and db["orders"][oid].get("client_id"):
                try:
                    await client.send_message(db["orders"][oid]["client_id"], f"Выноситель удалён, заказ {oid} аннулирован.")
                except:
                    pass
            del db["orders"][oid]
    name = db["users"][uid].get("name", "Пользователь")
    del db["users"][uid]
    save_db(db)
    await c.message.edit_text(f"{name} удалён.")
    await c.message.reply("Меню администратора:", reply_markup=get_reply_keyboard("Админ"))
    await c.answer()

@app.on_callback_query(filters.regex("^cancel_delete$"))
async def cancel_delete(client, c):
    try:
        await c.message.edit_text("Отменено.", reply_markup=get_reply_keyboard("Админ"))
    except:
        pass
    await c.answer()

async def ask_city(client, uid, role, is_role_change=False):
    user_states[uid] = {"state": "waiting_for_city", "role": role, "is_role_change": is_role_change}
    await client.send_message(uid, "Введите ваш город (например: Москва, Новосибирск):")

async def ask_order_details(client, uid, city, address):
    user_states[uid] = {"state": "waiting_for_details", "city": city, "address": address}
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="skip_details")]])
    await client.send_message(uid, "Введите дополнительное описание заказа (например, этаж, код домофона, вес, особые приметы).\nИли нажмите 'Пропустить':", reply_markup=kb)

async def create_order_final(client, uid, city, address, details=None):
    db = get_db()
    order_id = f"ORD-{int(datetime.now().timestamp())}"
    db["orders"][order_id] = {
        "client_id": uid, "city": city, "address": address, "details": details,
        "status": "pending", "remover_id": None, "date": datetime.now().strftime("%Y-%m-%d %H:%M")
    }
    if save_db(db):
        await client.send_message(uid, f"Заказ {order_id} создан! Ищем выносителя...", reply_markup=get_reply_keyboard("Клиент"))
    else:
        await client.send_message(uid, "Ошибка создания заказа.", reply_markup=get_reply_keyboard("Клиент"))

@app.on_message(filters.command("start") & filters.private)
async def start(client, m):
    db = get_db()
    uid = str(m.from_user.id)
    if uid in db["users"]:
        role = db["users"][uid]["role"]
        await m.reply(f"С возвращением! Вы вошли как {role}.", reply_markup=get_reply_keyboard(role))
    else:
        await m.reply("Привет! Выберите роль:", reply_markup=reg_kb())

@app.on_message(filters.text & filters.private & ~filters.command(["start", "cancel"]))
async def handle_reply_buttons(client, m):
    uid = str(m.from_user.id)
    if uid in user_states:
        await text_handler(client, m)
        return
    db = get_db()
    if uid not in db["users"]:
        await m.reply("Начните с /start", reply_markup=reg_kb())
        return
    role = db["users"][uid]["role"]
    text = m.text.strip()
    if role == "Клиент":
        if "Заказать вынос" in text:
            city = db["users"][uid].get("city")
            if city:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{city} (оставить)", callback_data="order_city_keep")],
                    [InlineKeyboardButton("Ввести другой", callback_data="order_city_change")]
                ])
                await m.reply(f"Ваш город: {city}. Использовать?", reply_markup=kb)
            else:
                await ask_city(client, uid, "Клиент", is_role_change=False)
        elif "Мои заказы" in text:
            await my_orders_command(client, m)
        elif "Сменить роль" in text:
            await m.reply("Выберите новую роль:", reply_markup=reg_kb())
        elif "Сменить город" in text:
            await ask_city(client, uid, role, is_role_change=False)
        else:
            await m.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    elif role == "Выноситель":
        if "Найти заказ" in text:
            await find_orders_command(client, m)
        elif "Завершить заказ" in text:
            await finish_order_command(client, m)
        elif "Сменить роль" in text:
            await m.reply("Выберите новую роль:", reply_markup=reg_kb())
        elif "Сменить город" in text:
            await ask_city(client, uid, role, is_role_change=False)
        else:
            await m.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    elif role == "Админ":
        if "Статистика" in text:
            await admin_stats_command(client, m)
        elif "Удалить пользователя" in text:
            await list_users_for_deletion(client, m)
        elif "Сменить роль" in text:
            await m.reply("Выберите новую роль:", reply_markup=reg_kb())
        else:
            await m.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))
    else:
        await m.reply("Используйте кнопки меню.", reply_markup=get_reply_keyboard(role))

async def my_orders_command(client, m):
    uid = str(m.from_user.id)
    db = get_db()
    text = "Ваши заказы:\n\n"
    has = False
    status_map = {"pending": "Ожидает", "awaiting_confirm": "Ожидает подтверждения", "active": "В работе",
                  "waiting_complete": "Ожидает подтверждения выполнения", "done": "Выполнен"}
    for oid, order in db["orders"].items():
        if order["client_id"] == uid:
            has = True
            details = f"\nОписание: {order.get('details', 'Нет')}" if order.get('details') else ""
            text += f"Заказ {oid}\nГород: {order.get('city', '')}\nАдрес: {order.get('address', '')}{details}\nСтатус: {status_map.get(order['status'], 'Неизвестно')}\n\n"
    await m.reply(text if has else "У вас пока нет заказов.", reply_markup=get_reply_keyboard("Клиент"))

async def find_orders_command(client, m):
    uid = str(m.from_user.id)
    db = get_db()
    user_city = db["users"][uid].get("city")
    if not user_city:
        return await m.reply("Сначала укажите город через 'Сменить город'.", reply_markup=get_reply_keyboard("Выноситель"))
    for oid, order in db["orders"].items():
        if order.get("remover_id") == uid and order.get("status") == "active":
            return await m.reply("Сначала завершите текущий заказ!", reply_markup=get_reply_keyboard("Выноситель"))
    kb = InlineKeyboardMarkup([])
    has = False
    for oid, order in db["orders"].items():
        if order.get("status") == "pending" and order.get("city") == user_city:
            has = True
            address = order.get('address', 'без адреса')
            details = order.get('details')
            label = f"{oid} | {address}"
            if details:
                label += f" | {details[:20]}..."
            kb.inline_keyboard.append([InlineKeyboardButton(label, callback_data=f"take_{oid}")])
    if not has:
        await m.reply(f"Свободных заказов в {user_city} нет.", reply_markup=get_reply_keyboard("Выноситель"))
    else:
        kb.inline_keyboard.append([InlineKeyboardButton("Назад", callback_data="back_remover")])
        await m.reply(f"Доступные заказы в {user_city}:", reply_markup=kb)

async def finish_order_command(client, m):
    uid = str(m.from_user.id)
    db = get_db()
    active = next((oid for oid, o in db["orders"].items() if o.get("remover_id") == uid and o.get("status") == "active"), None)
    if not active:
        return await m.reply("Нет активных заказов.", reply_markup=get_reply_keyboard("Выноситель"))
    db["orders"][active]["status"] = "waiting_complete"
    save_db(db)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Подтвердить выполнение", callback_data=f"complete_yes_{active}")],
        [InlineKeyboardButton("Отклонить (заказ продолжается)", callback_data=f"complete_no_{active}")]
    ])
    try:
        await client.send_message(db["orders"][active]["client_id"], f"Выноситель {db['users'][uid]['name']} отметил заказ {active} как выполненный.\nПодтверждаете?", reply_markup=kb)
        await m.reply(f"Запрос на подтверждение выполнения заказа {active} отправлен клиенту.", reply_markup=get_reply_keyboard("Выноситель"))
    except:
        db["orders"][active]["status"] = "active"
        save_db(db)
        await m.reply("Не удалось связаться с клиентом. Заказ остаётся активным.", reply_markup=get_reply_keyboard("Выноситель"))

async def admin_stats_command(client, m):
    db = get_db()
    stats = {s: sum(1 for o in db["orders"].values() if o["status"] == s) for s in ["pending", "awaiting_confirm", "active", "waiting_complete", "done"]}
    text = f"Статистика\nПользователей: {len(db['users'])}\nОжидают: {stats['pending']}\nЖдут подтверждения: {stats['awaiting_confirm']}\nВ работе: {stats['active']}\nЖдут подтверждения выполнения: {stats['waiting_complete']}\nВыполнено: {stats['done']}"
    await m.reply(text, reply_markup=get_reply_keyboard("Админ"))

@app.on_callback_query()
async def handle_callbacks(client, c):
    data = c.data
    uid = str(c.from_user.id)
    if data.startswith("reg_admin"):
        db = get_db()
        if uid in db["users"] and db["users"][uid]["role"] == "Админ":
            return await c.answer("Вы уже админ", show_alert=True)
        user_states[uid] = {"state": "waiting_admin_pass"}
        await c.message.reply("Введите пароль администратора:")
        await c.answer()
    elif data.startswith("reg_Клиент") or data.startswith("reg_Выноситель"):
        role = data.split("_", 1)[1]
        db = get_db()
        if uid in db["users"]:
            has_active = any(o["status"] in ["pending", "active", "awaiting_confirm", "waiting_complete"] and (o["client_id"] == uid or o["remover_id"] == uid) for o in db["orders"].values())
            if has_active:
                return await c.answer("Завершите текущие заказы", show_alert=True)
        await c.message.delete()
        if uid in db["users"] and db["users"][uid].get("city"):
            db["users"][uid]["role"] = role
            save_db(db)
            await client.send_message(uid, f"Роль изменена на {role}", reply_markup=get_reply_keyboard(role))
        else:
            await ask_city(client, uid, role, is_role_change=True)
        await c.answer()
    elif data == "order_city_keep":
        db = get_db()
        city = db["users"][uid].get("city")
        if city:
            user_states[uid] = {"state": "waiting_for_address", "city": city}
            await c.message.edit_text("Введите адрес (улица, дом):")
        await c.answer()
    elif data == "order_city_change":
        user_states[uid] = {"state": "order_city_new"}
        await c.message.edit_text("Введите город для заказа:")
        await c.answer()
    elif data.startswith("take_"):
        oid = data.split("_", 1)[1]
        db = get_db()
        order = db["orders"].get(oid)
        if not order or order["status"] != "pending":
            return await c.answer("Заказ недоступен", show_alert=True)
        if db["users"][uid].get("city") != order.get("city"):
            return await c.answer("Не ваш город", show_alert=True)
        order["status"] = "awaiting_confirm"
        order["remover_id"] = uid
        save_db(db)
        details_text = f"\nОписание: {order['details']}" if order.get('details') else ""
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Подтвердить", callback_data=f"confirm_yes_{oid}")],
            [InlineKeyboardButton("Отказать", callback_data=f"confirm_no_{oid}")]
        ])
        await client.send_message(order["client_id"], f"Выноситель {db['users'][uid]['name']} хочет взять ваш заказ {oid}.\nАдрес: {order['address']}{details_text}\n\nПодтверждаете?", reply_markup=kb)
        try:
            await c.message.delete()
        except:
            pass
        await c.message.reply("Запрос отправлен клиенту.", reply_markup=get_reply_keyboard("Выноситель"))
        await c.answer()
    elif data.startswith("confirm_yes_") or data.startswith("confirm_no_"):
        oid = data.split("_", 2)[2]
        approved = data.startswith("confirm_yes_")
        await process_confirmation(client, c, uid, oid, approved)
    elif data.startswith("complete_yes_") or data.startswith("complete_no_"):
        oid = data.split("_", 2)[2]
        approved = data.startswith("complete_yes_")
        await process_completion_confirmation(client, c, uid, oid, approved)
    elif data == "skip_details":
        state = user_states.get(uid)
        if state and state.get("state") == "waiting_for_details":
            city = state.get("city")
            address = state.get("address")
            await create_order_final(client, uid, city, address, details=None)
            user_states.pop(uid, None)
            try:
                await c.message.delete()
            except:
                pass
        await c.answer()
    elif data == "back_remover":
        try:
            await c.message.delete()
        except:
            pass
        await c.message.reply("Меню выносителя:", reply_markup=get_reply_keyboard("Выноситель"))
        await c.answer()
    elif data == "admin_stats":
        await admin_stats_command(client, c.message)
        try:
            await c.message.delete()
        except:
            pass
        await c.answer()

async def process_confirmation(client, c, uid, oid, approved):
    db = get_db()
    order = db["orders"].get(oid)
    if not order or order["client_id"] != uid or order["status"] != "awaiting_confirm":
        return await c.answer("Заказ обработан", show_alert=True)
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

async def process_completion_confirmation(client, c, uid, oid, approved):
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
async def cancel_command(client, m):
    uid = str(m.from_user.id)
    if uid in user_states:
        user_states.pop(uid, None)
        db = get_db()
        role = db["users"][uid]["role"] if uid in db["users"] else None
        await m.reply("Отменено.", reply_markup=get_reply_keyboard(role) if role else reg_kb())
    else:
        await m.reply("Нет активных действий.")

async def text_handler(client, m):
    if m.text.startswith("/"): return
    uid = str(m.from_user.id)
    state = user_states.get(uid)
    if not state: return
    if state["state"] == "waiting_admin_pass":
        if m.text == ADMIN_PASSWORD:
            db = get_db()
            if uid not in db["users"]:
                db["users"][uid] = {"name": m.from_user.first_name, "role": "Админ"}
            else:
                db["users"][uid]["role"] = "Админ"
            save_db(db)
            await m.reply("Вы Администратор.", reply_markup=get_reply_keyboard("Админ"))
        else:
            await m.reply("Неверный пароль.", reply_markup=reg_kb())
        user_states.pop(uid, None)
        return
    if state["state"] == "waiting_for_city":
        city_input = m.text.strip()
        status_msg = await m.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await status_msg.delete()
        if error:
            await m.reply(error)
            return
        db = get_db()
        new_role = state.get("role")
        is_change = state.get("is_role_change", False)
        if uid not in db["users"]:
            db["users"][uid] = {"name": m.from_user.first_name, "role": new_role, "city": city}
            save_db(db)
            await m.reply(f"Регистрация завершена! Роль: {new_role}, Город: {city}", reply_markup=get_reply_keyboard(new_role))
        elif is_change:
            db["users"][uid]["role"] = new_role
            db["users"][uid]["city"] = city
            save_db(db)
            await m.reply(f"Роль изменена на {new_role}. Город: {city}", reply_markup=get_reply_keyboard(new_role))
        else:
            db["users"][uid]["city"] = city
            save_db(db)
            await m.reply(f"Город изменён на {city}", reply_markup=get_reply_keyboard(db["users"][uid]["role"]))
        user_states.pop(uid, None)
        return
    if state["state"] == "order_city_new":
        city_input = m.text.strip()
        status_msg = await m.reply("Проверяю город...")
        city, error = await validate_city(city_input)
        await status_msg.delete()
        if error:
            await m.reply(error)
            return
        user_states[uid] = {"state": "waiting_for_address", "city": city}
        await m.reply(f"Выбран город: {city}\nВведите адрес (улица, дом):")
        return
    if state["state"] == "waiting_for_address":
        addr = m.text.strip()
        if not addr:
            return await m.reply("Адрес не может быть пустым.")
        city = state.get("city")
        if city:
            await ask_order_details(client, uid, city, addr)
        else:
            await m.reply("Ошибка: не указан город. Начните заказ заново.", reply_markup=get_reply_keyboard("Клиент"))
            user_states.pop(uid, None)
        return
    if state["state"] == "waiting_for_details":
        details = m.text.strip()
        city = state.get("city")
        address = state.get("address")
        await create_order_final(client, uid, city, address, details=details if details else None)
        user_states.pop(uid, None)
        return
    user_states.pop(uid, None)
    await m.reply("Сбой состояния. Начните с /start")
    app.run()
