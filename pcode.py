import json
import os
import struct
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = "УДАЛЕН"
API_ID = 0
API_HASH = "УДАЛЕН"
FILE = "data.json"

GOST_KEY = b'0123456789abcdef0123456789abcdef'

app = Client(
    "bot_session_name",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

user_states = {}

_SBOX = [
    [4, 10, 9, 2, 13, 8, 0, 14, 6, 11, 1, 12, 7, 15, 5, 3],
    [14, 11, 4, 12, 6, 13, 15, 10, 2, 3, 8, 1, 0, 7, 5, 9],
    [5, 8, 1, 13, 10, 3, 4, 2, 14, 15, 12, 7, 6, 0, 9, 11],
    [7, 13, 10, 1, 0, 8, 9, 15, 14, 4, 6, 12, 11, 2, 5, 3],
    [6, 12, 7, 1, 5, 15, 13, 8, 4, 10, 9, 14, 0, 3, 11, 2],
    [4, 11, 10, 0, 7, 2, 1, 13, 3, 6, 8, 5, 9, 12, 15, 14],
    [13, 11, 4, 1, 3, 15, 5, 9, 0, 10, 14, 7, 6, 8, 2, 12],
    [1, 15, 13, 0, 5, 7, 10, 4, 9, 2, 3, 14, 6, 11, 8, 12]
]


def _get_keys(key_bytes):
    return [struct.unpack('<I', key_bytes[i:i + 4])[0] for i in range(0, 32, 4)]


def _substitute(block):
    res = 0
    for i in range(8):
        idx = (block >> (4 * i)) & 0xF
        res |= _SBOX[7 - i][idx] << (4 * i)
    return res


def _encrypt_block(block, keys):
    n1 = block & 0xFFFFFFFF
    n0 = (block >> 32) & 0xFFFFFFFF
    for i in range(24):
        n1 = (_substitute((n1 + keys[i % 8]) & 0xFFFFFFFF) ^ n0) & 0xFFFFFFFF
        n0, n1 = n1, n0
    for i in range(7, -1, -1):
        n1 = (_substitute((n1 + keys[i]) & 0xFFFFFFFF) ^ n0) & 0xFFFFFFFF
        n0, n1 = n1, n0
    return (n1 << 32) | n0


def _decrypt_block(block, keys):
    n1 = block & 0xFFFFFFFF
    n0 = (block >> 32) & 0xFFFFFFFF
    for i in range(8):
        n1 = (_substitute((n1 + keys[i]) & 0xFFFFFFFF) ^ n0) & 0xFFFFFFFF
        n0, n1 = n1, n0
    for i in range(24):
        n1 = (_substitute((n1 + keys[7 - (i % 8)]) & 0xFFFFFFFF) ^ n0) & 0xFFFFFFFF
        n0, n1 = n1, n0
    return (n1 << 32) | n0


def _pkcs7_pad(data):
    pad_len = 8 - (len(data) % 8)
    return data + bytes([pad_len] * pad_len)


def _pkcs7_unpad(data):
    return data[:-data[-1]]


def encrypt_data(data_str):
    keys = _get_keys(GOST_KEY)
    data_bytes = _pkcs7_pad(data_str.encode('utf-8'))
    res = b''
    for i in range(0, len(data_bytes), 8):
        block = struct.unpack('>Q', data_bytes[i:i + 8])[0]
        res += struct.pack('>Q', _encrypt_block(block, keys))
    return res


def decrypt_data(data_bytes):
    keys = _get_keys(GOST_KEY)
    res = b''
    for i in range(0, len(data_bytes), 8):
        block = struct.unpack('>Q', data_bytes[i:i + 8])[0]
        res += struct.pack('>Q', _decrypt_block(block, keys))
    return _pkcs7_unpad(res).decode('utf-8')


def get_db():
    if os.path.exists(FILE):
        try:
            with open(FILE, "rb") as f:
                content = f.read()
                if not content: return {"users": {}, "orders": {}}
                decrypted = decrypt_data(content)
                data = json.loads(decrypted)
                if "users" not in data: data["users"] = {}
                if "orders" not in data: data["orders"] = {}
                return data
        except Exception:
            return {"users": {}, "orders": {}}
    return {"users": {}, "orders": {}}


def save_db(data):
    json_str = json.dumps(data, ensure_ascii=False, indent=2)
    encrypted = encrypt_data(json_str)
    with open(FILE, "wb") as f:
        f.write(encrypted)


def reg_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Клиент", callback_data="reg_Клиент")],
        [InlineKeyboardButton(text="Выноситель", callback_data="reg_Выноситель")],
        [InlineKeyboardButton(text="Админ", callback_data="reg_admin")]
    ])


def client_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Заказать вынос", callback_data="new_order")],
        [InlineKeyboardButton(text="Мои заказы", callback_data="my_orders")],
        [InlineKeyboardButton(text="Сменить роль", callback_data="change_role")]
    ])


def remover_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Найти заказ", callback_data="find_orders")],
        [InlineKeyboardButton(text="Завершить заказ", callback_data="finish_order")],
        [InlineKeyboardButton(text="Сменить роль", callback_data="change_role")]
    ])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="Удалить пользователя", callback_data="del_user")]
    ])


@app.on_message(filters.command("start") & filters.private)
async def start(client, m):
    db = get_db()
    uid = str(m.from_user.id)
    if uid in db["users"]:
        await show_main_menu(m, db["users"][uid]["role"])
    else:
        await m.reply("Привет! Выбери свою роль:", reply_markup=reg_kb())


async def show_main_menu(m, role):
    if role == "Клиент":
        await m.reply("Меню клиента:", reply_markup=client_kb())
    elif role == "Выноситель":
        await m.reply("Меню выносителя:", reply_markup=remover_kb())
    elif role == "Админ":
        await m.reply("Меню администратора:", reply_markup=admin_kb())


@app.on_callback_query(filters.regex("^change_role"))
async def change_role(client, c):
    await c.message.edit_text("Выберите новую роль:", reply_markup=reg_kb())
    await c.answer()


@app.on_callback_query(filters.regex("^reg_admin"))
async def reg_admin_prompt(client, c):
    uid = str(c.from_user.id)
    user_states[uid] = "waiting_admin_pass"
    await c.message.edit_text("Введите пароль для роли Администратора:")
    await c.answer()


@app.on_callback_query(filters.regex("^reg_(Клиент|Выноситель)"))
async def reg(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    role = c.data.split("_", 1)[1]

    if uid in db["users"]:
        has_unfinished = False
        for o in db["orders"].values():
            if o["status"] in ["pending", "active"] and (o["client_id"] == uid or o["remover_id"] == uid):
                has_unfinished = True
                break
        if has_unfinished:
            return await c.answer("Сначала завершите все текущие заказы", show_alert=True)

    name = c.from_user.username or c.from_user.first_name or "Аноним"
    is_new = uid not in db["users"]
    db["users"][uid] = {"name": name, "role": role}
    save_db(db)

    text = "Регистрация успешна!" if is_new else "Роль успешно изменена!"
    await c.message.edit_text(f"{text}\nВы теперь: {role}")
    await show_main_menu(c.message, role)
    await c.answer()


@app.on_callback_query(filters.regex("^new_order"))
async def new_order(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    if db["users"][uid]["role"] != "Клиент":
        return await c.answer("Доступно только клиентам", show_alert=True)
    user_states[uid] = "waiting_for_address"
    await c.message.edit_text("Напишите адрес дома:")
    await c.answer()


@app.on_callback_query(filters.regex("^my_orders"))
async def my_orders(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    text = "Ваши заказы:\n\n"
    has_orders = False
    for oid, order in db["orders"].items():
        if order["client_id"] == uid:
            has_orders = True
            status = order.get("status", "Неизвестно")
            status_text = {"pending": "Ожидает", "active": "В работе", "done": "Выполнен"}.get(status, "Неизвестно")
            addr = order.get("address", "")
            text += f"Заказ <b>{oid}</b>\nАдрес: {addr}\nСтатус: {status_text}\n\n"
    if not has_orders:
        text = "У вас пока нет заказов."
    await c.message.edit_text(text, reply_markup=client_kb())
    await c.answer()


@app.on_callback_query(filters.regex("^find_orders"))
async def find_orders(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    for oid, order in db["orders"].items():
        if order["remover_id"] == uid and order["status"] == "active":
            await c.answer("Сначала завершите текущий заказ!", show_alert=True)
            return

    kb = InlineKeyboardMarkup(inline_keyboard=[])
    has_orders = False
    for oid, order in db["orders"].items():
        if order["status"] == "pending":
            has_orders = True
            addr = order.get("address", "")
            kb.inline_keyboard.append(
                [InlineKeyboardButton(text=f"{oid} | {addr}", callback_data=f"take_{oid}")]
            )

    if not has_orders:
        await c.message.edit_text("Свободных заказов нет.", reply_markup=remover_kb())
    else:
        kb.inline_keyboard.append([InlineKeyboardButton(text="Назад", callback_data="back_remover")])
        await c.message.edit_text("Доступные заказы:", reply_markup=kb)
    await c.answer()


@app.on_callback_query(filters.regex("^take_"))
async def take_order(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    oid = c.data.split("_", 1)[1]
    if oid in db["orders"] and db["orders"][oid]["status"] == "pending":
        db["orders"][oid]["status"] = "active"
        db["orders"][oid]["remover_id"] = uid
        save_db(db)

        remover_name = db["users"][uid]["name"]
        client_id = db["orders"][oid]["client_id"]
        try:
            await client.send_message(
                client_id,
                f"Выноситель найден!\nЗаказ {oid} берет {remover_name}.",
                reply_markup=client_kb()
            )
        except Exception:
            pass

        order_addr = db["orders"][oid].get("address", "")
        await c.message.edit_text(f"Вы взяли заказ <b>{oid}</b>\nАдрес: {order_addr}\n\nСпешите!",
                                  reply_markup=remover_kb())
    else:
        await c.answer("Этот заказ уже кто-то взял", show_alert=True)


@app.on_callback_query(filters.regex("^finish_order"))
async def finish_order(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    active_order_id = None
    for oid, order in db["orders"].items():
        if order["remover_id"] == uid and order["status"] == "active":
            active_order_id = oid
            break

    if not active_order_id:
        return await c.answer("У вас нет активных заказов", show_alert=True)

    db["orders"][active_order_id]["status"] = "done"
    save_db(db)

    client_id = db["orders"][active_order_id]["client_id"]
    try:
        await client.send_message(client_id, f"Заказ <b>{active_order_id}</b> выполнен!",
                                  reply_markup=client_kb())
    except Exception:
        pass

    await c.message.edit_text(f"Заказ <b>{active_order_id}</b> успешно завершен!",
                              reply_markup=remover_kb())
    await c.answer()


@app.on_callback_query(filters.regex("^back_remover"))
async def back_remover(client, c):
    await c.message.edit_text("Меню выносителя:", reply_markup=remover_kb())
    await c.answer()


@app.on_callback_query(filters.regex("^del_user"))
async def del_user(client, c):
    uid = str(c.from_user.id)
    db = get_db()
    if db["users"][uid]["role"] != "Админ":
        return await c.answer("Нет прав", show_alert=True)
    user_states[uid] = "waiting_for_delete_id"
    await c.message.edit_text("Введите ID пользователя для удаления:")
    await c.answer()


@app.on_callback_query(filters.regex("^admin_stats"))
async def admin_stats(client, c):
    db = get_db()
    pending = sum(1 for o in db["orders"].values() if o["status"] == "pending")
    active = sum(1 for o in db["orders"].values() if o["status"] == "active")
    done = sum(1 for o in db["orders"].values() if o["status"] == "done")
    users_count = len(db["users"])

    text = (
        "Статистика TrashOut:\n\n"
        f"Пользователей: {users_count}\n"
        f"Ожидают выполнения: {pending}\n"
        f"В работе сейчас: {active}\n"
        f"Выполнено всего: {done}"
    )
    await c.message.edit_text(text, reply_markup=admin_kb())
    await c.answer()


@app.on_message(filters.text & filters.private)
async def text_handler(client, m):
    if m.text.startswith("/"):
        return

    uid = str(m.from_user.id)
    db = get_db()
    state = user_states.get(uid)

    if uid not in db["users"]:
        await m.reply("Пожалуйста, начните с /start")
        return

    if state == "waiting_admin_pass":
        user_states.pop(uid, None)
        if m.text == "123":
            name = m.from_user.username or m.from_user.first_name or "Аноним"
            db["users"][uid] = {"name": name, "role": "Админ"}
            save_db(db)
            await m.reply("Пароль верный!\nРегистрация успешна!\nВы теперь: Админ", reply_markup=admin_kb())
        else:
            await m.reply("Неверный пароль!", reply_markup=reg_kb())

    elif state == "waiting_for_address":
        user_states[uid] = "waiting_for_details"
        user_states[f"{uid}_addr"] = m.text
        await m.reply("Напишите детали (этаж, пакеты):")

    elif state == "waiting_for_details":
        address = user_states.pop(f"{uid}_addr")
        details = m.text
        user_states.pop(uid)

        order_id = f"ORD-{int(datetime.now().timestamp())}"
        db["orders"][order_id] = {
            "client_id": uid,
            "address": address,
            "details": details,
            "status": "pending",
            "remover_id": None,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
        save_db(db)

        await m.reply(
            f"Заказ <b>{order_id}</b> создан!\n\nАдрес: {address}\nДетали: {details}\n\nИщем выносителя...",
            reply_markup=client_kb()
        )

    elif state == "waiting_for_delete_id":
        target_id = m.text.strip()
        user_states.pop(uid, None)

        if target_id in db["users"]:
            orders_to_del = [oid for oid, o in db["orders"].items() if
                             o["client_id"] == target_id or o["remover_id"] == target_id]

            for oid in orders_to_del:
                del db["orders"][oid]

            del db["users"][target_id]
            save_db(db)
            await m.reply(f"Пользователь {target_id} и его заказы успешно удалены.", reply_markup=admin_kb())
        else:
            await m.reply("Пользователь не найден.", reply_markup=admin_kb())

    else:
        role = db["users"][uid]["role"]
        kb = client_kb() if role == "Клиент" else remover_kb() if role == "Выноситель" else admin_kb()
        await m.reply("Воспользуйтесь меню кнопок ниже", reply_markup=kb)


print("Бот запущен...")
app.run()
