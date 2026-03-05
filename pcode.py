import asyncio
import json
import os
from datetime import datetime
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton as IKB
TOKEN = "sometoken"
FILE = "data.json"

bot = Bot(token=TOKEN)
dp = Dispatcher()

def get_db():
    if os.path.exists(FILE):
        try:
            with open(FILE, "r", encoding="utf-8") as f:
                content = f.read()
                if not content:
                    return {}
                return json.loads(content)
        except (json.JSONDecodeError, Exception):
            return {}
    return {}

def save_db(data):
    with open(FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

kb = InlineKeyboardMarkup(inline_keyboard=[
    [IKB(text=t, callback_data=f"reg_{t}")] for t in ["Пользователь", "Выноситель", "Админ"]
])

@dp.message(CommandStart())
async def start(m):
    db = get_db()
    uid = str(m.from_user.id)
    if uid in db:
        await m.answer(f"Вы уже: {db[uid]['type']} ({db[uid]['date']})")
    else:
        await m.answer("Выберите тип:", reply_markup=kb)

@dp.callback_query(F.data.startswith("reg_"))
async def reg(c):
    uid = str(c.from_user.id)
    db = get_db()

    if uid in db:
        return await c.answer("Уже зарегистрированы", show_alert=True)

    u_type = c.data.split("_", 1)[1]
    name = c.from_user.username or c.from_user.first_name

    db[uid] = {
        "name": name,
        "type": u_type,
        "date": datetime.now().strftime("%Y-%m-%d")
    }
    save_db(db)

    await c.message.edit_text(f"OK: {name}\nТип: {u_type}")
    await c.answer()

@dp.message(F.text)
async def echo(m):
    if str(m.from_user.id) not in get_db():
        await m.answer("/start для регистрации")
async def main():
    await dp.start_polling(bot)
asyncio.run(main())
