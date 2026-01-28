from aiogram import Router, types
from aiogram.filters import StateFilter
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.db.connection import db_function
from app.filters.category_name import CategoryNameFilter
from app.states.finance import WriteEarnings
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(lambda m: m.text == 'Доход', StateFilter(None))
async def choose_category(message: Message, state: FSMContext) -> None:
    categories = await db_function('get_categories_name', message.chat.id, 10)
    await state.update_data(categorys=categories)
    kb = [
        [types.KeyboardButton(text=f'{categories[j]}') for j in range(i, i + 2) if j < len(categories)]
        for i in range(0, len(categories), 2)
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('откуда', reply_markup=keyboard)
    await state.set_state(WriteEarnings.choosing_category)


@router.message(WriteEarnings.choosing_category, CategoryNameFilter(10))
async def ask_sum(message: Message, state: FSMContext) -> None:
    await state.update_data(category=message.text)
    await message.answer('Сколько?', reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(WriteEarnings.writing_value)


@router.message(lambda x: x.text.split()[0].replace('.', '').replace(',', '').isdigit(), WriteEarnings.writing_value)
async def write_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category = data.get('category')
    value = message.text.split(' ', 2)
    value[0] = float(value[0].replace(',', '.'))
    await db_function('insert_revenue', message.chat.id, category, *value)
    await message.answer('OK', reply_markup=create_default_keyboard())
    await state.clear()
