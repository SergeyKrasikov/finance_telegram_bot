from aiogram import Router, types
from aiogram.filters import StateFilter
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.config import GROUP_EARNINGS
from app.db.categories import get_categories_name
from app.db.transactions import insert_revenue
from app.parsers.input import is_number_input, parse_amount_with_defaults
from app.filters.category_name import CategoryNameFilter
from app.states.finance import WriteEarnings
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(lambda m: m.text == 'Доход', StateFilter(None))
async def choose_category(message: Message, state: FSMContext) -> None:
    categories = await get_categories_name(message.chat.id, GROUP_EARNINGS)
    await state.update_data(categorys=categories)
    kb = [
        [types.KeyboardButton(text=f'{categories[j]}') for j in range(i, i + 2) if j < len(categories)]
        for i in range(0, len(categories), 2)
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('откуда', reply_markup=keyboard)
    await state.set_state(WriteEarnings.choosing_category)


@router.message(WriteEarnings.choosing_category, CategoryNameFilter(GROUP_EARNINGS))
async def ask_sum(message: Message, state: FSMContext) -> None:
    await state.update_data(category=message.text)
    await message.answer('Сколько?', reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(WriteEarnings.writing_value)


@router.message(lambda x: is_number_input(x.text), WriteEarnings.writing_value)
async def write_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category = data.get('category')
    amount, currency, comment = parse_amount_with_defaults(message.text)
    await insert_revenue(message.chat.id, category, amount, currency, comment)
    await message.answer('OK (валюта по умолчанию RUB)', reply_markup=create_default_keyboard())
    await state.clear()
