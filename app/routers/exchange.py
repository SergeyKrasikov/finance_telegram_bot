from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.config import GROUP_ALL
from app.db.connection import db_function
from app.filters.category_name import CategoryNameFilter
from app.states.finance import ExchangeCurrency
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(Command('exchange'))
async def cmd_exchange(message: Message, state: FSMContext) -> None:
    categories = await db_function('get_categories_name', message.chat.id, GROUP_ALL)
    kb = [
        [types.KeyboardButton(text=f'{categories[j]}') for j in range(i, i + 2) if j < len(categories)]
        for i in range(0, len(categories), 2)
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('Какая категория', reply_markup=keyboard)
    await state.set_state(ExchangeCurrency.choosing_category)
    await message.delete()


@router.message(ExchangeCurrency.choosing_category, CategoryNameFilter(GROUP_ALL))
async def ask_value_out(message: Message, state: FSMContext) -> None:
    category_id = await db_function('get_category_id_from_name', message.text)
    await state.update_data(category=category_id)
    await message.answer('Сколько отдал(а)?', reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ExchangeCurrency.value_out)


@router.message(ExchangeCurrency.value_out)
async def ask_value_in(message: Message, state: FSMContext) -> None:
    await state.update_data(value_out=message.text)
    await message.answer('Сколько получил(а)?', reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ExchangeCurrency.value_in)


@router.message(ExchangeCurrency.value_in)
async def exchange_currency_write(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category_id = data.get('category')[0]
    value_out, currency_out = data.get('value_out').split()
    value_in, currency_in = message.text.split()
    await db_function(
        'exchange',
        message.chat.id,
        category_id,
        float(value_out),
        currency_out.upper(),
        float(value_in),
        currency_in.upper(),
    )
    await message.answer('OK', reply_markup=create_default_keyboard())
    await state.clear()
