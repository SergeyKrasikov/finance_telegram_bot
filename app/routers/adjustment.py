from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.config import GROUP_ALL
from app.db.categories import get_categories_name
from app.db.transactions import insert_revenue, insert_spend
from app.parsers.input import is_number_input, parse_amount_parts
from app.filters.category_name import CategoryNameFilter
from app.states.finance import ManualAdjustment
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(Command('adjustment'))
async def cmd_adjustment(message: Message, state: FSMContext) -> None:
    kb = [[types.KeyboardButton(text='+'), types.KeyboardButton(text='-')]]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('?', reply_markup=keyboard)
    await state.set_state(ManualAdjustment.choosing_type)
    await message.delete()


@router.message(ManualAdjustment.choosing_type, F.text.in_(['-', '+']))
async def choosing_category(message: Message, state: FSMContext) -> None:
    await state.update_data(transaction_type=message.text)
    categories = await get_categories_name(message.chat.id, GROUP_ALL)
    kb = [
        [types.KeyboardButton(text=f'{categories[j]}') for j in range(i, i + 2) if j < len(categories)]
        for i in range(0, len(categories), 2)
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('Какая категория', reply_markup=keyboard)
    await state.set_state(ManualAdjustment.choosing_category)


@router.message(ManualAdjustment.choosing_category, CategoryNameFilter(GROUP_ALL))
async def ask_sum(message: Message, state: FSMContext) -> None:
    await state.update_data(category=message.text)
    await message.answer('Сколько?', reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ManualAdjustment.writing_value)


@router.message(lambda x: is_number_input(x.text), ManualAdjustment.writing_value)
async def write_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category = data.get('category')
    transaction_type = data.get('transaction_type')
    value = parse_amount_parts(message.text)
    value += ['RUB', '#ручная корректировка']
    value = value[:3]
    if transaction_type == '+':
        await insert_revenue(message.chat.id, category, *value)
        await message.answer('OK', reply_markup=create_default_keyboard())
        await state.clear()
    else:
        await insert_spend(message.chat.id, category, *value)
        await message.answer('OK', reply_markup=create_default_keyboard())
        await state.clear()
