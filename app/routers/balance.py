import logging
from aiogram import Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.config import GROUP_ALL, GROUP_COMMON, GROUP_PERSONAL, GROUP_SPEND
from app.db.balances import (
    get_all_balances,
    get_category_balance_with_currency,
    get_group_balance,
    get_remains,
)
from app.db.categories import get_categories_name, get_category_id_from_name
from app.states.finance import GettingBalance
from app.utils.formatting import format_amount
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(Command('balance'))
async def cmd_balance(message: Message, state: FSMContext) -> None:
    kb = [
        [types.KeyboardButton(text='Личные'), types.KeyboardButton(text='Все')],
        [
            types.KeyboardButton(text='Общие'),
            types.KeyboardButton(text='По категориям'),
            types.KeyboardButton(text='По категориям c валютами'),
        ],
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('Какой баланс?', reply_markup=keyboard)
    await state.set_state(GettingBalance.getting)
    await message.delete()


@router.message(GettingBalance.getting, F.text.in_(['Личные', 'Общие', 'Все', 'По категориям', 'По категориям c валютами']))
async def getting_balance(message: Message, state: FSMContext) -> None:
    if message.text == 'Личные':
        balance = await get_group_balance(message.chat.id, GROUP_PERSONAL)
        await message.answer(f'Остаток: {format_amount(balance)}₽', reply_markup=create_default_keyboard())
        await state.clear()
    elif message.text == 'Общие':
        balance = await get_group_balance(message.chat.id, GROUP_COMMON)
        await message.answer(f'Остаток: {format_amount(balance)}₽', reply_markup=create_default_keyboard())
        await state.clear()
    elif message.text == 'По категориям':
        balances = []
        for category in await get_categories_name(message.chat.id, GROUP_ALL):
            balance = await get_remains(message.chat.id, category)
            balances.append(f'{category:<10}: {format_amount(balance)}₽\n')
        await message.answer('Остаток: \n' + '\n'.join(balances), reply_markup=create_default_keyboard())
        await state.clear()
    elif message.text == 'По категориям c валютами':
        balances = []
        for category in await get_categories_name(message.chat.id, GROUP_ALL):
            category_id = await get_category_id_from_name(category)
            balance = await get_category_balance_with_currency(message.chat.id, category_id)
            balance = [('\n' + format_amount(i[0]) + ' ' + str(i[1])) for i in balance]
            balances.append(f'{category:<10}: {" ".join(balance)}\n')
        await message.answer('Остаток: \n' + '\n'.join(balances), reply_markup=create_default_keyboard())
        await state.clear()
    else:
        balance = await get_group_balance(message.chat.id, GROUP_ALL)
        await message.answer(f'Остаток: {format_amount(balance)}₽', reply_markup=create_default_keyboard())
        await state.clear()


@router.message(F.text == 'Остаток', StateFilter(None))
async def get_balances(message: Message) -> None:
    try:
        balances = await get_all_balances(message.chat.id, GROUP_SPEND)
        logging.info(f"Полученные данные balances: {balances}")
        balances_text = '\n\n'.join([
            f'{category:<20}: {format_amount(balance)}₽'
            for category, balance in balances
        ])
        await message.answer(f'Остаток: \n{balances_text}', reply_markup=create_default_keyboard())
    except ValueError as e:
        logging.error(f"Ошибка при обработке балансов: {e}", exc_info=True)
        await message.answer("Получены некорректные данные о балансах.")
    except Exception as e:
        logging.error(f"Ошибка при получении балансов: {e}", exc_info=True)
        await message.answer("Произошла ошибка при получении балансов.")
