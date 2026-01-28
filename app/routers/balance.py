import logging
from aiogram import Router, F, types
from aiogram.filters import Command, StateFilter
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.db.connection import db_function
from app.states.finance import GettingBalance
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
        balance = await db_function('get_group_balance', message.chat.id, 15)
        await message.answer(f'Остаток: {float(balance[0]):,.2f}₽', reply_markup=create_default_keyboard())
        await state.clear()
    elif message.text == 'Общие':
        balance = await db_function('get_group_balance', message.chat.id, 4)
        await message.answer(f'Остаток: {float(balance[0]):,.2f}₽', reply_markup=create_default_keyboard())
        await state.clear()
    elif message.text == 'По категориям':
        balances = []
        for category in await db_function('get_categories_name', message.chat.id, 14):
            balance = await db_function('get_remains', message.chat.id, category)
            balances.append(f'{category:<10}: {float(balance[0]):,.2f}₽\n')
        await message.answer('Остаток: \n' + '\n'.join(balances), reply_markup=create_default_keyboard())
        await state.clear()
    elif message.text == 'По категориям c валютами':
        balances = []
        for category in await db_function('get_categories_name', message.chat.id, 14):
            category_id = await db_function('get_category_id_from_name', category)
            balance = await db_function('get_category_balance_with_currency', message.chat.id, category_id[0])
            balance = [('\n' + str(i[0]) + ' ' + str(i[1])) for i in balance]
            balances.append(f'{category:<10}: {" ".join(balance)}\n')
        await message.answer('Остаток: \n' + '\n'.join(balances), reply_markup=create_default_keyboard())
        await state.clear()
    else:
        balance = await db_function('get_group_balance', message.chat.id, 14)
        await message.answer(f'Остаток: {float(balance[0]):,.2f}₽', reply_markup=create_default_keyboard())
        await state.clear()


@router.message(F.text == 'Остаток', StateFilter(None))
async def get_balances(message: Message) -> None:
    try:
        balances = await db_function('get_all_balances', message.chat.id, 8)
        logging.info(f"Полученные данные balances: {balances}")
        balances_text = '\n\n'.join([
            f'{category:<20}: {float(balance):,.2f}₽'
            for category, balance in balances
        ])
        await message.answer(f'Остаток: \n{balances_text}', reply_markup=create_default_keyboard())
    except ValueError as e:
        logging.error(f"Ошибка при обработке балансов: {e}", exc_info=True)
        await message.answer("Получены некорректные данные о балансах.")
    except Exception as e:
        logging.error(f"Ошибка при получении балансов: {e}", exc_info=True)
        await message.answer("Произошла ошибка при получении балансов.")
