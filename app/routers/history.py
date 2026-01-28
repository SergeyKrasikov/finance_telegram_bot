import asyncio
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.db.connection import db_function
from app.services.state import reset_state_after_timeout
from app.services.transactions import get_last_transaction, is_recent_transaction
from app.states.finance import GetingLasTransaction
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(Command('history'))
async def cmd_history(message: Message, state: FSMContext) -> None:
    transaction, transactions_id = await get_last_transaction(message.chat.id, 1)
    await state.update_data(transaction_number=1)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [[types.KeyboardButton(text='Предыдущая'), types.KeyboardButton(text='Следующая')]]
    if is_recent_transaction(transaction[0]):
        kb.append([types.KeyboardButton(text='Удалить')])
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    asyncio.create_task(reset_state_after_timeout(state, 300, message))
    await message.answer(''.join(transaction), reply_markup=keyboard)
    await message.delete()


@router.message(GetingLasTransaction.transaction_history, F.text == 'Предыдущая')
async def history_prev(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    transaction_number = data.get('transaction_number') + 1
    transaction, transactions_id = await get_last_transaction(message.chat.id, transaction_number)
    await state.update_data(transaction_number=transaction_number)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [[types.KeyboardButton(text='Предыдущая'), types.KeyboardButton(text='Следующая')]]
    if is_recent_transaction(transaction[0]):
        kb.append([types.KeyboardButton(text='Удалить')])
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(''.join(transaction), reply_markup=keyboard)


@router.message(GetingLasTransaction.transaction_history, F.text == 'Следующая')
async def history_next(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    transaction_number = max(data.get('transaction_number') - 1, 1)
    transaction, transactions_id = await get_last_transaction(message.chat.id, transaction_number)
    await state.update_data(transaction_number=transaction_number)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [[types.KeyboardButton(text='Предыдущая'), types.KeyboardButton(text='Следующая')]]
    if is_recent_transaction(transaction[0]):
        kb.append([types.KeyboardButton(text='Удалить')])
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(''.join(transaction), reply_markup=keyboard)


@router.message(GetingLasTransaction.transaction_history, F.text == 'Удалить')
async def question_delete_transaction(message: Message, state: FSMContext) -> None:
    kb = [[types.KeyboardButton(text='Да'), types.KeyboardButton(text='Нет')]]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await state.set_state(GetingLasTransaction.delete_category)
    await message.answer('Удалить транзакцию?', reply_markup=keyboard)


@router.message(GetingLasTransaction.delete_category, F.text.in_(['Да', 'Нет']))
async def delete_transaction(message: Message, state: FSMContext) -> None:
    if message.text == 'Да':
        data = await state.get_data()
        transactions_id = data.get('transactions_id')
        await db_function('delete_transaction', list(map(int, transactions_id)))
        await message.answer('Транзакция удалена', reply_markup=create_default_keyboard())
        await state.clear()
    else:
        await message.answer('Транзакция не удалена', reply_markup=create_default_keyboard())
        await state.clear()
