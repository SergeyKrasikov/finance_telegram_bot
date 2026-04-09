import asyncio
from aiogram import Router, F, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.services.state import reset_state_after_timeout
from app.services.transactions import get_last_transaction
from app.states.finance import GetingLasTransaction
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(Command("history"))
async def cmd_history(message: Message, state: FSMContext) -> None:
    transaction, transactions_id = await get_last_transaction(message.chat.id, 1)
    if not transaction:
        await message.answer(
            "Транзакций пока нет.", reply_markup=create_default_keyboard()
        )
        return
    await state.update_data(transaction_number=1)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [
        [
            types.KeyboardButton(text="Предыдущая"),
            types.KeyboardButton(text="Следующая"),
        ]
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    asyncio.create_task(reset_state_after_timeout(state, 300, message))
    await message.answer("".join(transaction), reply_markup=keyboard)
    await message.delete()


@router.message(GetingLasTransaction.transaction_history, F.text == "Предыдущая")
async def history_prev(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    transaction_number = data.get("transaction_number") + 1
    transaction, transactions_id = await get_last_transaction(
        message.chat.id, transaction_number
    )
    if not transaction:
        await message.answer(
            "Больше транзакций нет.", reply_markup=create_default_keyboard()
        )
        return
    await state.update_data(transaction_number=transaction_number)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [
        [
            types.KeyboardButton(text="Предыдущая"),
            types.KeyboardButton(text="Следующая"),
        ]
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("".join(transaction), reply_markup=keyboard)


@router.message(GetingLasTransaction.transaction_history, F.text == "Следующая")
async def history_next(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    transaction_number = max(data.get("transaction_number") - 1, 1)
    transaction, transactions_id = await get_last_transaction(
        message.chat.id, transaction_number
    )
    if not transaction:
        await message.answer(
            "Больше транзакций нет.", reply_markup=create_default_keyboard()
        )
        return
    await state.update_data(transaction_number=transaction_number)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [
        [
            types.KeyboardButton(text="Предыдущая"),
            types.KeyboardButton(text="Следующая"),
        ]
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer("".join(transaction), reply_markup=keyboard)


@router.message(GetingLasTransaction.transaction_history, F.text == "Удалить")
async def question_delete_transaction(message: Message, state: FSMContext) -> None:
    await message.answer(
        "Удаление транзакций временно отключено на ledger-миграции.",
        reply_markup=create_default_keyboard(),
    )
    await state.clear()


@router.message(GetingLasTransaction.delete_category, F.text.in_(["Да", "Нет"]))
async def delete_transaction(message: Message, state: FSMContext) -> None:
    await message.answer(
        "Удаление транзакций временно отключено на ledger-миграции.",
        reply_markup=create_default_keyboard(),
    )
    await state.clear()
