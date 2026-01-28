import asyncio
import logging
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from app.utils.keyboards import create_default_keyboard


async def reset_state_after_timeout(state: FSMContext, timeout: int, message: Message) -> None:
    await asyncio.sleep(timeout)
    current_state = await state.get_state()
    if current_state in ('GetingLasTransaction:transaction_history', 'GetingLasTransaction:delete_category'):
        await state.clear()
        await message.answer(
            "Состояние сброшено автоматически из-за отсутствия активности.",
            reply_markup=create_default_keyboard(),
        )
        logging.info("Состояние сброшено автоматически после таймаута.")
