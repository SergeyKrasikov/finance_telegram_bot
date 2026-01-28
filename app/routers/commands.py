from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(Command('start'))
async def cmd_start(message: Message) -> None:
    await message.answer('Hello!!!')
    await message.delete()


@router.message(Command('home'))
async def cmd_home(message: Message, state: FSMContext) -> None:
    await message.answer('OK', reply_markup=create_default_keyboard())
    await state.clear()
    await message.delete()
