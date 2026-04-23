import logging
from aiogram import Router, types
from aiogram.filters import StateFilter
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from app.config import GROUP_SPEND
from app.db.categories import get_categories_name
from app.db.balances import get_remains
from app.db.transactions import insert_spend, insert_spend_with_exchange
from app.parsers.input import is_amount_input, parse_amount_with_defaults
from app.filters.category_name import CategoryNameFilter
from app.states.finance import WriteSold
from app.utils.formatting import format_amount
from app.utils.keyboards import create_default_keyboard

router = Router()


@router.message(lambda x: is_amount_input(x.text), StateFilter(None))
async def choose_spend_category(message: Message, state: FSMContext) -> None:
    await state.update_data(value=message.text)
    categories = await get_categories_name(message.chat.id, GROUP_SPEND)
    kb = [
        [
            types.KeyboardButton(text=f"{categories[j]}")
            for j in range(i, i + 2)
            if j < len(categories)
        ]
        for i in range(0, len(categories), 2)
    ]
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Выбери категорию",
    )
    await message.answer("Какая категория", reply_markup=keyboard)
    await state.set_state(WriteSold.choosing_category)


@router.message(WriteSold.choosing_category, CategoryNameFilter(GROUP_SPEND))
async def write_spend(message: Message, state: FSMContext) -> None:
    try:
        category = message.text
        data = await state.get_data()
        amount, currency, comment = parse_amount_with_defaults(data.get("value"))

        if currency != "RUB":
            await insert_spend_with_exchange(
                message.chat.id, category, amount, currency, comment
            )
        else:
            await insert_spend(message.chat.id, category, amount, currency, comment)

        balance = await get_remains(message.chat.id, category)
        await message.answer(
            f"Остаток в {category}: {format_amount(balance)}₽",
            reply_markup=create_default_keyboard(),
        )
    except ValueError as e:
        logging.error(f"Ошибка преобразования суммы: {e}", exc_info=True)
        await message.answer(
            "Неверный формат суммы. Введите данные в формате: сумма валюта комментарий."
        )
    except Exception as e:
        logging.error(f"Ошибка при записи расходов: {e}", exc_info=True)
        await message.answer("Произошла ошибка при записи расходов. Попробуйте снова.")
    finally:
        await state.clear()
