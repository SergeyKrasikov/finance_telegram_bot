from aiogram import types


def create_default_keyboard(
    buttons: list[str] | None = None,
    placeholder: str = 'сумма валюта комментарий',
) -> types.ReplyKeyboardMarkup:
    if buttons is None:
        buttons = ['Остаток', 'Доход']
    kb = [[types.KeyboardButton(text=button) for button in buttons]]
    return types.ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder=placeholder,
    )
