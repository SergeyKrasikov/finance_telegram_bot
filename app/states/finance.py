from aiogram.fsm.state import StatesGroup, State


class WriteEarnings(StatesGroup):
    choosing_category = State()
    writing_value = State()


class WriteSold(StatesGroup):
    choosing_category = State()
    writing_value = State()


class GetingLasTransaction(StatesGroup):
    transaction_history = State()
    delete_category = State()


class GettingBalance(StatesGroup):
    getting = State()


class ManualAdjustment(StatesGroup):
    choosing_type = State()
    choosing_category = State()
    writing_value = State()


class ExchangeCurrency(StatesGroup):
    choosing_category = State()
    value_out = State()
    value_in = State()
