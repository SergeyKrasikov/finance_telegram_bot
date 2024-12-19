import datetime
import asyncio
import os
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram import Router, F
from aiogram.filters import Command, StateFilter, BaseFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncpg
from typing import Tuple

import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,  # Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат логов
    filename='app.log',  # Файл, в который будут записываться логи
    filemode='a'  # Режим записи в файл: 'a' для добавления, 'w' для перезаписи
)

# Пример использования
logging.info("Приложение запущено.")
logging.debug("Это сообщение для отладки.")
logging.warning("Это предупреждение.")
logging.error("Произошла ошибка.")
logging.critical("Критическая ошибка.")

import download_rates 

load_dotenv()
TOKEN = os.environ.get('TOKEN')
PG_USER = os.environ.get('POSTGRES_USER')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')

bot = Bot(TOKEN)
dp = Dispatcher()



async def create_connection() -> asyncpg.Connection:
    connection = await asyncpg.connect(user=PG_USER,
                                       password=PG_PASSWORD,
                                       host=PG_HOST,
                                       port=PG_PORT,
                                       database=PG_DATABASE)
    return connection


async def db_function(func: str, *args) -> list:
    """
    Calls a stored procedure in the database and returns the result.

    Args:
        func (str): The name of the stored procedure to call.
        *args: The arguments to pass to the stored procedure.

    Returns:
        list: The result of the stored procedure call.
    """
    logging.info(f"Calling stored procedure {func} with arguments {args}")
    connection = await create_connection()
    async with connection.transaction():
        placeholders = ', '.join(f'${i+1}' for i in range(len(args)))
        query = f"SELECT * FROM {func}({placeholders})"
        result = await connection.fetch(query, *args)
        logging.info(f"Stored procedure {func} returned {result}")
        if func == 'get_category_balance_with_currency':
            return result
        return [i[0] for i in result]


async def load_rate() -> None:
    today = datetime.datetime.now()
    currency = await db_function('get_currency')
    date = (today)
    result = download_rates.extract(date.strftime('%Y-%m-%d'), currency)
    result += download_rates.extract_cripto(currency)
    download_rates.load(result)


async def daily_task() -> None:
    users = await db_function('get_all_users_id')
    for user in users:
        transactions = await db_function('get_daily_transactions', user)
        if transactions:
            await bot.send_message(user, 'Транзакции за сегодня:\n'+'\n'.join([i.replace('00000000', '') for i in transactions]))
        else:
            await bot.send_message(user, 'Сегодня транзакций не было, или возможно стоит их внести')    


async def monthly_task() -> None:
    result = await db_function('monthly')
    response = {}
    for i in result:
        response[i['user_id']] = response.get(i['user_id'],{})
        response[i['second_user_id']] = response.get(i['second_user_id'],{})
        response[i['user_id']]['семейный_взнос'] = response[i['user_id']].get('семейный_взнос', 0) + i.get('семейный_взнос', 0)
        response[i['user_id']]['общие_категории'] = response[i['user_id']].get('общие_категории', 0) + i.get('общие_категории', 0)
        response[i['user_id']]['investition'] = response[i['user_id']].get('investition', 0) + i.get('investition', 0)
        response[i['user_id']]['month_earnings'] = response[i['user_id']].get('month_earnings', 0) + i.get('month_earnings', 0)
        response[i['user_id']]['month_spend'] = response[i['user_id']].get('month_spend', 0) + i.get('month_spend', 0)
        response[i['second_user_id']]['общие_категории'] = response[i['second_user_id']].get('общие_категории', 0) + i.get('second_user_pay', 0)
        response[i['second_user_id']]['investition'] = response[i['second_user_id']].get('investition', 0) + i.get('investition_second', 0)


    for user_id, values_dict in response.items():
        await bot.send_message(user_id, f"""Всего пришло за месяц {values_dict['month_earnings']:,.2f}₽\nВсего потрачено за месяц {values_dict['month_spend']:,.2f}₽\nПереведи! \nна семейный взнос {values_dict['семейный_взнос']:,.2f}₽ \nна общие категории {values_dict['общие_категории']:,.2f}₽ \nна инвестиции {values_dict['investition']:,.2f}₽""" )

async def get_last_transaction(user_id: str, num: int) -> Tuple[list, int]:
    result = await db_function('get_last_transaction', user_id, num)
    l = []
    transaactions_id = []
    for i in result:
        if i[2] and i[3]:
            l.append(f"{i[1].strftime('%Y-%m-%d %H:%M:%S')} \nc {i[2]} на {i[3]} {i[4]} {i[5]} \n\n".replace('"', ''))
        elif i[2]:  
            l.append(f"{i[1].strftime('%Y-%m-%d %H:%M:%S')} \nрасход {i[2]}  {i[4]} {i[5]} \n\n".replace('"', ''))  
        elif i[3]:  
            l.append(f"{i[1].strftime('%Y-%m-%d %H:%M:%S')} \nдоход {i[3]}  {i[4]} {i[5]} \n\n".replace('"', ''))      
        transaactions_id.append(i[0])
    return l, transaactions_id


class WriteEarnings (StatesGroup):
    choosing_category = State()
    writing_value = State()

class WriteSold (StatesGroup):
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


class CategoryNameFilter(BaseFilter):
    def __init__(self, group):
        self.group = group
        

    async def __call__(self, message: Message) -> bool:
        categories = await db_function('get_categories_name',
            message.from_user.id, self.group
        )
        if message.text in categories:
            return True
        return False     


@dp.message(Command('start'))
async def cmd_start(message: Message) -> None:
    await message.answer('Hello!!!')
    await message.delete()


@dp.message(Command('exchange'))    
async def cmd_exchange(message: Message, state: FSMContext) -> None:
    categorys = await db_function('get_categories_name', message.chat.id, 14)
    kb = [[types.KeyboardButton(text=f'{categorys[j]}') for j in range(i, i+2) if j < len(categorys)]for i in range(0,len(categorys), 2)]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('Какая категория', reply_markup=keyboard)
    await state.set_state(ExchangeCurrency.choosing_category)
    await message.delete()


@dp.message(Command('adjustment'))
async def cmd_adjustment(message: Message, state: FSMContext) -> None:
    kb = [[types.KeyboardButton(text='+'), types.KeyboardButton(text='-')]]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('?', reply_markup=keyboard)
    await state.set_state(ManualAdjustment.choosing_type)  
    await message.delete()


@dp.message(Command('balance'))
async def cmd_home(message: Message, state: FSMContext) -> None:
    kb = [[types.KeyboardButton(text='Личные'), types.KeyboardButton(text='Все')], [types.KeyboardButton(text='Общие'), types.KeyboardButton(text='По категориям') , types.KeyboardButton(text='По категориям c валютами')],]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('Какой баланс?', reply_markup=keyboard)    
    await state.set_state(GettingBalance.getting)
    await message.delete()


@dp.message(Command('home'))
async def cmd_home(message: Message, state: FSMContext) -> None:
    kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')]]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
    await message.answer('OK', reply_markup=keyboard)    
    await state.clear()
    await message.delete()


@dp.message(Command('history'))
async def cmd_history(message: Message, state: FSMContext) -> None:
    transaction, transactions_id = await get_last_transaction(message.chat.id, 1)
    await state.update_data(transaction_number=1)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [[types.KeyboardButton(text='Предыдущая'), types.KeyboardButton(text='Следующая')]]
    if datetime.datetime.now() <= datetime.datetime.strptime(' '.join(transaction[0].split(' ')[:2]), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1):
        kb.append([types.KeyboardButton(text='Удалить')])
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True )
    await message.answer(''.join(transaction), reply_markup=keyboard)
    await message.delete()


@dp.message(ExchangeCurrency.choosing_category, CategoryNameFilter(14))         
async def ask_value_out(message: Message, state: FSMContext) -> None:
    category_id = await db_function('get_category_id_from_name', message.text )
    await state.update_data(category=category_id)
    await message.answer('Сколько отдал(а)?', reply_markup = types.ReplyKeyboardRemove(), ) 
    await state.set_state(ExchangeCurrency.value_out)

@dp.message(ExchangeCurrency.value_out)        
async def ask_value_in(message: Message, state: FSMContext) -> None:
    await state.update_data(value_out=message.text)
    await message.answer('Сколько получил(а)?', reply_markup = types.ReplyKeyboardRemove(), ) 
    await state.set_state(ExchangeCurrency.value_in)


@dp.message(ExchangeCurrency.value_in)   
async def exchange_currenc_write(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category_id = data.get('category')[0]
    value_out, currency_out = data.get('value_out').split()
    value_in, currency_in = message.text.split()
    await db_function('exchange', message.chat.id, category_id, float(value_out), currency_out.upper(), float(value_in), currency_in.upper())
    kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
    await message.answer('OK', reply_markup=keyboard)   
    await state.clear()    

@dp.message(GetingLasTransaction.transaction_history, F.text == 'Предыдущая')
async def cmd_history(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    transaction_number = data.get('transaction_number') + 1
    transaction, transactions_id = await get_last_transaction(message.chat.id, transaction_number)
    await state.update_data(transaction_number=transaction_number)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [[types.KeyboardButton(text='Предыдущая'), types.KeyboardButton(text='Следующая')]]
    if datetime.datetime.now() <= datetime.datetime.strptime(' '.join(transaction[0].split(' ')[:2]), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1):
        kb.append([types.KeyboardButton(text='Удалить')])
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True )
    await message.answer(''.join(transaction), reply_markup=keyboard)


@dp.message(GetingLasTransaction.transaction_history, F.text == 'Следующая')
async def cmd_history(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    transaction_number = max(data.get('transaction_number') - 1, 1)
    transaction, transactions_id = await get_last_transaction(message.chat.id, transaction_number)
    await state.update_data(transaction_number=transaction_number)
    await state.update_data(transactions_id=transactions_id)
    await state.set_state(GetingLasTransaction.transaction_history)
    kb = [[types.KeyboardButton(text='Предыдущая'), types.KeyboardButton(text='Следующая')]]
    if datetime.datetime.now() <= datetime.datetime.strptime(' '.join(transaction[0].split(' ')[:2]), '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1):
        kb.append([types.KeyboardButton(text='Удалить')])
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True )
    await message.answer(''.join(transaction), reply_markup=keyboard)


@dp.message(GetingLasTransaction.transaction_history, F.text == 'Удалить')
async def questionn_delete_transaction(message: Message, state: FSMContext) -> None:
    kb = [[types.KeyboardButton(text='Да'), types.KeyboardButton(text='Нет')],]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await state.set_state(GetingLasTransaction.delete_category)
    await message.answer('Удалить транзакцию?', reply_markup=keyboard)


@dp.message(GetingLasTransaction.delete_category, F.text.in_(['Да', 'Нет']))
async def delete_transaction(message: Message, state: FSMContext) -> None:
    if  message.text == 'Да':
        data = await state.get_data()
        transactions_id = data.get('transactions_id')
        await db_function('delete_transaction', list(map(int,transactions_id)))
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer('Транзакция удалена', reply_markup=keyboard)   
        await state.clear()
    else:
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer('Транзакция не удалена', reply_markup=keyboard)   
        await state.clear()

        
@dp.message(ManualAdjustment.choosing_type, F.text.in_(['-', '+']))
async def choosing_category(message: Message, state: FSMContext) -> None:
    await state.update_data(transaction_type=message.text)
    categorys = await db_function('get_categories_name', message.chat.id, 14)
    kb = [[types.KeyboardButton(text=f'{categorys[j]}') for j in range(i, i+2) if j < len(categorys)]for i in range(0,len(categorys), 2)]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer('Какая категория', reply_markup=keyboard)
    await state.set_state(ManualAdjustment.choosing_category)


@dp.message(ManualAdjustment.choosing_category, CategoryNameFilter(14))         
async def ask_sum(message: Message, state: FSMContext) -> None:
        await state.update_data(category=message.text)
        await message.answer('Сколько?', reply_markup = types.ReplyKeyboardRemove()) 
        await state.set_state(ManualAdjustment.writing_value)

@dp.message(lambda x: x.text.split()[0].replace('.', '').replace(',', '').isdigit(), ManualAdjustment.writing_value)          
async def write_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category = data.get('category')
    transaction_type = data.get('transaction_type')
    value = message.text.split(' ', 2)
    value[0] = float(value[0].replace(',', '.'))
    value += ['RUB', '#ручная корректировка']
    value = value[:3]
    if transaction_type == '+':
        await db_function('insert_revenue', message.chat.id, category, *value)
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer('OK', reply_markup=keyboard) 
        await state.clear()
    else:
        await db_function('insert_spend', message.chat.id, category, *value)
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer(f'OK', reply_markup=keyboard) 
        await state.clear()     
    


@dp.message(GettingBalance.getting, F.text.in_(['Личные', 'Общие', 'Все', 'По категориям', 'По категориям c валютами']))
async def getting_balance(message: Message, state: FSMContext) -> None:
    if  message.text == 'Личные':
        balance = await db_function('get_group_balance', message.chat.id, 15)
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer(f'Остаток: {float(balance[0]):,.2f}₽', reply_markup=keyboard)   
        await state.clear()
    elif  message.text == 'Общие':
        balance = await db_function('get_group_balance', message.chat.id, 4)
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer(f'Остаток: {float(balance[0]):,.2f}₽', reply_markup=keyboard)   
        await state.clear() 
    elif  message.text == 'По категориям':
        balaces = []
        for category in await db_function('get_categories_name', message.chat.id, 14):
            balance = await db_function('get_remains', message.chat.id, category)
            kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
            keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
            balaces.append(f'{category:<10}: {float(balance[0]):,.2f}₽\n')
        await message.answer('Остаток: \n'+'\n'.join(balaces), reply_markup=keyboard)   
        await state.clear()  
    elif  message.text == 'По категориям c валютами':
        balaces = []
        for category in await db_function('get_categories_name', message.chat.id, 14):
            category_id = await db_function('get_category_id_from_name', category)
            balance = await db_function('get_category_balance_with_currency', message.chat.id, category_id[0])
            balance = [('\n' + str(i[0]) + ' ' + str(i[1])) for i in balance]
            kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
            keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
            balaces.append(f'{category:<10}: {" ".join(balance)}\n')
        await message.answer('Остаток: \n'+'\n'.join(balaces), reply_markup=keyboard)   
        await state.clear()  
    else:
        balance = await db_function('get_group_balance', message.chat.id, 14)
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        await message.answer(f'Остаток: {float(balance[0]):,.2f}₽', reply_markup=keyboard)   
        await state.clear()    


@dp.message(lambda x: x.text.split()[0].replace('.', '').replace(',', '').isdigit(), StateFilter(None))
async def choose_spend_category(message: Message, state: FSMContext) -> None:
    await state.update_data(value=message.text)
    categorys = await db_function('get_categories_name', message.chat.id, 8)
    kb = [[types.KeyboardButton(text=f'{categorys[j]}') for j in range(i, i+2) if j < len(categorys)]for i in range(0,len(categorys), 2)]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='Выбери категорию', )
    await message.answer('Какая категория', reply_markup=keyboard)
    await state.set_state(WriteSold.choosing_category)


@dp.message(WriteSold.choosing_category, CategoryNameFilter(8))
async def write_spend(message: Message, state: FSMContext) -> None:
    category = message.text
    data = await state.get_data()
    value = data.get('value').split(' ', 2)
    value[0] = float(value[0].replace(',', '.'))
    if len(value) > 1 and value[1].upper() != 'RUB':
        value[1] = value[1].upper()
        await db_function('insert_spend_with_exchange', message.chat.id, category, *value)
    else:    
        await db_function('insert_spend', message.chat.id, category, *value)
    balance = await db_function('get_remains', message.chat.id, category)
    kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
    await message.answer(f'остаток в {category}: {float(balance[0]):,.2f}₽', reply_markup=keyboard) 
    await state.clear()


@dp.message(F.text == 'Остаток', StateFilter(None))
async def get_ballances(message: Message) -> None:
    balaces = []
    for category in await db_function('get_categories_name', message.chat.id, 8):
        balance = await db_function('get_remains', message.chat.id, category)
        kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
        balaces.append(f'{category:<10}: {float(balance[0]):,.2f}₽\n')
    await message.answer('Остаток: \n'+'\n'.join(balaces), reply_markup=keyboard) 

@dp.message(F.text == 'Доход')        
async def choose_category(message: Message, state: FSMContext) -> None:
        categorys = await db_function('get_categories_name', message.chat.id, 10)
        await state.update_data(categorys=await db_function('get_categories_name', message.chat.id, 10))
        kb = [[types.KeyboardButton(text=f'{categorys[j]}') for j in range(i, i+2) if j < len(categorys)]for i in range(0,len(categorys), 2)]
        keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
        await message.answer('откуда', reply_markup=keyboard) 
        await state.set_state(WriteEarnings.choosing_category)

@dp.message(WriteEarnings.choosing_category, CategoryNameFilter(10))         
async def ask_sum(message: Message, state: FSMContext) -> None:
        await state.update_data(category=message.text)
        await message.answer('Сколько?', reply_markup = types.ReplyKeyboardRemove()) 
        await state.set_state(WriteEarnings.writing_value)

@dp.message(lambda x: x.text.split()[0].replace('.', '').replace(',', '').isdigit(), WriteEarnings.writing_value)          
async def write_value(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    category = data.get('category')
    value = message.text.split(' ', 2)
    value[0] = float(value[0].replace(',', '.'))
    await db_function('insert_revenue', message.chat.id, category, *value)
    kb = [[types.KeyboardButton(text='Остаток'), types.KeyboardButton(text='Доход')],]
    keyboard = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True, input_field_placeholder='сумма валюта комментарий')
    await message.answer('OK', reply_markup=keyboard) 
    await state.clear()


scheduler = AsyncIOScheduler()

scheduler.add_job(monthly_task, 'cron', month='*')
scheduler.add_job(daily_task, 'cron', hour='23', minute='59')
scheduler.add_job(load_rate, 'interval', hours=14)

        
async def on_startup() -> None: 
    print('START')
    scheduler.start()


async def main() -> None:
    # способ для пропуска старых апдейтов для 3 версии айограма
    await bot.delete_webhook(drop_pending_updates=True) 
    # собственно способ зарегистрировать функцию которая сработает при запуске бота
    dp.startup.register(on_startup)
    # в allowed_updates можно передать вызов метода resolve_used_update_types() от диспетчера, 
    # который пройдёт по всем роутерам, узнает, хэндлеры на какие типы есть в коде, 
    # и попросить Telegram присылать апдейты только про них
    # короче просто делайте так же
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == '__main__':
    asyncio.run(main())

