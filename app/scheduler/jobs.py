import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.db.transactions import get_daily_transactions, monthly_summary
from app.db.users import get_all_users_id
from app.config import (
    DAILY_REPORT_HOUR,
    DAILY_REPORT_MINUTE,
    MONTHLY_REPORT_CRON,
)


async def daily_task(bot) -> None:
    try:
        users = await get_all_users_id()
        for user in users:
            transactions = await get_daily_transactions(user)
            message = (
                'Транзакции за сегодня:\n' + '\n'.join(transactions)
                if transactions
                else 'Сегодня транзакций не было, или возможно стоит их внести'
            )
            await bot.send_message(user, message)
    except Exception:
        logging.error("Error while daily task", exc_info=True)


async def monthly_task(bot) -> None:
    try:
        result = await monthly_summary()
        response: dict[int, dict[str, float]] = {}

        for i in result:
            for key, fields in [
                ('user_id', ['семейный_взнос', 'общие_категории', 'investition', 'month_earnings', 'month_spend']),
                ('second_user_id', ['общие_категории', 'investition']),
            ]:
                user_id = i[key]
                if user_id not in response:
                    response[user_id] = {field: 0 for field in fields}
                for field in fields:
                    response[user_id][field] += i.get(field, 0)

        for user_id, values_dict in response.items():
            await bot.send_message(
                user_id,
                f"""Всего пришло за месяц {values_dict['month_earnings']:,.2f}₽
                    Всего потрачено за месяц {values_dict['month_spend']:,.2f}₽
                    Переведи!
                    На семейный взнос {values_dict['семейный_взнос']:,.2f}₽
                    На общие категории {values_dict['общие_категории']:,.2f}₽
                    На инвестиции {values_dict['investition']:,.2f}₽""",
            )
    except Exception:
        logging.error("Error while monthly task", exc_info=True)


def setup_scheduler(bot) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(lambda: monthly_task(bot), 'cron', **MONTHLY_REPORT_CRON)
    scheduler.add_job(lambda: daily_task(bot), 'cron', hour=DAILY_REPORT_HOUR, minute=DAILY_REPORT_MINUTE)
    return scheduler
