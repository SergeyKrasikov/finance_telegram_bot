import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.db.transactions import get_daily_transactions, monthly_summary
from app.db.users import get_all_users_id
from app.config import (
    DAILY_REPORT_HOUR,
    DAILY_REPORT_MINUTE,
    MONTHLY_REPORT_CRON,
)
from app.services.monthly_logic import aggregate_monthly_rows, build_monthly_message
from app.utils.formatting import format_amount


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
        response = aggregate_monthly_rows(result)

        for user_id, values_dict in response.items():
            message = build_monthly_message(values_dict, format_amount)
            await bot.send_message(user_id, message)
    except Exception:
        logging.error("Error while monthly task", exc_info=True)


def setup_scheduler(bot) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(lambda: monthly_task(bot), 'cron', **MONTHLY_REPORT_CRON)
    scheduler.add_job(lambda: daily_task(bot), 'cron', hour=DAILY_REPORT_HOUR, minute=DAILY_REPORT_MINUTE)
    return scheduler
