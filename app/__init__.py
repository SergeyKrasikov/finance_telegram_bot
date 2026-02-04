import asyncio

from aiogram import Bot, Dispatcher

from app.config import TOKEN
from app.logging_config import setup_logging
from app.scheduler.jobs import setup_scheduler
from app.routers import (
    commands,
    exchange,
    adjustment,
    history,
    balance,
    earnings,
    spend,
)


def setup_dispatcher(dp: Dispatcher) -> None:
    dp.include_router(commands.router)
    dp.include_router(exchange.router)
    dp.include_router(adjustment.router)
    dp.include_router(history.router)
    dp.include_router(balance.router)
    dp.include_router(earnings.router)
    dp.include_router(spend.router)


async def on_startup(bot: Bot) -> None:
    print("START")
    scheduler = setup_scheduler(bot)
    scheduler.start()


async def main() -> None:
    setup_logging()
    bot = Bot(TOKEN)
    dp = Dispatcher()
    setup_dispatcher(dp)
    dp.startup.register(lambda: on_startup(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


def app_main() -> None:
    asyncio.run(main())
