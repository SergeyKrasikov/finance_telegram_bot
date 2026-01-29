from app.db.connection import db_function


async def get_last_transaction(user_id: int, num: int):
    return await db_function('get_last_transaction', user_id, num)


async def delete_transactions(transaction_ids: list[int]) -> None:
    await db_function('delete_transaction', transaction_ids)


async def insert_revenue(user_id: int, category: str, amount: float, currency: str, comment: str | None = None) -> None:
    await db_function('insert_revenue', user_id, category, amount, currency, comment)


async def insert_spend(user_id: int, category: str, amount: float, currency: str, comment: str | None = None) -> None:
    await db_function('insert_spend', user_id, category, amount, currency, comment)


async def insert_spend_with_exchange(user_id: int, category: str, amount: float, currency: str, comment: str | None = None) -> None:
    await db_function('insert_spend_with_exchange', user_id, category, amount, currency, comment)


async def get_daily_transactions(user_id: int) -> list[str]:
    records = await db_function('get_daily_transactions', user_id)
    return [record[0] for record in records]


async def monthly_summary():
    return await db_function('monthly')
