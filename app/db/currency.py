from decimal import Decimal

from app.db.connection import db_function


async def get_currency_list() -> list[str]:
    records = await db_function("get_currency")
    return [record[0] for record in records]


async def exchange_currency(
    user_id: int,
    category_id: int,
    value_out: Decimal,
    currency_out: str,
    value_in: Decimal,
    currency_in: str,
) -> str:
    records = await db_function(
        "exchange", user_id, category_id, value_out, currency_out, value_in, currency_in
    )
    return records[0][0] if records else "OK"
