from decimal import Decimal

from app.db.connection import db_function


async def get_group_balance(user_id: int, group_id: int) -> Decimal:
    records = await db_function("get_group_balance", user_id, group_id)
    return records[0][0]


async def get_all_balances(user_id: int, group_id: int) -> list[tuple[str, Decimal]]:
    records = await db_function("get_all_balances", user_id, group_id)
    return [(record["category_name"], record["balance"]) for record in records]


async def get_remains(user_id: int, category_name: str) -> Decimal:
    records = await db_function("get_remains", user_id, category_name)
    return records[0][0]


async def get_category_balance_with_currency(
    user_id: int, category_id: int
) -> list[tuple[str, Decimal]]:
    records = await db_function(
        "get_category_balance_with_currency", user_id, category_id
    )
    return [(record[0], record[1]) for record in records]
