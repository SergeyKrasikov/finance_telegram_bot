from app.db.connection import db_function


async def get_all_users_id() -> list[int]:
    records = await db_function("get_all_users_id")
    return [record[0] for record in records]
