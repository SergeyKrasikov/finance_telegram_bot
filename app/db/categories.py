from app.db.connection import db_function


async def get_categories_name_v2(user_id: int, group_id: int) -> list[str]:
    records = await db_function("get_categories_name_v2", user_id, group_id)
    return [record[0] for record in records]


async def get_category_id_from_name_v2(user_id: int, name: str) -> int:
    records = await db_function("get_category_id_from_name_v2", user_id, name)
    return records[0][0]
