from aiogram.filters import BaseFilter
from aiogram.types import Message

from app.db.connection import db_function


class CategoryNameFilter(BaseFilter):
    def __init__(self, group: int):
        self.group = group

    async def __call__(self, message: Message) -> bool:
        categories = await db_function('get_categories_name', message.from_user.id, self.group)
        return message.text in categories
