from aiogram.filters import BaseFilter
from aiogram.types import Message

from app.db.categories import get_categories_name_v2


class CategoryNameFilter(BaseFilter):
    def __init__(self, group: int):
        self.group = group

    async def __call__(self, message: Message) -> bool:
        categories = await get_categories_name_v2(message.from_user.id, self.group)
        return message.text in categories
