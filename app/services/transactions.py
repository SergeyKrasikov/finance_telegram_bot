import datetime
import logging
from typing import Tuple
from psycopg2 import Error

from app.db.transactions import get_last_transaction as db_get_last_transaction


async def get_last_transaction(user_id: int, num: int) -> Tuple[list[str], list[int]]:
    try:
        result = await db_get_last_transaction(user_id, num)
        if not result:
            return [], []
        items = []
        transactions_id = []
        for i in result:
            if i[2] and i[3]:
                items.append(
                    f"{i[1].strftime('%Y-%m-%d %H:%M:%S')} \nc {i[2]} на {i[3]} {i[4]} {i[5]} \n\n".replace('"', '')
                )
            elif i[2]:
                items.append(
                    f"{i[1].strftime('%Y-%m-%d %H:%M:%S')} \nрасход {i[2]}  {i[4]} {i[5]} \n\n".replace('"', '')
                )
            elif i[3]:
                items.append(
                    f"{i[1].strftime('%Y-%m-%d %H:%M:%S')} \nдоход {i[3]}  {i[4]} {i[5]} \n\n".replace('"', '')
                )
            transactions_id.append(i[0])
        return items, transactions_id
    except (Exception, Error):
        logging.error("Error while getting last transaction", exc_info=True)
        raise


def is_recent_transaction(transaction_text: str) -> bool:
    ts = ' '.join(transaction_text.split(' ')[:2])
    return datetime.datetime.now() <= datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1)
