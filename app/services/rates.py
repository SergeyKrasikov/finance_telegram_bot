import datetime
import logging
from psycopg2 import Error

import download_rates

from app.db.currency import get_currency_list


async def load_rate() -> None:
    try:
        today = datetime.datetime.now()
        currency = await get_currency_list()
        result = download_rates.extract(today.strftime("%Y-%m-%d"), currency)
        result += download_rates.extract_cripto(currency)
        download_rates.load(result)
    except (Exception, Error):
        logging.error("Error while loading rates", exc_info=True)
