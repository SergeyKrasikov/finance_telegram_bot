import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging() -> None:
    log_directory = '/logs'  # внутри контейнера
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, 'bot.log')

    handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
    logging.getLogger().addHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='a',
    )

    logging.info("Приложение запущено.")
