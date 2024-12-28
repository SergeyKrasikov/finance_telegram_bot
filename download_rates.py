import requests
import psycopg2
import os
from dotenv import load_dotenv
import datetime
import logging

# Настройка логирования
log_directory = '/logs'  # Внутри контейнера
os.makedirs(log_directory, exist_ok=True)
log_file = os.path.join(log_directory, 'download_rates.log')

logging.basicConfig(
    level=logging.INFO,  # Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат логов
    filename=log_file,  # Файл, в который будут записываться логи
    filemode='a'  # Режим записи в файл: 'a' для добавления, 'w' для перезаписи
)


load_dotenv()
OPEN_EXCHANGE_TOKEN = os.environ.get('OPEN_EXCHANGE_TOKEN')
PG_USER = os.environ.get('POSTGRES_USER')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')
COINMARKETCAP_TOKEN = os.environ.get('COINMARKETCAP_TOKEN')


def extract_cripto(currency: list) -> list:
    """
    Извлекает данные о криптовалютах с CoinMarketCap API.

    Args:
        currency (list): Список криптовалютных символов для запроса.

    Returns:
        list: Список словарей с timestamp, currency и value.
    """
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': COINMARKETCAP_TOKEN
    }
    parameters = {
        'symbol': ','.join(currency),
        'convert': 'USD'
    }

    # Запрос к API
    try:
        response = requests.get(url, headers=headers, params=parameters)
        response.raise_for_status()
        data = response.json().get('data', {})
    except requests.RequestException as error:
        logging.error(f"Ошибка запроса к API CoinMarketCap: {error}")
        return []

    result = []
    for symbol in currency:
        try:
            crypto_data = data.get(symbol, {})
            if not crypto_data:
                logging.warning(f"Данные для {symbol} отсутствуют в ответе API.")
                continue

            # Извлекаем цену и инвертируем
            quote = crypto_data.get('quote', {}).get('USD', {})
            price = quote.get('price')
            if not isinstance(price, (float, int)) or price <= 0:
                logging.warning(f"Некорректная цена для {symbol}: {price}")
                continue
            inverted_price = 1 / price

            # Форматируем временную метку
            timestamp = crypto_data.get('last_updated')
            if timestamp and timestamp.endswith('Z'):
                timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
            else:
                logging.warning(f"Некорректное или отсутствующее время обновления для {symbol}: {timestamp}")
                continue

            # Добавляем результат
            result.append({
                'timestamp': timestamp,
                'currency': symbol,
                'value': inverted_price
            })

        except Exception as error:
            logging.error(f"Ошибка обработки данных для {symbol}: {error}")

    return result


def extract(report_date: str, currency: list) -> list:
    try:
        response_json = requests.get(f'https://openexchangerates.org/api/historical/{report_date}.json?app_id={OPEN_EXCHANGE_TOKEN}&symbols={",".join(currency)}').json()
        data = []
        for currency, value in response_json['rates'].items():
            data_dict = {}
            data_dict['timestamp'] = datetime.datetime.utcfromtimestamp(response_json['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            data_dict['currency'] = currency
            data_dict['value'] = value
            data.append(data_dict)
        return data
    except Exception as error:
        raise error


def load(result: dict) -> None:
    conn = None
    try:
        connection = psycopg2.connect(user=PG_USER,
                                    password=PG_PASSWORD,
                                    host=PG_HOST,
                                    port=PG_PORT,
                                    database=PG_DATABASE)
        cur = connection.cursor()
        print(3)
        print(result)
        print(4)
        for row in result:
            sql = f"DELETE FROM exchange_rates WHERE datetime = '{row['timestamp']}' AND currency = '{row['currency']}'"
            cur.execute(sql)
            sql = f"INSERT INTO exchange_rates VALUES ('{row['timestamp']}', '{row['currency']}', {row['value']})"
            cur.execute(sql)
        connection.commit()
        print(5)
    except Exception as error:
        print(error)
    finally:
        if conn:
            connection.close()
            cur.close()

def main():
    try:
        today = datetime.datetime.now()

        date = (today)
        result = extract(date.strftime('%Y-%m-%d'),['BTC', 'ETH', 'TON', 'RUB', 'EUR'])
        load(result)
    except Exception as error:
        print(error)

if __name__ == '__main__':
    main()
