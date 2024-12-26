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
    try:
        url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
        parameters = {
            'symbol': ','.join(currency),
            'convert': 'USD'
        }
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': COINMARKETCAP_TOKEN
        }
        
        data =[]
        r = requests.get(url, headers=headers, params=parameters).json().get('data')
        print(1)
        for i in currency:
            try:
                value = 1/r.get(i,{}).get('quote',{}).get('USD',{}).get('price')
            except Exception as error:
                value = None
                logging.error(f'Error: {error} for currency: {i} value: {1/r.get(i,{}).get('quote',{}).get('USD',{}).get('price')}')    
            if value:
                data.append({'timestamp': datetime.datetime.fromisoformat(r.get(i,{}).get('last_updated')).strftime('%Y-%m-%d %H:%M:%S'), 'currency': i, 'value': value})
        return data
    except Exception as error:
        raise error


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
