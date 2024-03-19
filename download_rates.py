import requests
import psycopg2
import os
from dotenv import load_dotenv
import datetime


load_dotenv()
OPEN_EXCHANGE_TOKEN = os.environ.get('OPEN_EXCHANGE_TOKEN')
PG_USER = os.environ.get('POSTGRES_USER')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
PG_HOST = os.environ.get('PG_HOST')
PG_PORT = os.environ.get('PG_PORT')
PG_DATABASE = os.environ.get('PG_DATABASE')


def extract(report_date: str, currency: list) -> dict:
    try:
        response_json = requests.get(f'https://openexchangerates.org/api/historical/{report_date}.json?app_id={OPEN_EXCHANGE_TOKEN}&symbols={",".join(currency)}').json()
        return response_json
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
        date_time = datetime.datetime.utcfromtimestamp(result['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        sql = f"DELETE FROM exchange_rates WHERE datetime = '{date_time}'"
        cur.execute(sql)
        for currency, value in result['rates'].items():
            sql = f"INSERT INTO exchange_rates VALUES ('{date_time}', '{currency}', {value})"
            cur.execute(sql)
        connection.commit()
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
        result = extract(date.strftime('%Y-%m-%d'))
        load(result)
    except Exception as error:
        print(error)

if __name__ == '__main__':
    main()
