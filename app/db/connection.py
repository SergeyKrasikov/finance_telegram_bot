import asyncpg
import logging
from psycopg2 import Error

from app.config import PG_DATABASE, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER


async def create_connection() -> asyncpg.Connection:
    try:
        return await asyncpg.connect(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
        )
    except (Exception, asyncpg.PostgresError):
        logging.error("Error while connecting to PostgreSQL", exc_info=True)
        raise


async def db_function(func: str, *args) -> list:
    connection = None
    try:
        connection = await create_connection()
        placeholders = ", ".join([f"${i+1}" for i in range(len(args))])
        query = f"SELECT * FROM {func}({placeholders})"
        response = await connection.fetch(query, *args)
        return response
    except (Exception, asyncpg.PostgresError, Error):
        logging.error("Error while calling function in PostgreSQL", exc_info=True)
        raise
    finally:
        if connection:
            await connection.close()
