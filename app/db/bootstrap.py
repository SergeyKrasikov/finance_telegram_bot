import asyncio
import logging
from pathlib import Path

from app.db.connection import create_connection


async def apply_db_schema_with_retry(
    max_attempts: int = 20, retry_delay_sec: float = 2.0
) -> None:
    base_dir = Path(__file__).resolve().parents[2]
    files = (base_dir / "tables.sql", base_dir / "sql_functions.sql")

    for attempt in range(1, max_attempts + 1):
        connection = None
        try:
            connection = await create_connection()
            for sql_file in files:
                await connection.execute(sql_file.read_text(encoding="utf-8"))
            return
        except Exception:
            if attempt == max_attempts:
                raise
            logging.warning(
                "DB bootstrap attempt %s/%s failed, retry in %ss",
                attempt,
                max_attempts,
                retry_delay_sec,
                exc_info=True,
            )
            await asyncio.sleep(retry_delay_sec)
        finally:
            if connection:
                await connection.close()
