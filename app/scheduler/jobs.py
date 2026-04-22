from __future__ import annotations

import logging
import json
from collections.abc import Iterable, Mapping
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.db.transactions import get_daily_transactions, monthly_summary
from app.db.users import get_all_users_id
from app.config import (
    DAILY_REPORT_HOUR,
    DAILY_REPORT_MINUTE,
    MONTHLY_REPORT_CRON,
)
from app.services.monthly_logic import aggregate_monthly_rows, build_monthly_message
from app.utils.formatting import format_amount


async def daily_task(bot) -> None:
    try:
        users = await get_all_users_id()
        for user in users:
            transactions = await get_daily_transactions(user)
            message = (
                "Транзакции за сегодня:\n" + "\n".join(transactions)
                if transactions
                else "Сегодня транзакций не было, или возможно стоит их внести"
            )
            await bot.send_message(user, message)
    except Exception:
        logging.error("Error while daily task", exc_info=True)


async def monthly_task(bot) -> None:
    try:
        result = await monthly_summary()
        normalized_rows = _normalize_monthly_rows(result)
        response = aggregate_monthly_rows(normalized_rows)

        for user_id, values_dict in response.items():
            message = build_monthly_message(values_dict, format_amount)
            try:
                await bot.send_message(user_id, message)
            except Exception:
                logging.error(
                    "Error while sending monthly report to user %s",
                    user_id,
                    exc_info=True,
                )
    except Exception:
        logging.error("Error while monthly task", exc_info=True)


def _normalize_monthly_rows(
    rows: Iterable[Mapping[str, object] | object],
) -> list[Mapping[str, object]]:
    """Handle SQL result shape from monthly(): [{get_remains: jsonb}] -> [{...}]."""
    normalized: list[Mapping[str, object]] = []
    for row in rows:
        payload: object = row
        row_mapping = _to_mapping(row)
        if row_mapping is not None:
            payload = row_mapping.get("get_remains", row_mapping)

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                continue

        payload_mapping = _to_mapping(payload)
        if payload_mapping is not None:
            normalized.append(payload_mapping)

    return normalized


def _to_mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value

    items = getattr(value, "items", None)
    if callable(items):
        try:
            return dict(items())
        except Exception:
            return None

    keys = getattr(value, "keys", None)
    if callable(keys):
        try:
            return {key: value[key] for key in keys()}
        except Exception:
            return None

    return None


def setup_scheduler(bot) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        monthly_task,
        "cron",
        id="monthly_report",
        replace_existing=True,
        kwargs={"bot": bot},
        misfire_grace_time=86400,
        coalesce=True,
        **MONTHLY_REPORT_CRON,
    )
    scheduler.add_job(
        daily_task,
        "cron",
        id="daily_report",
        replace_existing=True,
        kwargs={"bot": bot},
        hour=DAILY_REPORT_HOUR,
        minute=DAILY_REPORT_MINUTE,
    )
    logging.info(
        "Scheduler configured: daily at %02d:%02d, monthly cron=%s",
        DAILY_REPORT_HOUR,
        DAILY_REPORT_MINUTE,
        MONTHLY_REPORT_CRON,
    )
    return scheduler
