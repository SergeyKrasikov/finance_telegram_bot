from typing import Iterable, Mapping
from decimal import Decimal

ALL_MONTHLY_FIELDS = (
    "семейный_взнос",
    "общие_категории",
    "investition",
    "month_earnings",
    "month_spend",
)


def _to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def aggregate_monthly_rows(
    rows: Iterable[Mapping[str, object]],
) -> dict[int, dict[str, Decimal]]:
    response: dict[int, dict[str, Decimal]] = {}

    for row in rows:
        for user_key, fields in (
            (
                "user_id",
                (
                    "семейный_взнос",
                    "общие_категории",
                    "investition",
                    "month_earnings",
                    "month_spend",
                ),
            ),
            ("second_user_id", ("общие_категории", "investition")),
        ):
            user_id_raw = row.get(user_key)
            if user_id_raw is None:
                continue

            user_id = int(user_id_raw)
            if user_id not in response:
                response[user_id] = {
                    field: Decimal("0") for field in ALL_MONTHLY_FIELDS
                }

            for field in fields:
                value = row.get(field, 0)
                response[user_id][field] += _to_decimal(value)

    return response


def build_monthly_message(values: Mapping[str, Decimal], format_amount_fn) -> str:
    return (
        f"Всего пришло за месяц {format_amount_fn(values.get('month_earnings', 0))}₽\n"
        f"Всего потрачено за месяц {format_amount_fn(values.get('month_spend', 0))}₽\n"
        "Переведи!\n"
        f"На семейный взнос {format_amount_fn(values.get('семейный_взнос', 0))}₽\n"
        f"На общие категории {format_amount_fn(values.get('общие_категории', 0))}₽\n"
        f"На инвестиции {format_amount_fn(values.get('investition', 0))}₽"
    )
