from decimal import Decimal


def format_amount(value: int | Decimal | str) -> str:
    amount = Decimal(str(value))
    if abs(amount) >= Decimal("1"):
        return f"{amount:,.2f}".replace(",", " ")

    text = format(amount, "f").rstrip("0").rstrip(".")
    return text if text else "0"
