import re
from typing import List, Tuple, Optional

_AMOUNT_RE = re.compile(r'^\d+([.,]\d+)?(\s+[A-Za-z]{3})?(\s+.+)?$')
_NUMBER_RE = re.compile(r'^\d+([.,]\d+)?$')


def is_amount_input(text: str) -> bool:
    return bool(_AMOUNT_RE.match(text.strip()))


def is_number_input(text: str) -> bool:
    parts = text.strip().split()
    if not parts:
        return False
    return bool(_NUMBER_RE.match(parts[0]))


def parse_amount_parts(text: str) -> List:
    parts = text.strip().split(' ', 2)
    amount = float(parts[0].replace(',', '.'))
    return [amount] + parts[1:]


def parse_amount_with_defaults(text: str, default_currency: str = 'RUB') -> Tuple[float, str, Optional[str]]:
    parts = parse_amount_parts(text)
    currency = parts[1].upper() if len(parts) > 1 else default_currency
    comment = parts[2] if len(parts) > 2 else None
    return parts[0], currency, comment


def parse_amount_currency(text: str) -> Tuple[float, str]:
    parts = text.strip().split()
    if len(parts) != 2:
        raise ValueError('Expected: "<amount> <currency>"')
    amount = float(parts[0].replace(',', '.'))
    return amount, parts[1].upper()
