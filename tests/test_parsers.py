import unittest
import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "app" / "parsers" / "input.py"
spec = importlib.util.spec_from_file_location("parsers_input", MODULE_PATH)
parsers_input = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(parsers_input)

is_amount_input = parsers_input.is_amount_input
is_number_input = parsers_input.is_number_input
parse_amount_currency = parsers_input.parse_amount_currency
parse_amount_with_defaults = parsers_input.parse_amount_with_defaults


class ParserTests(unittest.TestCase):
    def test_is_number_input_accepts_dot_and_comma(self) -> None:
        self.assertTrue(is_number_input("10.5"))
        self.assertTrue(is_number_input("10,5"))

    def test_is_number_input_rejects_mixed_separators(self) -> None:
        self.assertFalse(is_number_input("1,2.3"))
        self.assertFalse(is_number_input("abc"))

    def test_is_amount_input_accepts_amount_with_optional_currency_comment(self) -> None:
        self.assertTrue(is_amount_input("1000"))
        self.assertTrue(is_amount_input("1000,25 USD"))
        self.assertTrue(is_amount_input("1000.25 usd coffee"))

    def test_parse_amount_with_defaults(self) -> None:
        amount, currency, comment = parse_amount_with_defaults("1000,5")
        self.assertEqual(amount, 1000.5)
        self.assertEqual(currency, "RUB")
        self.assertIsNone(comment)

    def test_parse_amount_currency(self) -> None:
        amount, currency = parse_amount_currency("80,5 usdt")
        self.assertEqual(amount, 80.5)
        self.assertEqual(currency, "USDT")


if __name__ == "__main__":
    unittest.main()
