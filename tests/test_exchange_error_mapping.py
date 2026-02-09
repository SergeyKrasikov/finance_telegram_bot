import unittest
import importlib.util
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "app" / "services" / "exchange_errors.py"
)
spec = importlib.util.spec_from_file_location("exchange_errors", MODULE_PATH)
exchange_errors = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(exchange_errors)

map_exchange_error = exchange_errors.map_exchange_error


class ExchangeErrorMappingTests(unittest.TestCase):
    def test_stablecoin_unknown_message(self) -> None:
        msg = map_exchange_error(
            "Stablecoin rate is unknown. Exchange stablecoin with USD first"
        )
        self.assertEqual(msg, "Нет курса стейбла. Сначала обменяй стейбл → USD.")

    def test_rates_for_message(self) -> None:
        msg = map_exchange_error(
            "Rates for AAA and BBB are unknown. Exchange via USD first"
        )
        self.assertEqual(
            msg, "Нет курсов для выбранной пары. Сначала обменяй через USD."
        )

    def test_rate_for_message(self) -> None:
        msg = map_exchange_error(
            "Rate for ETH is unknown. Exchange via USD or stablecoin first"
        )
        self.assertEqual(
            msg, "Нет курсов для выбранной пары. Сначала обменяй через USD."
        )

    def test_non_positive_message(self) -> None:
        msg = map_exchange_error("Exchange values must be greater than zero")
        self.assertEqual(msg, "Суммы должны быть больше нуля.")

    def test_fallback_message(self) -> None:
        msg = map_exchange_error("some unexpected error")
        self.assertEqual(
            msg, "Не удалось выполнить обмен. Проверь формат и попробуй снова."
        )


if __name__ == "__main__":
    unittest.main()
