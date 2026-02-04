import unittest
import importlib.util
from pathlib import Path

MONTHLY_LOGIC_PATH = Path(__file__).resolve().parents[1] / "app" / "services" / "monthly_logic.py"
FORMATTING_PATH = Path(__file__).resolve().parents[1] / "app" / "utils" / "formatting.py"

monthly_spec = importlib.util.spec_from_file_location("monthly_logic", MONTHLY_LOGIC_PATH)
monthly_logic = importlib.util.module_from_spec(monthly_spec)
assert monthly_spec and monthly_spec.loader
monthly_spec.loader.exec_module(monthly_logic)

format_spec = importlib.util.spec_from_file_location("utils_formatting", FORMATTING_PATH)
utils_formatting = importlib.util.module_from_spec(format_spec)
assert format_spec and format_spec.loader
format_spec.loader.exec_module(utils_formatting)

aggregate_monthly_rows = monthly_logic.aggregate_monthly_rows
build_monthly_message = monthly_logic.build_monthly_message
format_amount = utils_formatting.format_amount


class MonthlyLogicTests(unittest.TestCase):
    def test_aggregate_monthly_rows(self) -> None:
        rows = [
            {
                "user_id": 1,
                "second_user_id": 2,
                "семейный_взнос": 100,
                "общие_категории": 50,
                "investition": 10,
                "month_earnings": 1000,
                "month_spend": 300,
            },
            {
                "user_id": 2,
                "second_user_id": 1,
                "семейный_взнос": 80,
                "общие_категории": 40,
                "investition": 8,
                "month_earnings": 800,
                "month_spend": 200,
            },
        ]

        result = aggregate_monthly_rows(rows)

        self.assertEqual(result[1]["семейный_взнос"], 100)
        self.assertEqual(result[1]["общие_категории"], 90)
        self.assertEqual(result[1]["investition"], 18)
        self.assertEqual(result[1]["month_earnings"], 1000)
        self.assertEqual(result[1]["month_spend"], 300)

        self.assertEqual(result[2]["семейный_взнос"], 80)
        self.assertEqual(result[2]["общие_категории"], 90)
        self.assertEqual(result[2]["investition"], 18)
        self.assertEqual(result[2]["month_earnings"], 800)
        self.assertEqual(result[2]["month_spend"], 200)

    def test_aggregate_handles_missing_second_user(self) -> None:
        rows = [{"user_id": 3, "семейный_взнос": None, "month_earnings": 10}]
        result = aggregate_monthly_rows(rows)
        self.assertEqual(result[3]["семейный_взнос"], 0)
        self.assertEqual(result[3]["month_earnings"], 10)
        self.assertEqual(result[3]["month_spend"], 0)

    def test_build_monthly_message(self) -> None:
        values = {
            "month_earnings": 12345.678,
            "month_spend": 0.000123,
            "семейный_взнос": 100,
            "общие_категории": 25.5,
            "investition": 2,
        }
        message = build_monthly_message(values, format_amount)

        self.assertIn("Всего пришло за месяц 12 345.68₽", message)
        self.assertIn("Всего потрачено за месяц 0.000123₽", message)
        self.assertIn("На семейный взнос 100.00₽", message)
        self.assertIn("На общие категории 25.50₽", message)
        self.assertIn("На инвестиции 2.00₽", message)


if __name__ == "__main__":
    unittest.main()
