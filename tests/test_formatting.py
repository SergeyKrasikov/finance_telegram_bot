import unittest
import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "app" / "utils" / "formatting.py"
spec = importlib.util.spec_from_file_location("utils_formatting", MODULE_PATH)
utils_formatting = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(utils_formatting)

format_amount = utils_formatting.format_amount


class FormattingTests(unittest.TestCase):
    def test_rounds_large_numbers_to_2_decimals(self) -> None:
        self.assertEqual(format_amount(12345.678), "12 345.68")
        self.assertEqual(format_amount(-1500.5), "-1 500.50")

    def test_keeps_small_numbers_full(self) -> None:
        self.assertEqual(format_amount(0.0001234500), "0.00012345")
        self.assertEqual(format_amount(-0.004500), "-0.0045")

    def test_integer_boundary(self) -> None:
        self.assertEqual(format_amount(1), "1.00")
        self.assertEqual(format_amount(0), "0")


if __name__ == "__main__":
    unittest.main()
