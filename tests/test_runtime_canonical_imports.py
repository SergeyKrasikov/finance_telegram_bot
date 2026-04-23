import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]

RUNTIME_FILES = [
    ROOT / "app" / "filters" / "category_name.py",
    ROOT / "app" / "routers" / "adjustment.py",
    ROOT / "app" / "routers" / "balance.py",
    ROOT / "app" / "routers" / "earnings.py",
    ROOT / "app" / "routers" / "exchange.py",
    ROOT / "app" / "routers" / "spend.py",
    ROOT / "app" / "services" / "rates.py",
]

DB_WRAPPER_FILES = [
    ROOT / "app" / "db" / "balances.py",
    ROOT / "app" / "db" / "categories.py",
    ROOT / "app" / "db" / "connection.py",
    ROOT / "app" / "db" / "currency.py",
    ROOT / "app" / "db" / "transactions.py",
]

TRANSITIONAL_IMPORTS = {
    "app.db.balances": {
        "get_all_balances_v2",
        "get_category_balance_with_currency_v2",
        "get_group_balance_v2",
        "get_remains_v2",
    },
    "app.db.categories": {
        "get_categories_name_v2",
        "get_category_id_from_name_v2",
    },
    "app.db.currency": {
        "exchange_currency_v2",
        "get_currency_list_v2",
    },
    "app.db.transactions": {
        "get_last_transaction_v2",
        "insert_revenue_v2",
        "insert_spend_v2",
        "insert_spend_with_exchange_v2",
    },
}


class RuntimeCanonicalImportTests(unittest.TestCase):
    def test_runtime_modules_do_not_import_transitional_v2_wrappers(self) -> None:
        violations: list[str] = []

        for path in RUNTIME_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                banned_names = TRANSITIONAL_IMPORTS.get(node.module)
                if not banned_names:
                    continue
                imported_names = {alias.name for alias in node.names}
                bad = sorted(imported_names & banned_names)
                if bad:
                    violations.append(f"{path.name}: {', '.join(bad)}")

        self.assertEqual(
            violations,
            [],
            "Runtime modules must import canonical db wrappers only",
        )

    def test_db_wrappers_do_not_expose_transitional_v2_names(self) -> None:
        violations: list[str] = []

        for path in DB_WRAPPER_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if node.name.endswith("_v2"):
                        violations.append(f"{path.name}: {node.name}")
                elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if node.value.endswith("_v2"):
                        violations.append(f"{path.name}: {node.value}")

        self.assertEqual(
            violations,
            [],
            "DB wrappers and allowlist must expose canonical function names only",
        )


if __name__ == "__main__":
    unittest.main()
