import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]

RUNTIME_FILES = [
    ROOT / "app" / "routers" / "adjustment.py",
    ROOT / "app" / "routers" / "balance.py",
    ROOT / "app" / "routers" / "earnings.py",
    ROOT / "app" / "routers" / "exchange.py",
    ROOT / "app" / "routers" / "spend.py",
    ROOT / "app" / "services" / "rates.py",
]

LEGACY_IMPORTS = {
    "app.db.balances": {
        "get_all_balances",
        "get_category_balance_with_currency",
        "get_group_balance",
        "get_remains",
    },
    "app.db.categories": {
        "get_categories_name",
        "get_category_id_from_name",
    },
    "app.db.currency": {
        "exchange_currency",
        "get_currency_list",
    },
    "app.db.transactions": {
        "insert_revenue",
        "insert_spend",
        "insert_spend_with_exchange",
    },
}


class RuntimeV2ImportTests(unittest.TestCase):
    def test_runtime_modules_use_explicit_v2_db_wrappers(self) -> None:
        violations: list[str] = []

        for path in RUNTIME_FILES:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                banned_names = LEGACY_IMPORTS.get(node.module)
                if not banned_names:
                    continue
                imported_names = {alias.name for alias in node.names}
                bad = sorted(imported_names & banned_names)
                if bad:
                    violations.append(f"{path.name}: {', '.join(bad)}")

        self.assertEqual(
            violations,
            [],
            "Runtime modules must import explicit *_v2 db wrappers only",
        )


if __name__ == "__main__":
    unittest.main()
