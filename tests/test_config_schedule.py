from __future__ import annotations

import ast
import unittest
from pathlib import Path


CONFIG_PATH = Path("app/config.py")


class ConfigScheduleTests(unittest.TestCase):
    def test_monthly_report_runs_daily_at_midnight_utc(self) -> None:
        tree = ast.parse(CONFIG_PATH.read_text(encoding="utf-8"))
        monthly_cron = None
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "MONTHLY_REPORT_CRON":
                    monthly_cron = ast.literal_eval(node.value)
                    break
            if monthly_cron is not None:
                break

        self.assertIsNotNone(monthly_cron)
        self.assertEqual(
            monthly_cron,
            {"hour": 0, "minute": 0, "timezone": "UTC"},
        )


if __name__ == "__main__":
    unittest.main()
