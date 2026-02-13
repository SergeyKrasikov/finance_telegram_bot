import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
JOBS_PATH = ROOT / "app" / "scheduler" / "jobs.py"
MONTHLY_LOGIC_PATH = ROOT / "app" / "services" / "monthly_logic.py"
FORMATTING_PATH = ROOT / "app" / "utils" / "formatting.py"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class _ModulePatch:
    def __init__(self, overrides: dict[str, types.ModuleType]) -> None:
        self._overrides = overrides
        self._saved: dict[str, types.ModuleType | None] = {}

    def __enter__(self):
        for name, module in self._overrides.items():
            self._saved[name] = sys.modules.get(name)
            sys.modules[name] = module
        return self

    def __exit__(self, exc_type, exc, tb):
        for name, previous in self._saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def _load_jobs_with_rows(rows):
    monthly_logic = _load_module("monthly_logic_for_jobs_test", MONTHLY_LOGIC_PATH)
    formatting = _load_module("formatting_for_jobs_test", FORMATTING_PATH)

    apscheduler_mod = types.ModuleType("apscheduler")
    apscheduler_schedulers_mod = types.ModuleType("apscheduler.schedulers")
    apscheduler_asyncio_mod = types.ModuleType("apscheduler.schedulers.asyncio")

    class AsyncIOScheduler:
        def add_job(self, *args, **kwargs):
            return None

    apscheduler_asyncio_mod.AsyncIOScheduler = AsyncIOScheduler

    app_mod = types.ModuleType("app")
    app_mod.__path__ = []
    app_db_mod = types.ModuleType("app.db")
    app_db_mod.__path__ = []
    app_services_mod = types.ModuleType("app.services")
    app_services_mod.__path__ = []
    app_utils_mod = types.ModuleType("app.utils")
    app_utils_mod.__path__ = []

    app_db_transactions_mod = types.ModuleType("app.db.transactions")

    async def monthly_summary():
        return rows

    async def get_daily_transactions(_user_id):
        return []

    app_db_transactions_mod.monthly_summary = monthly_summary
    app_db_transactions_mod.get_daily_transactions = get_daily_transactions

    app_db_users_mod = types.ModuleType("app.db.users")

    async def get_all_users_id():
        return []

    app_db_users_mod.get_all_users_id = get_all_users_id

    app_config_mod = types.ModuleType("app.config")
    app_config_mod.DAILY_REPORT_HOUR = 23
    app_config_mod.DAILY_REPORT_MINUTE = 59
    app_config_mod.MONTHLY_REPORT_CRON = {"month": "*"}

    app_services_monthly_logic_mod = types.ModuleType("app.services.monthly_logic")
    app_services_monthly_logic_mod.aggregate_monthly_rows = (
        monthly_logic.aggregate_monthly_rows
    )
    app_services_monthly_logic_mod.build_monthly_message = (
        monthly_logic.build_monthly_message
    )

    app_utils_formatting_mod = types.ModuleType("app.utils.formatting")
    app_utils_formatting_mod.format_amount = formatting.format_amount

    patched_modules = {
        "apscheduler": apscheduler_mod,
        "apscheduler.schedulers": apscheduler_schedulers_mod,
        "apscheduler.schedulers.asyncio": apscheduler_asyncio_mod,
        "app": app_mod,
        "app.db": app_db_mod,
        "app.db.transactions": app_db_transactions_mod,
        "app.db.users": app_db_users_mod,
        "app.config": app_config_mod,
        "app.services": app_services_mod,
        "app.services.monthly_logic": app_services_monthly_logic_mod,
        "app.utils": app_utils_mod,
        "app.utils.formatting": app_utils_formatting_mod,
    }

    with _ModulePatch(patched_modules):
        jobs_module = _load_module("jobs_for_monthly_test", JOBS_PATH)
    return jobs_module


class _BotStub:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, user_id: int, text: str) -> None:
        self.messages.append((user_id, text))


class MonthlyReportTaskTests(unittest.TestCase):
    def test_monthly_task_sends_messages_for_each_user(self) -> None:
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
        jobs = _load_jobs_with_rows(rows)
        bot = _BotStub()

        asyncio.run(jobs.monthly_task(bot))

        self.assertEqual(len(bot.messages), 2)
        message_by_user = dict(bot.messages)
        self.assertIn("Всего пришло за месяц 1 000.00₽", message_by_user[1])
        self.assertIn("Всего потрачено за месяц 300.00₽", message_by_user[1])
        self.assertIn("На общие категории 90.00₽", message_by_user[1])
        self.assertIn("На инвестиции 18.00₽", message_by_user[1])

        self.assertIn("Всего пришло за месяц 800.00₽", message_by_user[2])
        self.assertIn("Всего потрачено за месяц 200.00₽", message_by_user[2])
        self.assertIn("На общие категории 90.00₽", message_by_user[2])
        self.assertIn("На инвестиции 18.00₽", message_by_user[2])

    def test_monthly_task_with_empty_data_sends_nothing(self) -> None:
        jobs = _load_jobs_with_rows([])
        bot = _BotStub()

        asyncio.run(jobs.monthly_task(bot))

        self.assertEqual(bot.messages, [])


if __name__ == "__main__":
    unittest.main()
