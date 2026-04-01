import ast
from pathlib import Path
import unittest


JOBS_FILE = Path("app/scheduler/jobs.py")


class SchedulerSetupTests(unittest.TestCase):
    def _setup_scheduler_calls(self) -> list[ast.Call]:
        tree = ast.parse(JOBS_FILE.read_text(encoding="utf-8"))
        setup_fn = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "setup_scheduler"
        )
        return [
            node
            for node in ast.walk(setup_fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_job"
        ]

    def test_scheduler_does_not_use_lambda_jobs(self) -> None:
        add_job_calls = self._setup_scheduler_calls()
        self.assertGreaterEqual(len(add_job_calls), 2)

        first_args = [call.args[0] for call in add_job_calls if call.args]
        for arg in first_args:
            self.assertFalse(
                isinstance(arg, ast.Lambda),
                "scheduler.add_job must receive coroutine function directly, not lambda",
            )

        names = {arg.id for arg in first_args if isinstance(arg, ast.Name)}
        self.assertIn("daily_task", names)
        self.assertIn("monthly_task", names)

    def test_scheduler_job_ids_present(self) -> None:
        add_job_calls = self._setup_scheduler_calls()
        ids = []
        for call in add_job_calls:
            for kw in call.keywords:
                if kw.arg == "id" and isinstance(kw.value, ast.Constant):
                    ids.append(kw.value.value)

        self.assertIn("daily_report", ids)
        self.assertIn("monthly_report", ids)

    def test_monthly_job_allows_misfire_recovery(self) -> None:
        add_job_calls = self._setup_scheduler_calls()
        monthly_call = next(
            call
            for call in add_job_calls
            if any(
                kw.arg == "id"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value == "monthly_report"
                for kw in call.keywords
            )
        )

        self.assertTrue(
            any(
                kw.arg == "misfire_grace_time"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value == 86400
                for kw in monthly_call.keywords
            ),
            "monthly_report must tolerate same-day startup delays",
        )
        self.assertTrue(
            any(
                kw.arg == "coalesce"
                and isinstance(kw.value, ast.Constant)
                and kw.value.value is True
                for kw in monthly_call.keywords
            ),
            "monthly_report should coalesce missed executions into one run",
        )


if __name__ == "__main__":
    unittest.main()
