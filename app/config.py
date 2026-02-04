import os
from dotenv import load_dotenv

load_dotenv()

# Telegram bot token
TOKEN = os.environ.get("TOKEN")

# PostgreSQL connection settings
PG_USER = os.environ.get("POSTGRES_USER")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")
AUTO_APPLY_DB_SCHEMA = os.environ.get("AUTO_APPLY_DB_SCHEMA", "true").lower() in {
    "1",
    "true",
    "yes",
}
DB_BOOTSTRAP_MAX_ATTEMPTS = int(os.environ.get("DB_BOOTSTRAP_MAX_ATTEMPTS", "20"))
DB_BOOTSTRAP_RETRY_DELAY_SEC = float(
    os.environ.get("DB_BOOTSTRAP_RETRY_DELAY_SEC", "2")
)

# Category group IDs from DB
GROUP_SPEND = 8
GROUP_EARNINGS = 10
GROUP_ALL = 14
GROUP_COMMON = 4
GROUP_PERSONAL = 15

# Scheduler settings
DAILY_REPORT_HOUR = 23
DAILY_REPORT_MINUTE = 59
MONTHLY_REPORT_CRON = {"month": "*"}
