# finance_telegram_bot

Telegram-бот для учета личных финансов с PostgreSQL и расписанием задач.

## Структура проекта
```
.
├── app.py
├── app/
│   ├── config.py
│   ├── logging_config.py
│   ├── db/
│   │   └── connection.py
│   ├── filters/
│   │   └── category_name.py
│   ├── routers/
│   │   ├── adjustment.py
│   │   ├── balance.py
│   │   ├── commands.py
│   │   ├── earnings.py
│   │   ├── exchange.py
│   │   ├── history.py
│   │   └── spend.py
│   ├── scheduler/
│   │   └── jobs.py
│   ├── services/
│   │   ├── rates.py
│   │   ├── state.py
│   │   └── transactions.py
│   ├── states/
│   │   └── finance.py
│   └── utils/
│       └── keyboards.py
├── download_rates.py
├── download_cripto_rates.py
├── docker-compose.yml
├── scripts/
│   └── apply_db_schema.sh
├── requirements.txt
├── sql_functions.sql
└── tables.sql
```

## Запуск

### Локально
1) Создай `.env` рядом с `app.py` (пример переменных ниже).
2) Установи зависимости:
```
pip install -r requirements.txt
```
3) Запусти:
```
python -m app
```

### Быстрый запуск тестов
```bash
make test
```

Полный прогон (Python + SQL-проверки):
```bash
make test-all
```

Проверка стиля/качества:
```bash
make lint
```

Авто-исправление форматирования:
```bash
make fmt
```

Для SQL-проверок можно переопределить подключение:
```bash
PGPASSWORD=postgres PGHOST=localhost PGPORT=5432 PGUSER=postgres PGDATABASE=finance_test make test-sql
```

### Через Docker
```
docker-compose up --build
```

## Деплой БД (универсально)
- При каждом деплое применяются:
  - `tables.sql` (создание таблиц/индексов, если их нет),
  - `sql_functions.sql` (обновление функций).
- Для этого используется скрипт `scripts/apply_db_schema.sh`.

Ручной запуск:
```
bash scripts/apply_db_schema.sh <postgres_container> <db_user> <db_name> <project_dir>
```

Пример:
```
bash scripts/apply_db_schema.sh finance_telegram_bot_postgres_1 my_finance_bot my_finance_bot /home/kras/finance_telegram_bot
```

## Pre-deploy проверки
- В CI перед деплоем запускаются Python unit-тесты:
  - `tests/test_parsers.py`
  - `tests/test_formatting.py`
  - `tests/test_monthly_logic.py`
  - `tests/test_exchange_error_mapping.py`
- И SQL-контрактные проверки:
  - `tests/sql/predeploy_business_checks.sql`
  - `tests/sql/exchange_negative_checks.sql`
  - `tests/sql/exchange_edge_case_checks.sql`
  - `tests/sql/spend_with_exchange_checks.sql`
  - `tests/sql/spend_with_exchange_negative_checks.sql`
  - `tests/sql/balance_functions_checks.sql`
  - `tests/sql/monthly_business_checks.sql`
  - `tests/sql/monthly_distribute_golden.sql`

## Переменные окружения
Файл: `.env` (располагается рядом с `app.py`). Пример: `.env.example`.
```
TOKEN=                # Токен Telegram-бота
POSTGRES_USER=        # Пользователь БД
POSTGRES_PASSWORD=    # Пароль пользователя БД
PG_HOST=              # Хост PostgreSQL
PG_PORT=              # Порт PostgreSQL
PG_DATABASE=          # Название базы данных
```

## Конфиги (единый список)
- `app/config.py` — переменные окружения и группы категорий.
- `app/logging_config.py` — настройки логирования.
- `docker-compose.yml` — сервисы и параметры контейнеров.
- `requirements.txt` — зависимости Python.
- `tables.sql` — схема БД (создание таблиц).
- `sql_functions.sql` — хранимые функции/процедуры для бота.

Константы расписания: `DAILY_REPORT_HOUR`, `DAILY_REPORT_MINUTE`, `MONTHLY_REPORT_CRON` (см. `app/config.py`).

Пример: чтобы перенести ежедневный отчёт на 20:00, установи `DAILY_REPORT_HOUR = 20` и `DAILY_REPORT_MINUTE = 0` в `app/config.py`.

## DB-модули
- `app/db/connection.py` — базовое подключение и выполнение функций.
- `app/db/transactions.py` — операции с транзакциями и дневной/месячной сводкой.
- `app/db/balances.py` — остатки и балансы по группам/категориям.
- `app/db/currency.py` — курсы/обмен валют.
- `app/db/categories.py` — справочник категорий.
- `app/db/users.py` — пользователи бота.

## Потоки
```
Telegram update
  → app/routers/*
    → app/services/* (логика/валидация/планировщик)
      → app/db/* (доменные функции)
        → PostgreSQL (sql_functions.sql)
```

Примеры:
- `/balance` → `app/routers/balance.py` → `app/db/balances.py`
- `/history` → `app/routers/history.py` → `app/services/transactions.py` → `app/db/transactions.py`
- Расход (сумма) → `app/routers/spend.py` → `app/parsers/input.py` → `app/db/transactions.py`

Scheduler:
- Ежедневный отчёт → `app/scheduler/jobs.py` → `app/db/transactions.py`
- Месячный отчёт → `app/scheduler/jobs.py` → `app/db/transactions.py`

Расписание задач (см. `app/scheduler/jobs.py`):
- Ежедневный отчёт: каждый день в 23:59.
- Месячный отчёт: каждый месяц (cron: `month='*'`).

## Курсы валют (без API)
- Все курсы хранятся в `exchange_rates` как количество валюты за 1 USD (USD = 1).
- При обмене с **USD** всегда обновляется другая валюта (USD не меняется).
- Стейблы (USDT/USDC/DAI/…) **обновляются только при обмене с USD**.
- Другие валюты обновляются при обмене с USD или со стейблами.
- Если обмен без USD/стейблов — курс **получаемой** валюты считается по курсу **отдаваемой**.
- Если для пары нет курсов, обмен запрещён — сначала обменяй через USD.

### Примеры
- **RUB → USDT**: обновляется курс **RUB** (USDT не меняется).  
- **USD → USDT**: обновляется курс **USDT**.  
- **USDT → USD**: обновляется курс **USDT**.  
- **USDT → ETH**: обновляется курс **ETH**, курс USDT не меняется.  
- **RUB → ETH**: курс ETH обновляется на основе курса RUB.  
- **ETH → RUB**: курс RUB обновляется на основе курса ETH.  

Источник расписания: `app/config.py`

## Группы категорий
- `GROUP_SPEND = 8` — категории расходов.
- `GROUP_EARNINGS = 10` — категории доходов.
- `GROUP_ALL = 14` — все категории (общий набор для операций).
- `GROUP_COMMON = 4` — общие/семейные категории.
- `GROUP_PERSONAL = 15` — личные категории.
  
Источник: `app/config.py`

## Заметки
- Основная точка входа: `app.py`.
- Роутеры лежат в `app/routers/`.
- Подключение к БД: `app/db/connection.py`.
- Планировщик задач: `app/scheduler/jobs.py`.

## Команды и сценарии

### Команды
- `/start` — приветствие.
- `/home` — сброс состояния и возврат к основному меню.
- `/balance` — выбор вида баланса (личные/общие/все/по категориям).
- `/history` — история транзакций с возможностью навигации и удаления.
- `/exchange` — обмен валют внутри категории.
- `/adjustment` — ручная корректировка (+/-).

### Быстрые сценарии ввода
- **Расход**: `СУММА [ВАЛЮТА] [КОММЕНТАРИЙ]`, затем выбор категории расхода.  
  Пример: `1200 rub продукты`
- **Доход**: кнопка `Доход`, выбор категории дохода, затем ввод суммы.  
  Пример: `50000` (валюта по умолчанию RUB)
- **Остаток**: кнопка `Остаток` — вывод всех категорий и балансов.
