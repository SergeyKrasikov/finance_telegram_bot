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

### Через Docker
```
docker-compose up --build
```

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
  Пример: `50000`
- **Остаток**: кнопка `Остаток` — вывод всех категорий и балансов.
