# Prod Monthly Migration

Цель: безопасно перевести прод с legacy monthly/cash_flow-ориентированной модели на allocation-ledger и scenario-driven monthly runtime без потери данных и без поломки Telegram-отчёта.

## Что считается готовностью к прод-миграции

- `allocation_postings` полностью backfill'нут из `cash_flow`.
- Все новые write-path'ы уже пишут в ledger-only или dual-write path, который признан корректным.
- `monthly()` использует `monthly_distribute_cascade()`.
- Monthly roots и routes уже разворачиваются seed'ом.
- Monthly scenario config (`allocation_scenarios`, `allocation_scenario_node_bindings`, `allocation_scenario_root_params`) полностью покрывает runtime без обязательного metadata fallback.
- Monthly bootstrap config (`allocation_seed_profiles*`) хранит seed-данные для pair/scenario bootstrap вне тела самого seed-скрипта.
- SQL-checks на monthly/ledger проходят в action.
- На тестовой базе сверены:
  - monthly postings
  - balances
  - report JSON
  - delete-flow
  - history read-path

## Что ещё нельзя считать завершённым

- Пока не принята финальная доменная модель для `allocation_scenarios` / `allocation_scenario_node_bindings` / `allocation_scenario_root_params`.
- Prep/reserve ветки всё ещё используют metadata-конфиг root-нод и не переведены полностью на scenario-layer.

## Порядок продового переезда

### Этап 1. Подготовка схемы

Раскатить:
- [tables.sql](/Users/kras/Documents/My%20Python%20progects/finance_telegram_bot/tables.sql)
- [sql_functions.sql](/Users/kras/Documents/My%20Python%20progects/finance_telegram_bot/sql_functions.sql)

Ожидаемый результат:
- существуют `allocation_postings`
- существуют `allocation_scenarios`
- существуют `allocation_scenario_node_bindings`
- существует `allocation_scenario_root_params`
- существуют `allocation_seed_profiles*`
- read/write helpers созданы
- `monthly()` и `monthly_distribute_cascade()` обновлены

Проверки:
- структура таблиц создалась без ошибок
- новые функции перекомпилировались
- старые legacy-функции не удалены, а остались как rollback/reference

### Этап 2. Backfill ledger

Запустить backfill:
- [scripts/backfill_cash_flow_to_allocation_postings.sql](/Users/kras/Documents/My%20Python%20progects/finance_telegram_bot/scripts/backfill_cash_flow_to_allocation_postings.sql)

Проверки:
- все положительные `cash_flow` rows отражены в `allocation_postings`
- `metadata.legacy_cash_flow_id` заполнен только у backfill rows
- exchange rows перенесены в обе стороны
- нулевые `cash_flow.value` не ломают backfill и не вставляются в ledger

Контрольные запросы:
- count missing legacy links
- count distinct linked legacy ids
- distribution by `metadata.kind/subkind/origin`

### Этап 3. Seed monthly graph и scenario config

Запустить:
- [scripts/seed_monthly_allocation_graph.sql](/Users/kras/Documents/My%20Python%20progects/finance_telegram_bot/scripts/seed_monthly_allocation_graph.sql)

Ожидаемый результат:
- созданы/обновлены monthly roots
- созданы/обновлены monthly routes
- созданы/обновлены monthly seed profiles
- созданы monthly scenarios
- созданы scenario bindings:
  - `branch_source`
  - `root_target`
  - `bridge_source`

Проверки:
- у monthly пользователей есть `salary_primary`
- есть активный monthly scenario
- есть bindings для всех required roots
- route validation не падает на duplicate remainder routes

### Этап 4. Верификация monthly runtime на продовой копии или тестовом порту

До включения для прода проверить:
- `monthly()` отрабатывает без ошибок
- monthly postings сходятся с ожидаемыми листами
- balances после monthly совпадают с бизнес-ожиданием
- report JSON содержит корректные:
  - `investition`
  - `семейный_взнос`
  - `investition_second`
  - `общие_категории`
  - `second_user_pay`

Отдельно проверить:
- shared rows пишутся с корректным `owner_user_id`
- partner branch пишет postings от имени целевого пользователя
- `cash_flow` не получает новых monthly rows, если путь уже ledger-only

### Этап 5. Переключение runtime

После верификации:
- deploy application code
- scheduler продолжает вызывать `monthly()` как единый entrypoint
- `monthly()` сам находит активные `salary_primary` roots

Важно:
- не удалять legacy `monthly_distribute()`
- `branch_source` и `bridge_source` уже обязательны; перед продовым включением нужно подтвердить, что bindings существуют для всех monthly users

## Пост-deploy проверки

Проверить сразу после раскатки:
- проходит startup приложения
- `/history` читает ledger-backed path
- delete-flow удаляет ledger rows корректно
- daily scheduler читает ledger
- exchange и manual write-path продолжают писать в ledger

Проверить после первого monthly run:
- нет `cash_flow` monthly inserts, если путь уже ledger-only
- есть expected monthly rows в `allocation_postings`
- balances и month report совпадают с ожиданием
- нет ошибок вида:
  - missing allocation root
  - missing scenario binding
  - duplicate remainder route
  - missing bridge source

## Условия для удаления fallback и legacy

`branch_source` и `bridge_source` уже должны быть подтверждены на проде:
- все monthly users покрыты `allocation_scenarios`
- все required roots покрыты `allocation_scenario_node_bindings`
- `branch_source` найден для всех `salary_primary`
- `bridge_source` найден для всех `family_contribution_out`
- single-target roots полностью materialize'ятся из `root_target`

Отдельно для полного ухода от metadata у prep/reserve веток нужно подтвердить:
- `allocation_scenario_root_params` заполнены для всех required monthly roots
- `source_legacy_group_id` найден для всех `monthly_income_sources` и `extra_income_sources`
- `spend_legacy_group_id` и `personal_legacy_group_id` найдены для всех `debt_reserve`

Убирать legacy `cash_flow` monthly dependence можно только после того, как подтверждено:
- balances читаются из ledger
- history читается из ledger
- delete-flow работает по ledger
- monthly и exchange не создают новых критичных записей, которые читаются только из `cash_flow`

## План отката

Если раскатка ломает monthly runtime:

1. Остановить scheduler monthly job.
2. Вернуть предыдущую версию application/sql deploy.
3. Использовать legacy `monthly_distribute()` как reference/rollback path.
4. Не удалять `allocation_postings`; rollback должен быть логическим, а не destructive.
5. Разобрать:
   - missing bindings
   - некорректный seed
   - расхождение balances/report

Если проблема только в scenario config:
- поправить seed/bindings
- не откатывать весь ledger migration, если write/read path уже стабилен

## Что ещё нужно подготовить перед финальным прод-переездом

- Финально решить доменную модель `invest_*_report` против `Н.З.`
- Перевести prep/reserve config с metadata на scenarios/bindings или отдельно подтверждённый config-layer
- Зафиксировать список обязательных prod SQL-checks в CI как release gate
- Подготовить операторский checklist: до monthly run, после monthly run, после rollback
