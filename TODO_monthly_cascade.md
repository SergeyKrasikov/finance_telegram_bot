# TODO: Monthly Cascade Migration

Цель: перевести месячное распределение со старой `monthly_distribute()` на граф `allocation_nodes` / `allocation_routes`, не ломая текущий контракт Telegram-отчёта.

## Текущее состояние

- Legacy reference/rollback функция: `public.monthly_distribute(_user_id, _income_category)`.
- Переходная функция: `public.monthly_distribute_cascade(_user_id, _income_category)`.
- Проверка эквивалентности старой и переходной функций уже собрана отдельным compare SQL.
- Переходная функция совпадает со старой по возвращаемому JSON для пользователей `943915310` и `249716305`.

## Что уже переведено

- Введён новый движок:
  - `allocation_distribute_recursive(...)`
  - `allocation_distribute(...)`
- Добавлены transition/helper-функции:
  - `find_allocation_node_id(...)`
  - `get_group_percent_sum(...)`
- Каскадные шаги групп `1`, `2`, `3`, `6` в `monthly_distribute_cascade()` уже идут через allocation-граф.
- Подготовительные шаги `11 -> 13`, `12 -> 7` и reserve уже встроены прямо в `monthly_distribute_cascade()` и требуют готовые allocation roots.
- Старые функции и legacy-группы не удалены.

## Текущий граф

Актуальная схема описана подробно в `README.md`, секция `Monthly Allocation Graph`.

Коротко:
- prep roots:
  - `monthly_income_sources`
  - `extra_income_sources`
  - `free_to_gifts`
  - `debt_reserve`
- `free_to_gifts` сохраняет legacy сумму перевода:
  - `free_balance * sum(percent(group 7))`
- Для test-пары reserve-source categories канонизированы:
  - `249716305 -> cat_2, cat_8, cat_9, cat_11`
  - `943915310 -> cat_17, cat_18, cat_20, cat_21, cat_26`
- main roots:
  - `salary_primary`
  - `family_contribution_in`
  - `partner_contribution_split`
  - `self_distribution`
  - `partner_distribution`
- report nodes:
  - `invest_self_report`
  - `family_contribution_out`
  - `invest_partner_report`
- shared leaves:
  - `group 4`, владельцем является `user_group monthly_pair_249716305_943915310`
- user-owned leaves:
  - `group 1`, `2`, `3`, `6`, `7`, `9`, `13`
- single-target roots во время тестовой миграции:
  - используют явные канонические leaf-категории для `249716305` и `943915310`, а не весь legacy group mapping
## Инварианты до полного переключения

- `monthly_distribute()` остаётся legacy reference/rollback функцией.
- `monthly_distribute_cascade()` должна оставаться эквивалентной по JSON-результату.
- Любое изменение новой логики сначала проверяется compare SQL между старой и переходной функциями.
- До полного переноса нельзя убирать legacy helper'ы:
  - `distribute_to_group(...)`
  - `transact_from_group_to_category(...)`
  - `get_categories_id(...)`
- Read-path migration started:
  - monthly allocation helpers уже считают source balance, `month_earnings` / `month_spend` из `allocation_postings`
  - добавлен read-only helper `get_last_allocation_postings(user_id, num)` для наблюдения за новым ledger
  - `/history` читает ledger-backed `get_last_transaction_v2(user_id, num)`
  - delete-flow удаляет `allocation_postings` и linked legacy `cash_flow`, если он есть в metadata
  - daily scheduler уже читает `get_daily_transactions()` из `allocation_postings`
  - `get_daily_allocation_transactions(user_id)` оставлен как явный alias на ledger-read
  - добавлены balance candidate helpers:
    `get_category_balance_v2`, `get_group_balance_v2`, `get_remains_v2`,
    `get_all_balances_v2`, `get_category_balance_with_currency_v2`
  - `/balance` и spend balance checks уже используют v2 balance helpers
  - legacy cash_flow-backed balance helpers оставлены в SQL, но убраны из app allowlist
  - legacy cash_flow-backed `get_last_transaction(...)` оставлен в SQL, но убран из app allowlist
  - category UI lookup уже использует allocation-backed `get_categories_name_v2` и `get_category_id_from_name_v2`
  - legacy `categories_category_groups` lookup helpers оставлены в SQL, но убраны из app allowlist
  - manual spend/revenue app write-paths уже используют allocation-primary `insert_spend_v2` / `insert_revenue_v2`
  - legacy `insert_spend(...)` / `insert_revenue(...)` оставлены в SQL как reference/compare/rollback

## Порядок безопасной миграции

1. Ветка `salary_primary`
- Проверить, что root существует и маршруты до `invest_self_report`, `family_contribution_out` и `self_distribution` корректны.
- Прогнать compare SQL.

2. Ветка `family_contribution_out -> family_contribution_in`
- Отдельно проверить межпользовательский сценарий.
- Прогнать compare SQL.

3. Ветка `partner_contribution_split`
- Проверить split входящего семейного взноса на `invest_partner_report` и `partner_distribution`.
- Прогнать compare SQL.

4. Ветки `self_distribution` и `partner_distribution`
- Проверить leaf/report-ноды личных и общих категорий.
- Проверить remainder route в free-category.
- Прогнать compare SQL.

5. Подготовительные шаги до каскада
- `11 -> 13`
  Статус: allocation-only, logic inlined в `monthly_distribute_cascade()`.
- `12 -> 7`
  Статус: allocation-only, logic inlined в `monthly_distribute_cascade()`.
- 1% с должников в резерв
  Статус: allocation-only, logic inlined в `monthly_distribute_cascade()`.
- После каждого изменения прогонять compare SQL.

6. Отчёт
- Начать замену legacy расчётов `общие_категории`, `second_user_pay`, `investition`, `investition_second` на суммы из report-нод.
- Классификация shared leaves и investment leaves уже переведена на `allocation_nodes` / `allocation_routes`.
- Сравнить с legacy JSON.
- Новый согласованный контракт для Python-слоя:
  - `общие_категории` уже приходит финальной суммой по общим group-owned leaf-категориям;
  - `second_user_pay` больше не должен дополнительно суммироваться в Python и может быть нулём.

7. Переключение entrypoint
- `monthly()` переведён на `monthly_distribute_cascade()`.
- `monthly_distribute()` остаётся в базе как legacy reference/rollback и пока не удаляется.

## Что ещё нужно сделать в схеме

- Финально убрать зависимость движка от legacy category/group функций.
- Free-category для `free_to_gifts` уже определяется через allocation remainder leaf, а не через `get_categories_id(group 6)`.
- Legacy share для `free_to_gifts` перенесён из orchestrator в allocation route.
- В схему добавлена `allocation_postings`; leaf-проводки allocation-движка уже пишутся туда ledger-only, без новых rows в `cash_flow`.
- Добавлены read-helper'ы для нового ledger: `get_allocation_node_balance(...)` и `get_allocation_node_balance_by_slug(...)`.
- Deploy now runs idempotent backfill `cash_flow -> allocation_postings`; historical/backfill rows may carry `metadata.legacy_cash_flow_id`.
- Определить финальную модель источника для monthly run:
  - либо старт от одной root-ноды,
  - либо orchestrator, который запускает несколько веток.
- Финально решить, остаётся ли `cash_flow` на legacy category ids или переводится на `allocation_nodes.id`.

## Что нельзя делать пока рано

- Удалять `monthly_distribute()`.
- Удалять compare SQL.
- Переводить сразу несколько веток без промежуточной проверки.
- Менять Telegram-формат отчёта до завершения миграции бизнес-логики.

## Finalization Checklist

Этот checklist определяет момент, когда `monthly_distribute_cascade()` можно считать полной заменой legacy `monthly_distribute()`.

### 1. Allocation-only monthly path

- В monthly-path больше нет переходных fallback-вызовов.
- Подготовительные шаги `11 -> 13`, `12 -> 7` и reserve встроены прямо в `monthly_distribute_cascade()`.
- Если нужной root-ноды нет, функция падает явно, а не уходит в legacy-ветку.

### 2. Собраны все monthly root-ноды

- Для каждого пользователя/группы, участвующих в monthly-сценарии, существуют и активны:
  - `monthly_income_sources`
  - `extra_income_sources`
  - `free_to_gifts`
  - `salary_primary`
  - `self_distribution`
  - `family_contribution_out`
  - `family_contribution_in`
  - `partner_contribution_split`
  - `partner_distribution`
  - `invest_self_report`
  - `invest_partner_report`
  - `debt_reserve` если reserve остаётся частью нового графа
- Для этих нод проверены:
  - active routes
  - отсутствие циклов
  - корректные leaf-ноды
  - нужные report-ноды

### 3. Финальный JSON не зависит от legacy percent/group formulas

- Поля:
  - `семейный_взнос`
  - `общие_категории`
  - `second_user_pay`
  - `investition`
  - `investition_second`
  считаются через report-ноды / allocation aggregation.
- Для упрощённого Telegram-контракта допустимо:
  - `общие_категории` считать единой суммой по общим group-owned leaf-нодам;
  - `second_user_pay` вернуть как `0`, если Python больше не использует его как отдельную прибавку.
- Эти поля больше не зависят от:
  - `categories.percent`
  - `category_groups`
  - ручных формул из `_sum_value`
- `month_earnings` и `month_spend` читаются из `allocation_postings`, а не из `cash_flow`.

### 4. Reserve rule зафиксирован

- Явно выбран и задокументирован один вариант:
  - `legacy-compatible reserve`
  - `personal-spend-only reserve`
- Тесты проверяют именно выбранное правило.
- В коде нет скрытого смешанного поведения reserve.

### 5. Compare tests зелёные

- Основной compare old vs cascade проходит на фиксированном fixture.
- Reserve compare проходит на отдельном fixture/срезе.
- Тесты не зависят от продового `cash_flow`.
- При необходимости отдельно зафиксирован и test bridge для legacy category ids.

### 6. Убран временный compatibility bridge

- Если `cash_flow` остаётся на `categories(id)`, это решение зафиксировано как официальная совместимость.
- Если `cash_flow` переводится на `allocation_nodes.id`, удалены:
  - временная bridge-логика
  - test override для `allocation_distribute_recursive(...)`
- В прод-коде не осталось скрытых тестовых обходов схемы.

### 7. Переключён entrypoint

- `monthly()` использует `monthly_distribute_cascade()`.
- Старая `monthly_distribute()` оставлена как legacy reference/rollback.
- Переходные helper'ы переименованы или удалены.
- TODO migration можно закрыть только после зелёного compare на финальной реализации.

### Definition of done

Миграция считается завершённой, когда одновременно выполнено всё ниже:

- Legacy `monthly_distribute()` либо удалена, либо отдельно помечена как reference-only.
- Compare-тест на фиксированном fixture зелёный.
- Итоговый JSON строится из allocation-report, а не из legacy-формул.
- В monthly-коде нет fallback-логики.
