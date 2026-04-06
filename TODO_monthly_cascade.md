# TODO: Monthly Cascade Migration

Цель: перевести месячное распределение со старой `monthly_distribute()` на граф `allocation_nodes` / `allocation_routes`, не ломая текущий контракт Telegram-отчёта.

## Текущее состояние

- Старый эталон: `public.monthly_distribute(_user_id, _income_category)`.
- Переходная функция: `public.monthly_distribute_cascade(_user_id, _income_category)`.
- Проверка эквивалентности старой и переходной функций уже собрана отдельным compare SQL.
- Переходная функция совпадает со старой по возвращаемому JSON для пользователей `943915310` и `249716305`.

## Что уже переведено

- Введён новый движок:
  - `allocation_distribute_recursive(...)`
  - `allocation_distribute(...)`
- Добавлены переходные helper-функции:
  - `find_allocation_node_id(...)`
  - `get_group_percent_sum(...)`
  - `distribute_with_allocation_fallback(...)`
- `transact_group_to_allocation_fallback(...)` для шагов консолидации many-to-one.
- `reserve_negative_personal_expenses_to_allocation_fallback(...)` для reserve-шага по личным тратам.
- Каскадные шаги групп `1`, `2`, `3`, `6` в `monthly_distribute_cascade()` уже идут через allocation-граф.
- Подготовительный шаг `11 -> 13` уже может идти через allocation-root `monthly_income_sources`.
- Подготовительный шаг `12 -> 7` уже может идти через allocation-root `extra_income_sources`.
- Старые функции и legacy-группы не удалены.

## Инварианты до полного переключения

- `monthly_distribute()` остаётся эталоном.
- `monthly_distribute_cascade()` должна оставаться эквивалентной по JSON-результату.
- Любое изменение новой логики сначала проверяется compare SQL между старой и переходной функциями.
- До полного переноса нельзя убирать legacy helper'ы:
  - `distribute_to_group(...)`
  - `transact_from_group_to_category(...)`
  - `get_categories_id(...)`

## Порядок безопасной миграции

1. Ветка `salary_primary`
- Проверить, что все leaf/report-ноды ветки существуют.
- Перевести расчёт и маршруты только этой ветки на новые `allocation_routes`.
- Прогнать compare SQL.

2. Ветка `salary_secondary`
- Отдельно проверить report-ноды общих категорий до слияния.
- Прогнать compare SQL.

3. Ветка `family_split`
- Проверить межпользовательский сценарий.
- Прогнать compare SQL.

4. Ветка `free_pool`
- Статус: переведена на прямой `allocation_distribute(...)` без fallback.
- Прогнать compare SQL.

5. Подготовительные шаги до каскада
- `11 -> 13`
  Статус: переведён через `transact_group_to_allocation_fallback(...)`, но всё ещё имеет legacy fallback.
- `12 -> 7`
  Статус: переведён через `transact_group_to_allocation_fallback(...)`, но всё ещё имеет legacy fallback.
- 1% с должников в резерв
  Статус: вынесен в `reserve_negative_personal_expenses_to_allocation_fallback(...)`.
- После каждого изменения прогонять compare SQL.

6. Отчёт
- Начать замену legacy расчётов `общие_категории`, `second_user_pay`, `investition`, `investition_second` на суммы из report-нод.
- Сравнить с legacy JSON.
- Новый согласованный контракт для Python-слоя:
  - `общие_категории` уже приходит финальной суммой по общим group-owned leaf-категориям;
  - `second_user_pay` больше не должен дополнительно суммироваться в Python и может быть нулём.

7. Переключение entrypoint
- После стабильного совпадения переименовать/переключить `monthly()` на новую функцию.
- Только после этого начинать удалять fallback.

## Что ещё нужно сделать в схеме

- Финально убрать зависимость движка от legacy category/group функций.
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

- В monthly-path больше нет переходных fallback-вызовов:
  - `distribute_with_allocation_fallback(...)`
  - `transact_group_to_allocation_fallback(...)`
- Все шаги месячного сценария используют только allocation-граф или allocation-only helper'ы.
- Если нужной root-ноды нет, функция падает явно, а не уходит в legacy-ветку.

### 2. Собраны все monthly root-ноды

- Для каждого пользователя/группы, участвующих в monthly-сценарии, существуют и активны:
  - `monthly_income_sources`
  - `extra_income_sources`
  - `salary_primary`
  - `salary_secondary`
  - `family_split`
  - `free_pool`
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
- Допустимо оставить legacy-подсчёт только для:
  - `month_earnings`
  - `month_spend`
  если они по-прежнему читаются из `cash_flow`.

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

- `monthly_distribute()` использует новое тело или перенаправлен на `monthly_distribute_cascade()`.
- Старая реализация удалена или архивирована.
- Переходные helper'ы переименованы или удалены.
- TODO migration можно закрыть только после зелёного compare на финальной реализации.

### Definition of done

Миграция считается завершённой, когда одновременно выполнено всё ниже:

- `monthly_distribute()` использует только allocation-каскад.
- Compare-тест на фиксированном fixture зелёный.
- Итоговый JSON строится из allocation-report, а не из legacy-формул.
- В monthly-коде нет fallback-логики.
