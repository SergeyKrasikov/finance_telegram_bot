-- edge-case checks for exchange and get_category_balance calculation consistency
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/exchange_edge_case_checks.sql

BEGIN;

-- cleanup deterministic fixture scope
DELETE FROM allocation_postings WHERE user_id IN (901101, 901102);
DELETE FROM allocation_nodes
WHERE user_id IN (901101, 901102)
   OR legacy_category_id IN (901111);
DELETE FROM cash_flow WHERE users_id IN (901101, 901102);
DELETE FROM user_group_memberships WHERE user_id IN (901101, 901102);
DELETE FROM user_groups WHERE slug = 'exchange_edge_group';
DELETE FROM users WHERE id IN (901101, 901102);
DELETE FROM categories WHERE id IN (901111);
DELETE FROM exchange_rates WHERE currency IN ('USD', 'RUB', 'ETH', 'USDT');

-- fixture users and shared household group for get_users_id()
INSERT INTO users(id, nickname) VALUES
    (901101, 'edge_u1'),
    (901102, 'edge_u2');

INSERT INTO user_groups(slug, "name", description)
VALUES ('exchange_edge_group', 'exchange edge group', 'fixture');

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 901101, id FROM user_groups WHERE slug = 'exchange_edge_group';

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 901102, id FROM user_groups WHERE slug = 'exchange_edge_group';

-- category for balance and exchange tests
INSERT INTO categories(id, "name", "percent") VALUES
    (901111, 'EdgeWallet', 0.00);

INSERT INTO allocation_nodes(
    user_id,
    slug,
    "name",
    description,
    node_kind,
    legacy_category_id,
    visible,
    include_in_report,
    active
)
VALUES
    (901101, 'edge_wallet_1', 'EdgeWallet', 'edge exchange wallet fixture', 'both', 901111, true, true, true),
    (901102, 'edge_wallet_2', 'EdgeWallet', 'edge exchange wallet fixture', 'both', 901111, true, true, true);

-- latest rates + older rates to validate we always use latest
INSERT INTO exchange_rates("datetime", currency, rate) VALUES
    (now() - interval '2 day', 'USD', 1),
    (now() - interval '2 day', 'RUB', 80),
    (now() - interval '2 day', 'ETH', 0.001),
    (now(), 'USD', 1),
    (now(), 'RUB', 100),
    (now(), 'ETH', 0.0005);

-- balance movements in mixed currencies across both users in same runtime household
INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    901101,
    id,
    0.1,
    'ETH',
    'edge in eth',
    jsonb_build_object('kind', 'fixture', 'origin', 'exchange_edge_case')
FROM allocation_nodes
WHERE user_id = 901101
  AND slug = 'edge_wallet_1';

INSERT INTO allocation_postings(user_id, from_node_id, value, currency, description, metadata)
SELECT
    901102,
    id,
    5000,
    'RUB',
    'edge out rub',
    jsonb_build_object('kind', 'fixture', 'origin', 'exchange_edge_case')
FROM allocation_nodes
WHERE user_id = 901102
  AND slug = 'edge_wallet_2';

INSERT INTO allocation_postings(user_id, to_node_id, value, currency, description, metadata)
SELECT
    901102,
    id,
    10,
    'USD',
    'edge in usd',
    jsonb_build_object('kind', 'fixture', 'origin', 'exchange_edge_case')
FROM allocation_nodes
WHERE user_id = 901102
  AND slug = 'edge_wallet_2';

DO $$
DECLARE
    rub_balance numeric;
    usd_balance numeric;
BEGIN
    SELECT public.get_category_balance(901101, 901111, 'RUB') INTO rub_balance;
    SELECT public.get_category_balance(901101, 901111, 'USD') INTO usd_balance;

    -- Expected with latest rates (RUB=100, ETH=0.0005):
    -- +0.1 ETH => +20000 RUB
    -- -5000 RUB => -5000 RUB
    -- +10 USD  => +1000 RUB
    -- total    => 16000 RUB
    IF rub_balance IS NULL OR abs(rub_balance - 16000) > 1e-9 THEN
        RAISE EXCEPTION 'Expected RUB balance 16000, got %', rub_balance;
    END IF;

    -- USD view: +200 -50 +10 = 160
    IF usd_balance IS NULL OR abs(usd_balance - 160) > 1e-9 THEN
        RAISE EXCEPTION 'Expected USD balance 160, got %', usd_balance;
    END IF;
END $$;

-- high stablecoin rate is accepted as factual user rate and propagates as-is
DELETE FROM exchange_rates WHERE currency IN ('USD', 'USDT', 'RUB');
INSERT INTO exchange_rates("datetime", currency, rate) VALUES (now(), 'USD', 1);

SELECT public.exchange(901101, 901111, 1::numeric, 'USD', 99::numeric, 'USDT');
SELECT public.exchange(901101, 901111, 80::numeric, 'RUB', 1::numeric, 'USDT');

DO $$
DECLARE
    usdt_rate numeric;
    rub_rate numeric;
BEGIN
    SELECT rate INTO usdt_rate FROM exchange_rates WHERE currency = 'USDT' ORDER BY datetime DESC LIMIT 1;
    SELECT rate INTO rub_rate FROM exchange_rates WHERE currency = 'RUB' ORDER BY datetime DESC LIMIT 1;

    IF usdt_rate IS NULL OR abs(usdt_rate - 99) > 1e-9 THEN
        RAISE EXCEPTION 'Expected USDT rate 99, got %', usdt_rate;
    END IF;

    -- RUB updated from known stable rate: RUB = USDT * (80 / 1) = 7920
    IF rub_rate IS NULL OR abs(rub_rate - 7920) > 1e-9 THEN
        RAISE EXCEPTION 'Expected RUB rate 7920, got %', rub_rate;
    END IF;
END $$;

ROLLBACK;
