-- monthly aggregation contract checks (SQL side)
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/monthly_business_checks.sql

BEGIN;

DO $$
DECLARE
    u1_family numeric;
    u1_common numeric;
    u1_invest numeric;
    u1_earnings numeric;
    u1_spend numeric;

    u2_family numeric;
    u2_common numeric;
    u2_invest numeric;
    u2_earnings numeric;
    u2_spend numeric;
BEGIN
    WITH rows AS (
        SELECT *
        FROM (VALUES
            (1::bigint, 2::bigint, 100::numeric, 50::numeric, 10::numeric, 1000::numeric, 300::numeric),
            (2::bigint, 1::bigint, 80::numeric, 40::numeric, 8::numeric, 800::numeric, 200::numeric)
        ) t(user_id, second_user_id, family, common, invest, earnings, spend)
    ),
    aggregated AS (
        SELECT uid,
               SUM(family) AS family,
               SUM(common) AS common,
               SUM(invest) AS invest,
               SUM(earnings) AS earnings,
               SUM(spend) AS spend
        FROM (
            SELECT user_id AS uid,
                   family,
                   common,
                   invest,
                   earnings,
                   spend
            FROM rows

            UNION ALL

            SELECT second_user_id AS uid,
                   0::numeric AS family,
                   common,
                   invest,
                   0::numeric AS earnings,
                   0::numeric AS spend
            FROM rows
        ) s
        GROUP BY uid
    )
    SELECT
        MAX(CASE WHEN uid = 1 THEN family END),
        MAX(CASE WHEN uid = 1 THEN common END),
        MAX(CASE WHEN uid = 1 THEN invest END),
        MAX(CASE WHEN uid = 1 THEN earnings END),
        MAX(CASE WHEN uid = 1 THEN spend END),
        MAX(CASE WHEN uid = 2 THEN family END),
        MAX(CASE WHEN uid = 2 THEN common END),
        MAX(CASE WHEN uid = 2 THEN invest END),
        MAX(CASE WHEN uid = 2 THEN earnings END),
        MAX(CASE WHEN uid = 2 THEN spend END)
    INTO
        u1_family, u1_common, u1_invest, u1_earnings, u1_spend,
        u2_family, u2_common, u2_invest, u2_earnings, u2_spend
    FROM aggregated;

    IF u1_family <> 100 OR u1_common <> 90 OR u1_invest <> 18 OR u1_earnings <> 1000 OR u1_spend <> 300 THEN
        RAISE EXCEPTION 'Monthly SQL contract failed for user 1: %, %, %, %, %', u1_family, u1_common, u1_invest, u1_earnings, u1_spend;
    END IF;

    IF u2_family <> 80 OR u2_common <> 90 OR u2_invest <> 18 OR u2_earnings <> 800 OR u2_spend <> 200 THEN
        RAISE EXCEPTION 'Monthly SQL contract failed for user 2: %, %, %, %, %', u2_family, u2_common, u2_invest, u2_earnings, u2_spend;
    END IF;
END $$;

ROLLBACK;
