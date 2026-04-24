-- checks for get_users_id() membership semantics
-- Run with: psql -v ON_ERROR_STOP=1 -f tests/sql/user_membership_helper_checks.sql

BEGIN;

DELETE FROM user_group_memberships WHERE user_id IN (911001, 911002, 911003);
DELETE FROM user_groups WHERE slug IN ('test_membership_group_a', 'test_membership_group_b');
DELETE FROM users WHERE id IN (911001, 911002, 911003);

INSERT INTO users(id, nickname) VALUES
    (911001, 'solo_u'),
    (911002, 'group_u1'),
    (911003, 'group_u2');

DO $$
DECLARE
    solo_members text;
BEGIN
    SELECT string_agg(user_id::text, ',' ORDER BY user_id)
    INTO solo_members
    FROM public.get_users_id(911001);

    IF solo_members <> '911001' THEN
        RAISE EXCEPTION 'Expected single user helper result 911001, got %', solo_members;
    END IF;
END $$;

INSERT INTO user_groups(slug, "name", description)
VALUES
    ('test_membership_group_a', 'membership group a', 'fixture'),
    ('test_membership_group_b', 'membership group b', 'fixture');

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 911002, id
FROM user_groups
WHERE slug = 'test_membership_group_a';

INSERT INTO user_group_memberships(user_id, user_group_id)
SELECT 911003, id
FROM user_groups
WHERE slug = 'test_membership_group_a';

DO $$
DECLARE
    household_members text;
BEGIN
    SELECT string_agg(user_id::text, ',' ORDER BY user_id)
    INTO household_members
    FROM public.get_users_id(911002);

    IF household_members <> '911002,911003' THEN
        RAISE EXCEPTION 'Expected runtime household helper result 911002,911003, got %', household_members;
    END IF;
END $$;

ROLLBACK;
