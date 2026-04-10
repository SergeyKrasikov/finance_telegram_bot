-- Seed for monthly allocation graph used by public.monthly_distribute_cascade(...).
-- Assumptions:
-- 1) users 249716305 and 943915310 already exist;
-- 2) legacy categories_category_groups is already filled;
-- 3) legacy users_groups relation already contains the family pair.
--
-- Run after tables.sql and sql_functions.sql:
--   psql -v ON_ERROR_STOP=1 -f scripts/seed_monthly_allocation_graph.sql

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.users WHERE id = 249716305) THEN
        RAISE EXCEPTION 'User 249716305 not found';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM public.users WHERE id = 943915310) THEN
        RAISE EXCEPTION 'User 943915310 not found';
    END IF;
END $$;

-- Shared group for common monthly leaves.
INSERT INTO public.user_groups (slug, "name", description)
VALUES (
    'monthly_pair_249716305_943915310',
    'Monthly pair 249716305/943915310',
    'Shared allocation group for monthly cascade'
)
ON CONFLICT (slug) DO NOTHING;

INSERT INTO public.user_group_memberships (user_id, user_group_id, active)
SELECT
    u.user_id,
    g.id,
    true
FROM (
    VALUES
        (249716305::bigint),
        (943915310::bigint)
) AS u(user_id)
CROSS JOIN (
    SELECT id
    FROM public.user_groups
    WHERE slug = 'monthly_pair_249716305_943915310'
) AS g
ON CONFLICT (user_id, user_group_id)
DO UPDATE SET active = EXCLUDED.active;

-- Core technical and report nodes per user.
INSERT INTO public.allocation_nodes (
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
SELECT
    u.user_id,
    v.slug,
    v.name,
    v.description,
    'technical',
    NULL,
    false,
    v.include_in_report,
    true
FROM (
    VALUES
        (249716305::bigint),
        (943915310::bigint)
) AS u(user_id)
CROSS JOIN (
    VALUES
        ('monthly_income_sources', 'Monthly income sources', 'Consolidation root for group 11', false),
        ('extra_income_sources', 'Extra income sources', 'Consolidation root for group 12', false),
        ('free_to_gifts', 'Free to gifts', 'Transfer free money from group 6 to gifts bucket', false),
        ('debt_reserve', 'Debt reserve', 'Reserve root for negative personal spend', false),
        ('salary_primary', 'Salary primary', 'Main monthly cascade root', false),
        ('invest_self_report', 'Invest self report', '10 percent of own monthly income', true),
        ('family_contribution_out', 'Family contribution out', '40 percent sent to partner', true),
        ('family_contribution_in', 'Family contribution in', 'Incoming family contribution from partner', false),
        ('partner_contribution_split', 'Partner contribution split', 'Split incoming family contribution', false),
        ('invest_partner_report', 'Invest partner report', '10 percent of received family contribution', true),
        ('self_distribution', 'Self distribution', 'Distribute own remainder', false),
        ('partner_distribution', 'Partner distribution', 'Distribute received family remainder', false)
) AS v(slug, name, description, include_in_report)
WHERE NOT EXISTS (
    SELECT 1
    FROM public.allocation_nodes an
    WHERE an.user_id = u.user_id
      AND an.slug = v.slug
);

-- User-owned leaves for non-common monthly categories.
INSERT INTO public.allocation_nodes (
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
SELECT DISTINCT
    ccg.users_id::bigint,
    CONCAT('cat_', ccg.categories_id),
    c."name",
    CONCAT('Legacy monthly leaf cat_', ccg.categories_id),
    CASE
        WHEN ccg.category_groyps_id = 13 THEN 'income'
        ELSE 'expense'
    END,
    ccg.categories_id,
    true,
    true,
    true
FROM public.categories_category_groups ccg
JOIN public.categories c
  ON c.id = ccg.categories_id
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id IN (1, 2, 3, 6, 7, 9, 13)
  AND ccg.categories_id NOT IN (
      SELECT DISTINCT common_ccg.categories_id
      FROM public.categories_category_groups common_ccg
      WHERE common_ccg.users_id IN (249716305, 943915310)
        AND common_ccg.category_groyps_id = 4
  )
  AND NOT EXISTS (
      SELECT 1
      FROM public.allocation_nodes an
      WHERE an.user_id = ccg.users_id
        AND an.slug = CONCAT('cat_', ccg.categories_id)
  );

-- Shared common leaves owned by the pair group.
INSERT INTO public.allocation_nodes (
    user_group_id,
    slug,
    "name",
    description,
    node_kind,
    legacy_category_id,
    visible,
    include_in_report,
    active
)
SELECT
    g.id,
    CONCAT('cat_', common_ids.categories_id),
    c."name",
    CONCAT('Shared common monthly leaf cat_', common_ids.categories_id),
    'expense',
    common_ids.categories_id,
    true,
    true,
    true
FROM (
    SELECT id
    FROM public.user_groups
    WHERE slug = 'monthly_pair_249716305_943915310'
) AS g
JOIN (
    SELECT DISTINCT categories_id
    FROM public.categories_category_groups
    WHERE users_id IN (249716305, 943915310)
      AND category_groyps_id = 4
) AS common_ids
  ON true
JOIN public.categories c
  ON c.id = common_ids.categories_id
WHERE NOT EXISTS (
    SELECT 1
    FROM public.allocation_nodes an
    WHERE an.user_group_id = g.id
      AND an.slug = CONCAT('cat_', common_ids.categories_id)
);

-- Keep monthly allocation group memberships in sync with legacy category mappings.
-- The backfill script also does this globally, but the monthly seed should be
-- self-contained because monthly_distribute_cascade() reads groups 8/11/12/15
-- from allocation_node_groups at runtime.
INSERT INTO public.allocation_node_groups (
    node_id,
    legacy_group_id,
    active
)
SELECT DISTINCT
    an.id,
    ccg.category_groyps_id,
    true
FROM public.categories_category_groups ccg
JOIN public.allocation_nodes an
  ON an.active
 AND an.legacy_category_id = ccg.categories_id
 AND (
     an.user_id = ccg.users_id
     OR an.user_group_id IN (
         SELECT ugm.user_group_id
         FROM public.user_group_memberships ugm
         WHERE ugm.user_id = ccg.users_id
           AND ugm.active
     )
 )
WHERE ccg.users_id IN (249716305, 943915310)
ON CONFLICT (node_id, legacy_group_id)
DO UPDATE SET active = EXCLUDED.active;

-- Graph-native monthly runtime config.
-- The monthly orchestrator reads these root settings from node metadata instead
-- of hard-coding legacy group/category ids inside public.monthly().
UPDATE public.allocation_nodes root
SET metadata = jsonb_strip_nulls(
    COALESCE(root.metadata, '{}'::jsonb)
    || jsonb_build_object('source_legacy_group_id', config.source_legacy_group_id)
)
FROM (
    VALUES
        (249716305::bigint, 'monthly_income_sources'::text, 11::integer),
        (943915310::bigint, 'monthly_income_sources'::text, 11::integer),
        (249716305::bigint, 'extra_income_sources'::text, 12::integer),
        (943915310::bigint, 'extra_income_sources'::text, 12::integer)
) AS config(user_id, root_slug, source_legacy_group_id)
WHERE root.user_id = config.user_id
  AND root.slug = config.root_slug
  AND root.active;

UPDATE public.allocation_nodes root
SET metadata = jsonb_strip_nulls(
    COALESCE(root.metadata, '{}'::jsonb)
    || jsonb_build_object(
        'spend_legacy_group_id', 8,
        'personal_legacy_group_id', 15
    )
)
WHERE root.user_id IN (249716305, 943915310)
  AND root.slug = 'debt_reserve'
  AND root.active;

UPDATE public.allocation_nodes root
SET metadata = COALESCE(root.metadata, '{}'::jsonb) - 'partner_source_category_slug'
WHERE root.user_id IN (249716305, 943915310)
  AND root.slug = 'family_contribution_out'
  AND root.active;

UPDATE public.allocation_nodes root
SET metadata = COALESCE(root.metadata, '{}'::jsonb) - 'source_category_node_id'
WHERE root.user_id IN (249716305, 943915310)
  AND root.slug = 'salary_primary'
  AND root.active;

-- User-owned monthly scenarios and node bindings.
INSERT INTO public.allocation_scenarios (
    owner_user_id,
    scenario_kind,
    schedule_cron,
    slug,
    "name",
    description,
    active
)
VALUES
    (249716305::bigint, 'monthly', NULL, 'monthly_default', 'Monthly default', 'Default monthly allocation scenario', true),
    (943915310::bigint, 'monthly', NULL, 'monthly_default', 'Monthly default', 'Default monthly allocation scenario', true)
ON CONFLICT DO NOTHING;

UPDATE public.allocation_scenarios
SET
    schedule_cron = NULL,
    "name" = 'Monthly default',
    description = 'Default monthly allocation scenario',
    active = true
WHERE owner_user_id IN (249716305, 943915310)
  AND scenario_kind = 'monthly'
  AND slug = 'monthly_default';

DELETE FROM public.allocation_scenario_node_bindings binding
USING public.allocation_scenarios scenario,
      public.allocation_nodes root
WHERE binding.scenario_id = scenario.id
  AND binding.root_node_id = root.id
  AND scenario.scenario_kind = 'monthly'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND root.user_id = scenario.owner_user_id
  AND root.slug IN (
      'salary_primary',
      'monthly_income_sources',
      'extra_income_sources',
      'free_to_gifts',
      'debt_reserve',
      'invest_self_report',
      'invest_partner_report',
      'family_contribution_out'
  )
  AND binding.binding_kind IN ('branch_source', 'root_target', 'bridge_source');

INSERT INTO public.allocation_scenario_node_bindings (
    scenario_id,
    root_node_id,
    binding_kind,
    bound_node_id,
    priority,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    config.binding_kind,
    bound.id,
    100,
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
FROM (
    VALUES
        (249716305::bigint, 'salary_primary'::text, 'branch_source'::text, 'cat_16'::text),
        (943915310::bigint, 'salary_primary'::text, 'branch_source'::text, 'cat_37'::text),
        (249716305::bigint, 'monthly_income_sources'::text, 'root_target'::text, 'cat_16'::text),
        (943915310::bigint, 'monthly_income_sources'::text, 'root_target'::text, 'cat_37'::text),
        (249716305::bigint, 'extra_income_sources'::text, 'root_target'::text, 'cat_7'::text),
        (943915310::bigint, 'extra_income_sources'::text, 'root_target'::text, 'cat_26'::text),
        (249716305::bigint, 'free_to_gifts'::text, 'root_target'::text, 'cat_7'::text),
        (943915310::bigint, 'free_to_gifts'::text, 'root_target'::text, 'cat_26'::text),
        (249716305::bigint, 'debt_reserve'::text, 'root_target'::text, 'cat_28'::text),
        (943915310::bigint, 'debt_reserve'::text, 'root_target'::text, 'cat_27'::text),
        (249716305::bigint, 'invest_self_report'::text, 'root_target'::text, 'cat_1'::text),
        (943915310::bigint, 'invest_self_report'::text, 'root_target'::text, 'cat_22'::text),
        (249716305::bigint, 'invest_partner_report'::text, 'root_target'::text, 'cat_1'::text),
        (943915310::bigint, 'invest_partner_report'::text, 'root_target'::text, 'cat_22'::text)
) AS config(user_id, root_slug, binding_kind, bound_slug)
JOIN public.allocation_scenarios scenario
  ON scenario.owner_user_id = config.user_id
 AND scenario.scenario_kind = 'monthly'
 AND scenario.slug = 'monthly_default'
 AND scenario.active
JOIN public.allocation_nodes root
  ON root.user_id = config.user_id
 AND root.slug = config.root_slug
 AND root.active
JOIN public.allocation_nodes bound
  ON bound.user_id = config.user_id
 AND bound.slug = config.bound_slug
 AND bound.active;

INSERT INTO public.allocation_scenario_node_bindings (
    scenario_id,
    root_node_id,
    binding_kind,
    bound_node_id,
    priority,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    'bridge_source',
    partner_source.id,
    100,
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'family_contribution_out'
 AND root.active
JOIN public.get_users_id(scenario.owner_user_id) partner
  ON partner.user_id <> scenario.owner_user_id
JOIN public.allocation_nodes partner_source
  ON partner_source.user_id = partner.user_id
 AND partner_source.slug = 'cat_15'
 AND partner_source.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active;

-- A category can move out of legacy group 4 during cleanup. Keep old shared
-- common leaves from being preferred over the current user-owned leaf.
UPDATE public.allocation_nodes an
SET active = false
WHERE an.user_group_id = (
        SELECT id
        FROM public.user_groups
        WHERE slug = 'monthly_pair_249716305_943915310'
    )
  AND an.slug ~ '^cat_[0-9]+$'
  AND an.legacy_category_id NOT IN (
      SELECT DISTINCT ccg.categories_id
      FROM public.categories_category_groups ccg
      WHERE ccg.users_id IN (249716305, 943915310)
        AND ccg.category_groyps_id = 4
  );

-- Rebuild managed monthly routes from scratch.
-- Older seed versions could leave stale remainder routes on the same source nodes,
-- and validate_allocation_routes() treats every percent = 1 route as a remainder route.
DELETE FROM public.allocation_routes r
USING public.allocation_nodes src
WHERE r.source_node_id = src.id
  AND (
      (src.user_id IN (249716305, 943915310) AND src.slug IN (
          'monthly_income_sources',
          'extra_income_sources',
          'free_to_gifts',
          'debt_reserve',
          'salary_primary',
          'invest_self_report',
          'family_contribution_out',
          'family_contribution_in',
          'partner_contribution_split',
          'invest_partner_report',
          'self_distribution',
          'partner_distribution'
      ))
      OR
      (src.user_group_id = (
          SELECT id
          FROM public.user_groups
          WHERE slug = 'monthly_pair_249716305_943915310'
      ) AND src.slug ~ '^cat_[0-9]+$')
  );

-- Single-target roots are seeded via explicit canonical leaves.
-- While we are testing migration on top of restored legacy data, some category groups
-- can contain dirty duplicates (for example, cat_15 leaking into group 1).
-- validate_allocation_routes() interprets every percent = 1 route as a remainder route,
-- so these roots must always have exactly one outgoing route.

-- monthly_income_sources -> canonical income bucket
INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    root.id,
    bound.id,
    1.0,
    'monthly_income_sources -> income bucket',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'monthly_income_sources'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;

-- extra_income_sources -> canonical extra/gift bucket
INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    root.id,
    bound.id,
    1.0,
    'extra_income_sources -> extra income bucket',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'extra_income_sources'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;

-- free_to_gifts -> canonical extra/gift bucket
INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    root.id,
    bound.id,
    p.percent,
    'free_to_gifts -> extra income bucket',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'free_to_gifts'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
JOIN (
    SELECT
        ccg.users_id AS user_id,
        COALESCE(SUM(c.percent), 0) AS percent
    FROM public.categories_category_groups ccg
    JOIN public.categories c
      ON c.id = ccg.categories_id
    WHERE ccg.users_id IN (249716305, 943915310)
      AND ccg.category_groyps_id = 7
    GROUP BY ccg.users_id
) p
  ON p.user_id = scenario.owner_user_id
 AND p.percent > 0
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;
-- debt_reserve -> canonical reserve bucket
INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    root.id,
    bound.id,
    1.0,
    'debt_reserve -> reserve bucket',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'debt_reserve'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;

-- investment report nodes -> canonical own investment leaves
INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    root.id,
    bound.id,
    1.0,
    'invest_self_report -> own investment leaf',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'invest_self_report'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    root.id,
    bound.id,
    1.0,
    'invest_partner_report -> partner investment leaf',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'invest_partner_report'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;

-- salary_primary split
INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 0.10, 'salary_primary -> invest_self_report', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'invest_self_report'
WHERE src.user_id IN (249716305, 943915310)
  AND src.slug = 'salary_primary'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 0.40, 'salary_primary -> family_contribution_out', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'family_contribution_out'
WHERE src.user_id IN (249716305, 943915310)
  AND src.slug = 'salary_primary'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 1.0, 'salary_primary -> self_distribution remainder', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'self_distribution'
WHERE src.user_id IN (249716305, 943915310)
  AND src.slug = 'salary_primary'
ON CONFLICT DO NOTHING;

-- partner bridge and split
INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT
    root.id,
    dst.id,
    1.0,
    'family_contribution_out -> partner family_contribution_in',
    true
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.slug = 'family_contribution_out'
 AND root.active
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'bridge_source'
 AND binding.active
JOIN public.allocation_nodes source_node
  ON source_node.id = binding.bound_node_id
 AND source_node.active
 AND source_node.user_id IS NOT NULL
JOIN public.allocation_nodes dst
  ON dst.user_id = source_node.user_id
 AND dst.slug = 'family_contribution_in'
 AND dst.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.slug = 'monthly_default'
  AND scenario.owner_user_id IN (249716305, 943915310)
  AND scenario.active
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 1.0, 'family_contribution_in -> partner_contribution_split', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'partner_contribution_split'
WHERE src.user_id IN (249716305, 943915310)
  AND src.slug = 'family_contribution_in'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 0.10, 'partner_contribution_split -> invest_partner_report', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'invest_partner_report'
WHERE src.user_id IN (249716305, 943915310)
  AND src.slug = 'partner_contribution_split'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 1.0, 'partner_contribution_split -> partner_distribution remainder', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'partner_distribution'
WHERE src.user_id IN (249716305, 943915310)
  AND src.slug = 'partner_contribution_split'
ON CONFLICT DO NOTHING;

-- self_distribution (group 2): common leaves, personal leaves, free remainder
-- Cleanup old incorrect routes from previous seed versions:
-- a legacy category with percent = 1 would create a second remainder route and break validation.
DELETE FROM public.allocation_routes r
USING public.allocation_nodes src
WHERE r.source_node_id = src.id
  AND src.user_id IN (249716305, 943915310)
  AND src.slug IN ('self_distribution', 'partner_distribution')
  AND r.percent = 1
  AND r.description NOT IN (
      'self_distribution -> free remainder',
      'partner_distribution -> free remainder'
  );

INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    src.id,
    COALESCE(common_dst.id, user_dst.id),
    c.percent,
    CONCAT('self_distribution -> cat_', ccg.categories_id),
    true
FROM public.categories_category_groups ccg
JOIN public.categories c
  ON c.id = ccg.categories_id
JOIN public.allocation_nodes src
  ON src.user_id = ccg.users_id
 AND src.slug = 'self_distribution'
LEFT JOIN public.allocation_nodes common_dst
  ON common_dst.user_group_id = (
         SELECT id
         FROM public.user_groups
         WHERE slug = 'monthly_pair_249716305_943915310'
     )
 AND common_dst.slug = CONCAT('cat_', ccg.categories_id)
 AND common_dst.active
LEFT JOIN public.allocation_nodes user_dst
  ON user_dst.user_id = ccg.users_id
 AND user_dst.slug = CONCAT('cat_', ccg.categories_id)
 AND user_dst.active
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id = 2
  AND COALESCE(c.percent, 0) > 0
  AND COALESCE(c.percent, 0) < 1
  AND NOT EXISTS (
      SELECT 1
      FROM (
          VALUES
              (249716305::bigint, 1::integer),
              (943915310::bigint, 22::integer)
      ) AS invest_leaf(user_id, category_id)
      WHERE invest_leaf.user_id = ccg.users_id
        AND invest_leaf.category_id = ccg.categories_id
  )
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    src.id,
    dst.id,
    1.0,
    'self_distribution -> free remainder',
    true
FROM public.categories_category_groups ccg
JOIN public.allocation_nodes src
  ON src.user_id = ccg.users_id
 AND src.slug = 'self_distribution'
JOIN public.allocation_nodes dst
  ON dst.user_id = ccg.users_id
 AND dst.slug = CONCAT('cat_', ccg.categories_id)
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id = 6
ON CONFLICT DO NOTHING;

-- partner_distribution (group 3): common leaves, partner personal leaves, free remainder
INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    src.id,
    COALESCE(common_dst.id, user_dst.id),
    c.percent / COALESCE(
        NULLIF(
            1 - (
                SELECT inv_c.percent
                FROM (
                    VALUES
                        (249716305::bigint, 1::integer),
                        (943915310::bigint, 22::integer)
                ) AS invest_leaf(user_id, category_id)
                JOIN public.categories_category_groups inv_ccg
                  ON inv_ccg.users_id = invest_leaf.user_id
                 AND inv_ccg.category_groyps_id = 3
                 AND inv_ccg.categories_id = invest_leaf.category_id
                JOIN public.categories inv_c
                  ON inv_c.id = invest_leaf.category_id
                WHERE invest_leaf.user_id = ccg.users_id
                LIMIT 1
            ),
            0
        ),
        1
    ),
    CONCAT('partner_distribution -> cat_', ccg.categories_id),
    true
FROM public.categories_category_groups ccg
JOIN public.categories c
  ON c.id = ccg.categories_id
JOIN public.allocation_nodes src
  ON src.user_id = ccg.users_id
 AND src.slug = 'partner_distribution'
LEFT JOIN public.allocation_nodes common_dst
  ON common_dst.user_group_id = (
         SELECT id
         FROM public.user_groups
         WHERE slug = 'monthly_pair_249716305_943915310'
     )
 AND common_dst.slug = CONCAT('cat_', ccg.categories_id)
 AND common_dst.active
LEFT JOIN public.allocation_nodes user_dst
  ON user_dst.user_id = ccg.users_id
 AND user_dst.slug = CONCAT('cat_', ccg.categories_id)
 AND user_dst.active
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id = 3
  AND COALESCE(c.percent, 0) > 0
  AND COALESCE(c.percent, 0) < 1
  AND NOT EXISTS (
      SELECT 1
      FROM (
          VALUES
              (249716305::bigint, 1::integer),
              (943915310::bigint, 22::integer)
      ) AS invest_leaf(user_id, category_id)
      WHERE invest_leaf.user_id = ccg.users_id
        AND invest_leaf.category_id = ccg.categories_id
  )
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (
    source_node_id,
    target_node_id,
    percent,
    description,
    active
)
SELECT
    src.id,
    dst.id,
    1.0,
    'partner_distribution -> free remainder',
    true
FROM public.categories_category_groups ccg
JOIN public.allocation_nodes src
  ON src.user_id = ccg.users_id
 AND src.slug = 'partner_distribution'
JOIN public.allocation_nodes dst
  ON dst.user_id = ccg.users_id
 AND dst.slug = CONCAT('cat_', ccg.categories_id)
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id = 6
ON CONFLICT DO NOTHING;

COMMIT;
