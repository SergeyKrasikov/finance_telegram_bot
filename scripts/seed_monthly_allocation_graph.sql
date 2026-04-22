-- Seed for monthly allocation graph used by public.monthly_distribute_cascade(...).
-- Bootstrap config is stored in allocation_seed_profiles* and synchronized below.
-- Assumptions:
-- 1) users 249716305 and 943915310 already exist;
-- 2) legacy categories_category_groups is already filled;
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

INSERT INTO public.allocation_seed_profiles (
    profile_kind,
    slug,
    "name",
    description,
    shared_group_slug,
    shared_group_name,
    shared_group_description,
    active,
    metadata
)
VALUES (
    'monthly',
    'monthly_default_pair',
    'Monthly default pair',
    'Default monthly seed profile for restored pair data',
    'monthly_pair_249716305_943915310',
    'Monthly pair 249716305/943915310',
    'Shared allocation group for monthly cascade',
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
)
ON CONFLICT (profile_kind, slug)
DO UPDATE SET
    "name" = EXCLUDED."name",
    description = EXCLUDED.description,
    shared_group_slug = EXCLUDED.shared_group_slug,
    shared_group_name = EXCLUDED.shared_group_name,
    shared_group_description = EXCLUDED.shared_group_description,
    active = EXCLUDED.active;

INSERT INTO public.allocation_seed_profile_users (
    profile_id,
    user_id,
    scenario_slug,
    scenario_name,
    scenario_description,
    active,
    metadata
)
SELECT
    profile.id,
    cfg.user_id,
    cfg.scenario_slug,
    cfg.scenario_name,
    cfg.scenario_description,
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
FROM public.allocation_seed_profiles profile
JOIN (
    VALUES
        (249716305::bigint, 'monthly_default'::varchar(100), 'Monthly default'::varchar(100), 'Default monthly allocation scenario'::text),
        (943915310::bigint, 'monthly_default'::varchar(100), 'Monthly default'::varchar(100), 'Default monthly allocation scenario'::text)
) AS cfg(user_id, scenario_slug, scenario_name, scenario_description)
  ON true
WHERE profile.profile_kind = 'monthly'
  AND profile.slug = 'monthly_default_pair'
ON CONFLICT (profile_id, user_id)
DO UPDATE SET
    scenario_slug = EXCLUDED.scenario_slug,
    scenario_name = EXCLUDED.scenario_name,
    scenario_description = EXCLUDED.scenario_description,
    active = EXCLUDED.active;

INSERT INTO public.allocation_seed_profile_bindings (
    profile_id,
    user_id,
    root_slug,
    binding_kind,
    bound_slug,
    priority,
    active,
    metadata
)
SELECT
    profile.id,
    cfg.user_id,
    cfg.root_slug,
    cfg.binding_kind,
    cfg.bound_slug,
    100,
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
FROM public.allocation_seed_profiles profile
JOIN (
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
) AS cfg(user_id, root_slug, binding_kind, bound_slug)
  ON true
WHERE profile.profile_kind = 'monthly'
  AND profile.slug = 'monthly_default_pair'
ON CONFLICT (profile_id, user_id, root_slug, binding_kind, bound_slug)
DO UPDATE SET
    priority = EXCLUDED.priority,
    active = EXCLUDED.active;

INSERT INTO public.allocation_seed_profile_root_params (
    profile_id,
    user_id,
    root_slug,
    param_key,
    param_value,
    active,
    metadata
)
SELECT
    profile.id,
    cfg.user_id,
    cfg.root_slug,
    cfg.param_key,
    cfg.param_value,
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
FROM public.allocation_seed_profiles profile
JOIN (
    VALUES
        (249716305::bigint, 'monthly_income_sources'::text, 'source_legacy_group_id'::text, '11'::text),
        (943915310::bigint, 'monthly_income_sources'::text, 'source_legacy_group_id'::text, '11'::text),
        (249716305::bigint, 'extra_income_sources'::text, 'source_legacy_group_id'::text, '12'::text),
        (943915310::bigint, 'extra_income_sources'::text, 'source_legacy_group_id'::text, '12'::text),
        (249716305::bigint, 'debt_reserve'::text, 'spend_legacy_group_id'::text, '8'::text),
        (943915310::bigint, 'debt_reserve'::text, 'spend_legacy_group_id'::text, '8'::text),
        (249716305::bigint, 'debt_reserve'::text, 'personal_legacy_group_id'::text, '15'::text),
        (943915310::bigint, 'debt_reserve'::text, 'personal_legacy_group_id'::text, '15'::text)
) AS cfg(user_id, root_slug, param_key, param_value)
  ON true
WHERE profile.profile_kind = 'monthly'
  AND profile.slug = 'monthly_default_pair'
ON CONFLICT (profile_id, user_id, root_slug, param_key)
DO UPDATE SET
    param_value = EXCLUDED.param_value,
    active = EXCLUDED.active;

CREATE TEMP TABLE tmp_monthly_seed_users
ON COMMIT DROP
AS
SELECT
    sp.id AS profile_id,
    sp.shared_group_slug,
    spu.user_id,
    spu.scenario_slug,
    spu.scenario_name,
    spu.scenario_description
FROM public.allocation_seed_profiles sp
JOIN public.allocation_seed_profile_users spu
  ON spu.profile_id = sp.id
 AND spu.active
WHERE sp.profile_kind = 'monthly'
  AND sp.active;

CREATE TEMP TABLE tmp_monthly_seed_shared_group
ON COMMIT DROP
AS
SELECT DISTINCT
    sp.id AS profile_id,
    sp.shared_group_slug AS slug,
    sp.shared_group_name AS "name",
    COALESCE(sp.shared_group_description, '') AS description
FROM public.allocation_seed_profiles sp
WHERE sp.profile_kind = 'monthly'
  AND sp.active;

CREATE TEMP TABLE tmp_monthly_seed_binding_config
ON COMMIT DROP
AS
SELECT
    spb.user_id,
    spb.root_slug,
    spb.binding_kind,
    spb.bound_slug
FROM public.allocation_seed_profiles sp
JOIN public.allocation_seed_profile_bindings spb
  ON spb.profile_id = sp.id
 AND spb.active
WHERE sp.profile_kind = 'monthly'
  AND sp.active;

CREATE TEMP TABLE tmp_monthly_seed_root_param_config
ON COMMIT DROP
AS
SELECT
    spp.user_id,
    spp.root_slug,
    spp.param_key,
    spp.param_value
FROM public.allocation_seed_profiles sp
JOIN public.allocation_seed_profile_root_params spp
  ON spp.profile_id = sp.id
 AND spp.active
WHERE sp.profile_kind = 'monthly'
  AND sp.active;

DO $$
DECLARE
    _duplicate_user_id bigint;
BEGIN
    SELECT user_id
    INTO _duplicate_user_id
    FROM tmp_monthly_seed_users
    GROUP BY user_id
    HAVING COUNT(*) > 1
    LIMIT 1;

    IF _duplicate_user_id IS NOT NULL THEN
        RAISE EXCEPTION
            'Monthly seed config is ambiguous: user % belongs to more than one active monthly seed profile',
            _duplicate_user_id;
    END IF;
END $$;

-- Shared group for common monthly leaves.
INSERT INTO public.user_groups (slug, "name", description)
SELECT slug, "name", description
FROM tmp_monthly_seed_shared_group
ON CONFLICT (slug) DO NOTHING;

INSERT INTO public.user_group_memberships (user_id, user_group_id, active)
SELECT
    u.user_id,
    g.id,
    true
FROM tmp_monthly_seed_users u
JOIN public.user_groups g
  ON g.slug = u.shared_group_slug
ON CONFLICT (user_id, user_group_id)
DO UPDATE SET active = EXCLUDED.active;

CREATE TEMP TABLE tmp_monthly_seed_user_groups
ON COMMIT DROP
AS
SELECT DISTINCT
    u.profile_id,
    u.user_id,
    u.shared_group_slug,
    g.id AS shared_group_id
FROM tmp_monthly_seed_users u
JOIN public.user_groups g
  ON g.slug = u.shared_group_slug;

SELECT public.ensure_monthly_allocation_nodes_from_legacy(
    ARRAY(SELECT user_id FROM tmp_monthly_seed_users)
);

CREATE TEMP TABLE tmp_monthly_seed_leaf_groups
ON COMMIT DROP
AS
SELECT DISTINCT
    seed.user_id AS owner_user_id,
    an.id AS node_id,
    an.user_id AS node_user_id,
    an.user_group_id,
    an.slug,
    an.legacy_category_id AS category_id,
    ang.legacy_group_id AS group_id,
    c.percent,
    (an.user_group_id IS NOT NULL) AS is_shared
FROM tmp_monthly_seed_users seed
JOIN public.allocation_nodes an
  ON an.active
 AND an.legacy_category_id IS NOT NULL
 AND (
     an.user_id = seed.user_id
     OR an.user_group_id IN (
         SELECT ugm.user_group_id
         FROM public.user_group_memberships ugm
         WHERE ugm.user_id = seed.user_id
           AND ugm.active
     )
 )
JOIN public.allocation_node_groups ang
  ON ang.node_id = an.id
 AND ang.active
LEFT JOIN public.categories c
  ON c.id = an.legacy_category_id;

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
    SELECT user_id
    FROM tmp_monthly_seed_users
) AS u
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

UPDATE public.allocation_nodes root
SET metadata = COALESCE(root.metadata, '{}'::jsonb) - 'partner_source_category_slug'
WHERE root.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND root.slug = 'family_contribution_out'
  AND root.active;

UPDATE public.allocation_nodes root
SET metadata = COALESCE(root.metadata, '{}'::jsonb) - 'source_category_node_id'
WHERE root.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND root.slug = 'salary_primary'
  AND root.active;

UPDATE public.allocation_nodes root
SET metadata = jsonb_strip_nulls(
    COALESCE(root.metadata, '{}'::jsonb)
    - 'source_legacy_group_id'
    - 'spend_legacy_group_id'
    - 'personal_legacy_group_id'
)
WHERE root.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND root.slug IN ('monthly_income_sources', 'extra_income_sources', 'debt_reserve')
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
SELECT
    cfg.user_id,
    'monthly',
    NULL,
    cfg.scenario_slug,
    cfg.scenario_name,
    cfg.scenario_description,
    true
FROM tmp_monthly_seed_users cfg
ON CONFLICT DO NOTHING;

UPDATE public.allocation_scenarios
SET
    schedule_cron = NULL,
    "name" = cfg.scenario_name,
    description = cfg.scenario_description,
    active = true
FROM tmp_monthly_seed_users cfg
WHERE owner_user_id = cfg.user_id
  AND scenario_kind = 'monthly'
  AND slug = cfg.scenario_slug;

DELETE FROM public.allocation_scenario_node_bindings binding
USING public.allocation_scenarios scenario,
      public.allocation_nodes root
WHERE binding.scenario_id = scenario.id
  AND binding.root_node_id = root.id
  AND scenario.scenario_kind = 'monthly'
  AND scenario.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
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

DELETE FROM public.allocation_scenario_root_params param
USING public.allocation_scenarios scenario,
      public.allocation_nodes root
WHERE param.scenario_id = scenario.id
  AND param.root_node_id = root.id
  AND scenario.scenario_kind = 'monthly'
  AND scenario.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND root.user_id = scenario.owner_user_id
  AND root.slug IN (
      'monthly_income_sources',
      'extra_income_sources',
      'debt_reserve'
  )
  AND param.param_key IN (
      'source_legacy_group_id',
      'spend_legacy_group_id',
      'personal_legacy_group_id'
  );

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
    SELECT user_id, root_slug, binding_kind, bound_slug
    FROM tmp_monthly_seed_binding_config
) AS config
JOIN public.allocation_scenarios scenario
  ON scenario.owner_user_id = config.user_id
 AND scenario.scenario_kind = 'monthly'
 AND scenario.slug = (
        SELECT cfg.scenario_slug
        FROM tmp_monthly_seed_users cfg
        WHERE cfg.user_id = config.user_id
    )
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
  AND scenario.slug = (
        SELECT cfg.scenario_slug
        FROM tmp_monthly_seed_users cfg
        WHERE cfg.user_id = scenario.owner_user_id
    )
  AND scenario.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND scenario.active;

INSERT INTO public.allocation_scenario_root_params (
    scenario_id,
    root_node_id,
    param_key,
    param_value,
    active,
    metadata
)
SELECT
    scenario.id,
    root.id,
    config.param_key,
    config.param_value,
    true,
    jsonb_build_object('origin', 'seed_monthly_allocation_graph')
FROM (
    SELECT user_id, root_slug, param_key, param_value
    FROM tmp_monthly_seed_root_param_config
) AS config
JOIN public.allocation_scenarios scenario
  ON scenario.owner_user_id = config.user_id
 AND scenario.scenario_kind = 'monthly'
 AND scenario.slug = (
        SELECT cfg.scenario_slug
        FROM tmp_monthly_seed_users cfg
        WHERE cfg.user_id = config.user_id
    )
 AND scenario.active
JOIN public.allocation_nodes root
  ON root.user_id = config.user_id
 AND root.slug = config.root_slug
 AND root.active;

CREATE TEMP TABLE tmp_monthly_seed_root_target_categories
ON COMMIT DROP
AS
SELECT
    scenario.owner_user_id AS user_id,
    root.slug AS root_slug,
    bound.legacy_category_id AS category_id
FROM public.allocation_scenarios scenario
JOIN public.allocation_nodes root
  ON root.user_id = scenario.owner_user_id
 AND root.active
 AND root.slug IN ('invest_self_report', 'invest_partner_report')
JOIN public.allocation_scenario_node_bindings binding
  ON binding.scenario_id = scenario.id
 AND binding.root_node_id = root.id
 AND binding.binding_kind = 'root_target'
 AND binding.active
JOIN public.allocation_nodes bound
  ON bound.id = binding.bound_node_id
 AND bound.active
WHERE scenario.scenario_kind = 'monthly'
  AND scenario.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND scenario.slug = (
        SELECT cfg.scenario_slug
        FROM tmp_monthly_seed_users cfg
        WHERE cfg.user_id = scenario.owner_user_id
    );

-- A category can move out of legacy group 4 during cleanup. Keep old shared
-- common leaves from being preferred over the current user-owned leaf.
UPDATE public.allocation_nodes an
SET active = false
FROM (
    SELECT DISTINCT shared_group_id, shared_group_slug
    FROM tmp_monthly_seed_user_groups
) AS tug
WHERE an.user_group_id = tug.shared_group_id
  AND an.slug ~ '^cat_[0-9]+$'
  AND NOT EXISTS (
      SELECT 1
      FROM public.allocation_node_groups ang
      WHERE ang.node_id = an.id
        AND ang.legacy_group_id = 4
        AND ang.active
  );

-- Rebuild managed monthly routes from scratch.
-- Older seed versions could leave stale remainder routes on the same source nodes,
-- and validate_allocation_routes() treats every percent = 1 route as a remainder route.
DELETE FROM public.allocation_routes r
USING public.allocation_nodes src
WHERE r.source_node_id = src.id
  AND (
      (src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users) AND src.slug IN (
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
      (src.user_group_id IN (
          SELECT DISTINCT shared_group_id
          FROM tmp_monthly_seed_user_groups
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
        owner_user_id AS user_id,
        COALESCE(SUM(percent), 0) AS percent
    FROM tmp_monthly_seed_leaf_groups
    WHERE group_id = 7
      AND node_user_id = owner_user_id
    GROUP BY owner_user_id
) p
  ON p.user_id = scenario.owner_user_id
 AND p.percent > 0
WHERE scenario.scenario_kind = 'monthly'
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
  AND scenario.active
ON CONFLICT DO NOTHING;

-- salary_primary split
INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 0.10, 'salary_primary -> invest_self_report', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'invest_self_report'
WHERE src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND src.slug = 'salary_primary'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 0.40, 'salary_primary -> family_contribution_out', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'family_contribution_out'
WHERE src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND src.slug = 'salary_primary'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 1.0, 'salary_primary -> self_distribution remainder', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'self_distribution'
WHERE src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
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
JOIN tmp_monthly_seed_users seed_user
  ON seed_user.user_id = scenario.owner_user_id
 AND seed_user.scenario_slug = scenario.slug
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
  AND scenario.active
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 1.0, 'family_contribution_in -> partner_contribution_split', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'partner_contribution_split'
WHERE src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND src.slug = 'family_contribution_in'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 0.10, 'partner_contribution_split -> invest_partner_report', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'invest_partner_report'
WHERE src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND src.slug = 'partner_contribution_split'
ON CONFLICT DO NOTHING;

INSERT INTO public.allocation_routes (source_node_id, target_node_id, percent, description, active)
SELECT src.id, dst.id, 1.0, 'partner_contribution_split -> partner_distribution remainder', true
FROM public.allocation_nodes src
JOIN public.allocation_nodes dst
  ON dst.user_id = src.user_id
 AND dst.slug = 'partner_distribution'
WHERE src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND src.slug = 'partner_contribution_split'
ON CONFLICT DO NOTHING;

-- self_distribution (group 2): common leaves, personal leaves, free remainder
-- Cleanup old incorrect routes from previous seed versions:
-- a legacy category with percent = 1 would create a second remainder route and break validation.
DELETE FROM public.allocation_routes r
USING public.allocation_nodes src
WHERE r.source_node_id = src.id
  AND src.user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
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
    leaf.percent,
    CONCAT('self_distribution -> cat_', leaf.category_id),
    true
FROM tmp_monthly_seed_leaf_groups leaf
JOIN public.allocation_nodes src
  ON src.user_id = leaf.owner_user_id
 AND src.slug = 'self_distribution'
LEFT JOIN public.allocation_nodes common_dst
  ON common_dst.id = (
        SELECT shared_leaf.node_id
        FROM tmp_monthly_seed_leaf_groups shared_leaf
        WHERE shared_leaf.owner_user_id = leaf.owner_user_id
          AND shared_leaf.category_id = leaf.category_id
          AND shared_leaf.is_shared
        ORDER BY shared_leaf.node_id
        LIMIT 1
    )
 AND common_dst.active
LEFT JOIN public.allocation_nodes user_dst
  ON user_dst.id = (
        SELECT user_leaf.node_id
        FROM tmp_monthly_seed_leaf_groups user_leaf
        WHERE user_leaf.owner_user_id = leaf.owner_user_id
          AND user_leaf.category_id = leaf.category_id
          AND user_leaf.node_user_id = leaf.owner_user_id
        ORDER BY user_leaf.node_id
        LIMIT 1
    )
 AND user_dst.active
WHERE leaf.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND leaf.group_id = 2
  AND leaf.node_user_id = leaf.owner_user_id
  AND COALESCE(leaf.percent, 0) > 0
  AND COALESCE(leaf.percent, 0) < 1
  AND NOT EXISTS (
      SELECT 1
      FROM tmp_monthly_seed_root_target_categories invest_leaf
      WHERE invest_leaf.root_slug = 'invest_self_report'
        AND invest_leaf.user_id = leaf.owner_user_id
        AND invest_leaf.category_id = leaf.category_id
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
FROM tmp_monthly_seed_leaf_groups leaf
JOIN public.allocation_nodes src
  ON src.user_id = leaf.owner_user_id
 AND src.slug = 'self_distribution'
JOIN public.allocation_nodes dst
  ON dst.user_id = leaf.owner_user_id
 AND dst.slug = CONCAT('cat_', leaf.category_id)
WHERE leaf.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND leaf.group_id = 6
  AND leaf.node_user_id = leaf.owner_user_id
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
    leaf.percent / COALESCE(
        NULLIF(
            1 - (
                SELECT partner_leaf.percent
                FROM tmp_monthly_seed_root_target_categories invest_leaf
                JOIN tmp_monthly_seed_leaf_groups partner_leaf
                  ON partner_leaf.owner_user_id = invest_leaf.user_id
                 AND partner_leaf.group_id = 3
                 AND partner_leaf.category_id = invest_leaf.category_id
                 AND partner_leaf.node_user_id = invest_leaf.user_id
                WHERE invest_leaf.root_slug = 'invest_partner_report'
                  AND invest_leaf.user_id = leaf.owner_user_id
                LIMIT 1
            ),
            0
        ),
        1
    ),
    CONCAT('partner_distribution -> cat_', leaf.category_id),
    true
FROM tmp_monthly_seed_leaf_groups leaf
JOIN public.allocation_nodes src
  ON src.user_id = leaf.owner_user_id
 AND src.slug = 'partner_distribution'
LEFT JOIN public.allocation_nodes common_dst
  ON common_dst.id = (
        SELECT shared_leaf.node_id
        FROM tmp_monthly_seed_leaf_groups shared_leaf
        WHERE shared_leaf.owner_user_id = leaf.owner_user_id
          AND shared_leaf.category_id = leaf.category_id
          AND shared_leaf.is_shared
        ORDER BY shared_leaf.node_id
        LIMIT 1
    )
 AND common_dst.active
LEFT JOIN public.allocation_nodes user_dst
  ON user_dst.id = (
        SELECT user_leaf.node_id
        FROM tmp_monthly_seed_leaf_groups user_leaf
        WHERE user_leaf.owner_user_id = leaf.owner_user_id
          AND user_leaf.category_id = leaf.category_id
          AND user_leaf.node_user_id = leaf.owner_user_id
        ORDER BY user_leaf.node_id
        LIMIT 1
    )
 AND user_dst.active
WHERE leaf.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND leaf.group_id = 3
  AND leaf.node_user_id = leaf.owner_user_id
  AND COALESCE(leaf.percent, 0) > 0
  AND COALESCE(leaf.percent, 0) < 1
  AND NOT EXISTS (
      SELECT 1
      FROM tmp_monthly_seed_root_target_categories invest_leaf
      WHERE invest_leaf.root_slug = 'invest_partner_report'
        AND invest_leaf.user_id = leaf.owner_user_id
        AND invest_leaf.category_id = leaf.category_id
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
FROM tmp_monthly_seed_leaf_groups leaf
JOIN public.allocation_nodes src
  ON src.user_id = leaf.owner_user_id
 AND src.slug = 'partner_distribution'
JOIN public.allocation_nodes dst
  ON dst.user_id = leaf.owner_user_id
 AND dst.slug = CONCAT('cat_', leaf.category_id)
WHERE leaf.owner_user_id IN (SELECT user_id FROM tmp_monthly_seed_users)
  AND leaf.group_id = 6
  AND leaf.node_user_id = leaf.owner_user_id
ON CONFLICT DO NOTHING;

COMMIT;
