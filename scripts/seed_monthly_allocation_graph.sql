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
    src.id,
    dst.id,
    1.0,
    'monthly_income_sources -> income bucket',
    true
FROM (
    VALUES
        (249716305::bigint, 'monthly_income_sources'::text, 'cat_16'::text),
        (943915310::bigint, 'monthly_income_sources'::text, 'cat_37'::text)
) AS m(user_id, source_slug, target_slug)
JOIN public.allocation_nodes src
  ON src.user_id = m.user_id
 AND src.slug = m.source_slug
JOIN public.allocation_nodes dst
  ON dst.user_id = m.user_id
 AND dst.slug = m.target_slug
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
    src.id,
    dst.id,
    1.0,
    'extra_income_sources -> extra income bucket',
    true
FROM (
    VALUES
        (249716305::bigint, 'extra_income_sources'::text, 'cat_7'::text),
        (943915310::bigint, 'extra_income_sources'::text, 'cat_26'::text)
) AS m(user_id, source_slug, target_slug)
JOIN public.allocation_nodes src
  ON src.user_id = m.user_id
 AND src.slug = m.source_slug
JOIN public.allocation_nodes dst
  ON dst.user_id = m.user_id
 AND dst.slug = m.target_slug
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
    src.id,
    dst.id,
    1.0,
    'debt_reserve -> reserve bucket',
    true
FROM (
    VALUES
        (249716305::bigint, 'debt_reserve'::text, 'cat_28'::text),
        (943915310::bigint, 'debt_reserve'::text, 'cat_27'::text)
) AS m(user_id, source_slug, target_slug)
JOIN public.allocation_nodes src
  ON src.user_id = m.user_id
 AND src.slug = m.source_slug
JOIN public.allocation_nodes dst
  ON dst.user_id = m.user_id
 AND dst.slug = m.target_slug
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
    src.id,
    dst.id,
    1.0,
    'invest_self_report -> own investment leaf',
    true
FROM (
    VALUES
        (249716305::bigint, 'invest_self_report'::text, 'cat_1'::text),
        (943915310::bigint, 'invest_self_report'::text, 'cat_22'::text)
) AS m(user_id, source_slug, target_slug)
JOIN public.allocation_nodes src
  ON src.user_id = m.user_id
 AND src.slug = m.source_slug
JOIN public.allocation_nodes dst
  ON dst.user_id = m.user_id
 AND dst.slug = m.target_slug
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
    'invest_partner_report -> partner investment leaf',
    true
FROM (
    VALUES
        (249716305::bigint, 'invest_partner_report'::text, 'cat_1'::text),
        (943915310::bigint, 'invest_partner_report'::text, 'cat_22'::text)
) AS m(user_id, source_slug, target_slug)
JOIN public.allocation_nodes src
  ON src.user_id = m.user_id
 AND src.slug = m.source_slug
JOIN public.allocation_nodes dst
  ON dst.user_id = m.user_id
 AND dst.slug = m.target_slug
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
    src.id,
    dst.id,
    1.0,
    'family_contribution_out -> partner family_contribution_in',
    true
FROM (
    VALUES
        (249716305::bigint, 943915310::bigint),
        (943915310::bigint, 249716305::bigint)
) AS p(src_user_id, dst_user_id)
JOIN public.allocation_nodes src
  ON src.user_id = p.src_user_id
 AND src.slug = 'family_contribution_out'
JOIN public.allocation_nodes dst
  ON dst.user_id = p.dst_user_id
 AND dst.slug = 'family_contribution_in'
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
LEFT JOIN public.allocation_nodes user_dst
  ON user_dst.user_id = ccg.users_id
 AND user_dst.slug = CONCAT('cat_', ccg.categories_id)
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id = 2
  AND COALESCE(c.percent, 0) > 0
  AND COALESCE(c.percent, 0) < 1
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
    c.percent,
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
LEFT JOIN public.allocation_nodes user_dst
  ON user_dst.user_id = ccg.users_id
 AND user_dst.slug = CONCAT('cat_', ccg.categories_id)
WHERE ccg.users_id IN (249716305, 943915310)
  AND ccg.category_groyps_id = 3
  AND COALESCE(c.percent, 0) > 0
  AND COALESCE(c.percent, 0) < 1
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
