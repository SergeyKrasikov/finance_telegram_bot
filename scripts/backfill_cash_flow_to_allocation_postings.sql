-- Idempotent backfill from legacy cash_flow into allocation_postings.
-- Assumes allocation_nodes/allocation_routes and monthly seed are already applied.

WITH legacy_categories AS (
    SELECT DISTINCT
        cf.users_id AS user_id,
        x.legacy_category_id
    FROM public.cash_flow cf
    CROSS JOIN LATERAL (
        VALUES
            (cf.category_id_from),
            (cf.category_id_to)
    ) AS x(legacy_category_id)
    WHERE cf.users_id IS NOT NULL
      AND x.legacy_category_id IS NOT NULL
)
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
    lc.user_id,
    CONCAT('legacy_bridge_cat_', lc.legacy_category_id),
    c."name",
    CONCAT('Compatibility bridge for legacy category ', lc.legacy_category_id),
    'both',
    lc.legacy_category_id,
    false,
    false,
    true
FROM legacy_categories lc
JOIN public.categories c
  ON c.id = lc.legacy_category_id
WHERE NOT EXISTS (
    SELECT 1
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id = lc.legacy_category_id
      AND (
          an.user_id = lc.user_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = lc.user_id
                AND ugm.active
          )
      )
)
ON CONFLICT (user_id, slug) WHERE user_id IS NOT NULL
DO UPDATE SET
    legacy_category_id = EXCLUDED.legacy_category_id,
    active = true;

INSERT INTO public.allocation_postings (
    "datetime",
    user_id,
    from_node_id,
    to_node_id,
    value,
    currency,
    description,
    metadata
)
SELECT
    cf."datetime",
    cf.users_id,
    from_node.id,
    to_node.id,
    cf.value,
    cf.currency,
    cf.description,
    jsonb_strip_nulls(
        jsonb_build_object(
            'kind', CASE
                WHEN cf.description ILIKE 'exchange to %'
                  OR cf.description ILIKE 'exchange from %'
                  OR cf.description ILIKE 'auto exchange %'
                    THEN 'exchange'
                ELSE 'backfill'
            END,
            'subkind', CASE
                WHEN cf.description ILIKE 'auto exchange %' THEN 'auto'
                WHEN cf.description ILIKE 'exchange to %'
                  OR cf.description ILIKE 'exchange from %'
                    THEN 'manual'
                ELSE 'cash_flow'
            END,
            'origin', 'migration',
            'direction', CASE
                WHEN cf.description ILIKE 'exchange to %'
                  OR cf.description ILIKE 'auto exchange % to %'
                    THEN 'out'
                WHEN cf.description ILIKE 'exchange from %'
                  OR cf.description ILIKE 'auto exchange % from %'
                    THEN 'in'
                ELSE NULL
            END,
            'backfill_kind', 'cash_flow',
            'legacy_cash_flow_id', cf.id,
            'legacy_category_id_from', cf.category_id_from,
            'legacy_category_id_to', cf.category_id_to,
            'backfilled_from_cash_flow', true
        )
    )
FROM public.cash_flow cf
LEFT JOIN LATERAL (
    SELECT an.id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id = cf.category_id_from
      AND (
          an.user_id = cf.users_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = cf.users_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = cf.users_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1
) AS from_node ON true
LEFT JOIN LATERAL (
    SELECT an.id
    FROM public.allocation_nodes an
    WHERE an.active
      AND an.legacy_category_id = cf.category_id_to
      AND (
          an.user_id = cf.users_id
          OR an.user_group_id IN (
              SELECT ugm.user_group_id
              FROM public.user_group_memberships ugm
              WHERE ugm.user_id = cf.users_id
                AND ugm.active
          )
      )
    ORDER BY
        CASE WHEN an.user_id = cf.users_id THEN 0 ELSE 1 END,
        an.id
    LIMIT 1
) AS to_node ON true
WHERE (from_node.id IS NOT NULL OR to_node.id IS NOT NULL)
  AND COALESCE(cf.value, 0) > 0
  AND NOT EXISTS (
      SELECT 1
      FROM public.allocation_postings ap
      WHERE ap.metadata->>'legacy_cash_flow_id' = cf.id::text
         OR (
             ap."datetime" = cf."datetime"
             AND ap.user_id = cf.users_id
             AND ap.from_node_id IS NOT DISTINCT FROM from_node.id
             AND ap.to_node_id IS NOT DISTINCT FROM to_node.id
             AND ap.value = cf.value
             AND ap.currency = cf.currency
             AND COALESCE(ap.description, '') = COALESCE(cf.description, '')
         )
  );

DO $$
DECLARE
    _cash_flow_count bigint;
    _allocation_postings_count bigint;
BEGIN
    SELECT count(*) INTO _cash_flow_count FROM public.cash_flow;
    SELECT count(*) INTO _allocation_postings_count FROM public.allocation_postings;

    IF _cash_flow_count > 0 AND _allocation_postings_count = 0 THEN
        RAISE EXCEPTION
            'allocation_postings backfill produced 0 rows while cash_flow has % rows',
            _cash_flow_count;
    END IF;
END $$;

-- Reclassify legacy exchange rows that were already backfilled before exchange
-- metadata was introduced.
UPDATE public.allocation_postings ap
SET metadata = jsonb_strip_nulls(
    ap.metadata
    || jsonb_build_object(
        'kind', 'exchange',
        'subkind', CASE
            WHEN ap.description ILIKE 'auto exchange %' THEN 'auto'
            ELSE 'manual'
        END,
        'origin', 'migration',
        'direction', CASE
            WHEN ap.description ILIKE 'exchange to %'
              OR ap.description ILIKE 'auto exchange % to %'
                THEN 'out'
            WHEN ap.description ILIKE 'exchange from %'
              OR ap.description ILIKE 'auto exchange % from %'
                THEN 'in'
            ELSE NULL
        END,
        'backfill_kind', 'cash_flow'
    )
)
WHERE ap.metadata->>'kind' = 'backfill'
  AND (
      ap.description ILIKE 'exchange to %'
      OR ap.description ILIKE 'exchange from %'
      OR ap.description ILIKE 'auto exchange %'
  );
