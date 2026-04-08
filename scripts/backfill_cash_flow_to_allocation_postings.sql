-- Idempotent backfill from legacy cash_flow into allocation_postings.
-- Assumes allocation_nodes/allocation_routes and monthly seed are already applied.

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
            'kind', 'backfill',
            'subkind', 'cash_flow',
            'origin', 'migration',
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
