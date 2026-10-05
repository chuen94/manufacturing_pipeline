{{ config(materialized='table') }}

-- One row per production run: the run's own outcome metrics (yield / scrap
-- / downtime) plus its primary material -- the highest-blend_pct
-- resin/reclaim row, excluding masterbatch additives. Deriving it from the
-- as-run blend matters: reading "whatever the formula code implies" is wrong
-- whenever a substitution happened underneath an unchanged formula code.
WITH eligible_materials AS (
    SELECT sm.run_key, sm.material_code, sm.blend_pct
    FROM {{ ref('stg_shift_materials') }} sm
    JOIN {{ ref('dim_material') }} dm ON sm.material_code = dm.material_code
    WHERE dm.material_type IN ('resin', 'reclaim')
),
primary_material AS (
    SELECT run_key, material_code,
           ROW_NUMBER() OVER (PARTITION BY run_key ORDER BY blend_pct DESC) AS rn
    FROM eligible_materials
)

SELECT
    r.run_key,
    r.line_id,
    r.prod_date,
    r.shift,
    r.formula,
    pm.material_code AS primary_material,
    r.prod_lbs,
    r.downtime_min,
    r.total_scrap_lbs,
    r.qc_scrap_lbs,
    r.yield_pct
FROM {{ ref('stg_shift_runs') }} r
LEFT JOIN primary_material pm ON r.run_key = pm.run_key AND pm.rn = 1
