-- Long: one row per run x material. blend_pct is the % of that run's input
-- mix this material accounted for; the highest-blend_pct substitutable row
-- per run becomes that run's "primary material" in fct_run.
SELECT
    run_key,
    material_code,
    CAST(blend_pct AS DOUBLE) AS blend_pct
FROM {{ source('factory', 'raw_shift_materials') }}
