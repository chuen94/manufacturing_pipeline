{{ config(materialized='table') }}

-- Grouped comparison table: same line + formula, grouped by which primary
-- material actually ran. Compare rows only WITHIN the same line_id --
-- the same material pair can behave differently depending on which machine
-- it ran on, so pooling across lines would hide exactly the thing worth
-- finding.
SELECT
    line_id,
    formula,
    primary_material,
    COUNT(*) AS n,
    AVG(yield_pct) AS yield_pct_mean,
    AVG(downtime_min) AS downtime_min_mean,
    AVG(total_scrap_lbs) AS total_scrap_lbs_mean,
    AVG(qc_scrap_lbs) AS qc_scrap_lbs_mean
FROM {{ ref('fct_run') }}
WHERE primary_material IS NOT NULL
GROUP BY 1, 2, 3
