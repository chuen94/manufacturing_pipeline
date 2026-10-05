-- One row per production run (line, date, shift, formula). Casts straight
-- from the synthetic generator's raw table.
SELECT
    run_key,
    line_id,
    CAST(prod_date AS DATE) AS prod_date,
    shift,
    formula,
    CAST(prod_lbs AS DOUBLE) AS prod_lbs,
    CAST(downtime_min AS DOUBLE) AS downtime_min,
    CAST(total_scrap_lbs AS DOUBLE) AS total_scrap_lbs,
    CAST(qc_scrap_lbs AS DOUBLE) AS qc_scrap_lbs,
    CAST(yield_pct AS DOUBLE) AS yield_pct
FROM {{ source('factory', 'raw_shift_runs') }}
