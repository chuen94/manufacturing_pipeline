{{ config(
    materialized='external',
    location='../serving/gold_telemetry.parquet'
) }}

-- =====================================================================
-- gold_telemetry : 30-day serving layer for the dashboard.
--
-- Materialized as an external Parquet so the dashboard reads a
-- file and never attaches the DuckDB warehouse. DuckDB is single-writer
-- across processes: if a reader holds the .duckdb file open, the pipeline
-- can't write. Writing Parquet sidesteps that entirely - the writer owns
-- the warehouse, the reader owns the file.
--
-- 30-day window keeps the file small enough to rewrite each cycle.
-- =====================================================================

SELECT
    line_id,
    reading_time,
    parameter_name,
    reading_value
FROM {{ ref('fct_telemetry') }}
WHERE reading_time >= CURRENT_DATE - INTERVAL 30 DAY
