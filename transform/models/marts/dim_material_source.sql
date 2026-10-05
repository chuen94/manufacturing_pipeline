{{ config(materialized='table') }}

-- =====================================================================
-- dim_material_source : slowly-changing source (Type 2) -> material dimension.
--
-- The material-source log is append-only event data, so the right tool is a derived
-- SCD2 (valid_from / valid_to via LEAD) rather than a dbt snapshot -
-- only need snapshots when the source is mutable and history isn't kept.
-- With valid_from/valid_to you can answer "what material was in source N on
-- date D" by joining on a date-between range.
-- =====================================================================

SELECT
    source_id,
    material_code,
    assigned_date AS valid_from,
    COALESCE(
        LEAD(assigned_date) OVER (PARTITION BY source_id ORDER BY assigned_date),
        DATE '9999-12-31'
    ) AS valid_to
FROM {{ source('factory', 'raw_material_source') }}
