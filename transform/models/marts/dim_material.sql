{{ config(materialized='table') }}

-- Material dimension: code -> type. type distinguishes a substitutable
-- resin/reclaim from a constant masterbatch additive -- only resin/reclaim
-- rows are eligible to be a run's primary material.
SELECT DISTINCT
    material_code,
    CASE
        WHEN material_code LIKE 'RECYCLED%' THEN 'reclaim'
        WHEN material_code = 'MB-ADD' THEN 'masterbatch'
        ELSE 'resin'
    END AS material_type
FROM {{ ref('stg_shift_materials') }}
