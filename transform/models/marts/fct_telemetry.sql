{{ config(
    materialized='incremental',
    unique_key=['line_id', 'reading_time', 'parameter_name'],
    incremental_strategy='delete+insert'
) }}

-- =====================================================================
-- fct_telemetry : long-format plant telemetry, one row per
--                 (line, reading_time, parameter).
--
-- Long/narrow because the line families expose different sensor sets; a
-- wide table would be sparse and need editing every time a sensor is added.
--
-- INCREMENTAL: the first run builds all history; every run after only
-- reprocesses a trailing 2-hour window and uses delete+insert to dedupe.
-- That keeps each refresh sub-second, which is what makes the ~1-minute
-- micro-batch loop viable. The window also absorbs late-arriving rows.
-- =====================================================================

WITH unioned AS (
    SELECT * FROM {{ ref('stg_process') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_assembly') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_oee') }}
)

SELECT *
FROM unioned
{% if is_incremental() %}
WHERE reading_time >= (SELECT MAX(reading_time) FROM {{ this }}) - INTERVAL 2 HOUR
{% endif %}
