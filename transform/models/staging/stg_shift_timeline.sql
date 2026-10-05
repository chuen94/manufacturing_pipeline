-- One row per run marking when it started. Deliberately simple -- the
-- synthetic generator already knows each run's start time directly, so
-- this model's only job is the type cast.
SELECT
    run_key,
    line_id,
    CAST(event_ts AS TIMESTAMP) AS event_ts
FROM {{ source('factory', 'raw_shift_timeline') }}
