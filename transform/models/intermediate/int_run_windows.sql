-- Start/end timestamp per run. A fuller version would derive run
-- boundaries from changeover-event detection in a raw timeline export;
-- here the synthetic generator already emits one timeline row per run
-- start, so this model's job is just the LEAD-based end-timestamp
-- derivation.
SELECT
    run_key,
    line_id,
    event_ts AS window_start,
    COALESCE(
        LEAD(event_ts) OVER (PARTITION BY line_id ORDER BY event_ts),
        event_ts + INTERVAL 12 HOUR
    ) AS window_end
FROM {{ ref('stg_shift_timeline') }}
