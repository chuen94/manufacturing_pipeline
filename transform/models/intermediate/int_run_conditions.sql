-- Range join: every telemetry reading that falls inside a run's window,
-- aggregated to one row per (run, parameter). This is the mechanism that
-- connects "what material fed the line" (shift report) to "what the
-- machine was actually doing" (PLC telemetry) -- the core join of the
-- whole material-substitution analysis.
SELECT
    w.run_key,
    t.parameter_name,
    AVG(t.reading_value) AS mean_value,
    STDDEV(t.reading_value) AS stddev_value,
    COUNT(*) AS n
FROM {{ ref('int_run_windows') }} w
JOIN {{ ref('fct_telemetry') }} t
    ON t.line_id = w.line_id
    AND t.reading_time >= w.window_start
    AND t.reading_time < w.window_end
GROUP BY 1, 2
