-- Unpivot the wide oee table into long format (line_id, reading_time,
-- parameter_name, reading_value). 
SELECT
    line_id,
    reading_time,
    parameter_name,
    CAST(reading_value AS DOUBLE) AS reading_value
FROM (
    UNPIVOT {{ source('factory', 'raw_oee') }}
    ON COLUMNS(* EXCLUDE (line_id, reading_time))
    INTO NAME parameter_name VALUE reading_value
)
