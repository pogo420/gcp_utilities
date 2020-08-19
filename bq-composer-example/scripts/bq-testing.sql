  CREATE OR REPLACE TABLE `sublime-mission-251813.practice_data_1.sensor_data_testing`
     AS
     SELECT
         *
     FROM
         `sublime-mission-251813.practice_data_1.sensor_data`
     WHERE
         1=1
         --AND update_time > '{{ prev_start_date_success }}' -- returning null
         -- AND update_time < '{{ ts }}' -- for first time
         AND update_time < TIMESTAMP_SUB('{{ ts }}', INTERVAL {{params.scan_interval}} MINUTE)
;