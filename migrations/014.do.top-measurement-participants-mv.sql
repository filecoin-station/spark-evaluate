DROP MATERIALIZED VIEW top_measurement_participants_yesterday_mv;
CREATE MATERIALIZED VIEW top_measurement_participants_yesterday_mv AS
WITH yesterday_station_and_measurement_counts AS (
  SELECT
    day,
    participant_id,
    COUNT(DISTINCT station_id) AS station_count,
    SUM(accepted_measurement_count) AS accepted_measurement_count
  FROM
    recent_station_details
  WHERE
    day = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY
    day, participant_id
),
yesterday_subnet_counts AS (
  SELECT 
    day,
    participant_id,
    COUNT(DISTINCT subnet) AS inet_group_count
  FROM
    recent_participant_subnets
  WHERE
    day = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY
    day, participant_id
)
SELECT
  ysmc.day,
  p.participant_address,
  ysmc.station_count,
  ysmc.accepted_measurement_count,
  ysc.inet_group_count
FROM yesterday_station_and_measurement_counts AS ysmc 
JOIN yesterday_subnet_counts AS ysc ON ysmc.day = ysc.day AND ysmc.participant_id = ysc.participant_id
JOIN participants p ON ysmc.participant_id = p.id
ORDER BY ysmc.accepted_measurement_count DESC;
