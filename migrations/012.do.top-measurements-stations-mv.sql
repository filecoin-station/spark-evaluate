CREATE MATERIALIZED VIEW top_measurement_participants_yesterday_mv AS
SELECT
  participant_address,
  COUNT(DISTINCT inet_group) AS inet_group_count,
  COUNT(DISTINCT station_id) AS station_count,
  SUM(accepted_measurement_count) AS accepted_measurement_count
FROM daily_stations
WHERE day = CURRENT_DATE - INTERVAL '1 day'
GROUP BY participant_address
ORDER BY accepted_measurement_count DESC;
