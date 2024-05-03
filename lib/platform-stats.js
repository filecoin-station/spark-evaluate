import createDebug from 'debug'

const debug = createDebug('spark:platform-stats')

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess').Measurement[]} honestMeasurements
 */
export const updatePlatformStats = async (pgClient, honestMeasurements) => {
  await updateDailyStationStats(pgClient, honestMeasurements)
}

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess').Measurement[]} honestMeasurements
 */
export const updateDailyStationStats = async (pgClient, honestMeasurements) => {
  const measurementCountsPerStation = new Map()
  for (const m of honestMeasurements) {
    measurementCountsPerStation.set(
      m.stationId,
      (measurementCountsPerStation.get(m.stationId) || 0) + 1
    )
  }

  debug('Updating daily station stats, unique_count=%s', measurementCountsPerStation.size)

  // Coalesce to 0 to avoid null + count = null
  // Use EXCLUDED to allow incrementing the count
  // Parameterized stationId to prevent SQL injection
  await pgClient.query(`
    INSERT INTO daily_stations (station_id, day, accepted_measurement_count)
    VALUES (unnest($1::text[]), now(), unnest($2::int[]))
    ON CONFLICT (station_id, day) DO UPDATE
    SET accepted_measurement_count = daily_stations.accepted_measurement_count
                                      + EXCLUDED.accepted_measurement_count
  `, [
    Array.from(measurementCountsPerStation.keys()),
    Array.from(measurementCountsPerStation.values())
  ])
}
