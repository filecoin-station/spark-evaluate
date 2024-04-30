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
  const stationIds = []
  const counts = []
  const honestMeasurementCounts = new Map()
  for (const m of honestMeasurements) {
    honestMeasurementCounts.set(m.stationId, (honestMeasurementCounts.get(m.stationId) || 0) + 1)
  }

  for (const [stationId, count] of honestMeasurementCounts.entries()) {
    stationIds.push(stationId)
    counts.push(count)
  }

  debug('Updating daily station stats, unique_count=%s', stationIds.length)

  // Coalesce to 0 to avoid null + count = null
  // Use EXCLUDED to allow incrementing the count
  // Parameterized stationId to prevent SQL injection
  await pgClient.query(`
    INSERT INTO daily_stations (station_id, day, honest_measurement_count)
    VALUES (unnest($1::text[]), now(), unnest($2::int[]))
    ON CONFLICT (station_id, day) DO UPDATE
    SET honest_measurement_count = COALESCE(daily_stations.honest_measurement_count, 0)
                                    + EXCLUDED.honest_measurement_count
  `, [stationIds, counts])
}
