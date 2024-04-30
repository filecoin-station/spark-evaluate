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
  // TODO: when we add more fields, we will update the ON CONFLICT clause
  // to update those fields, and we won't just use a Set for the stationIds
  // which currently removes all granular measurement details
  const stationIds = [...new Set(honestMeasurements.map(m => m.stationId))]
  debug('Updating daily station stats, unique_count=%s', stationIds.length)

  await pgClient.query(`
    INSERT INTO daily_stations (station_id, day)
    SELECT unnest($1::text[]), now()
    ON CONFLICT (station_id, day) DO NOTHING
  `, [stationIds])
}
