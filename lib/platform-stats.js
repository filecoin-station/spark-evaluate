import createDebug from 'debug'

const debug = createDebug('spark:platform-stats')

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess').Measurement[]} honestMeasurements
 */
export const updatePlatformStats = async (pgClient, honestMeasurements) => {
  await updateDailyNodeMetrics(pgClient, honestMeasurements)
}

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess').Measurement[]} honestMeasurements
 */
export const updateDailyNodeMetrics = async (pgClient, honestMeasurements) => {
  debug('Updating daily node metrics, count=%s', honestMeasurements.length)
  for (const m of honestMeasurements) {
    await pgClient.query(`
      INSERT INTO daily_node_metrics (station_id, day)
      VALUES ($1, now())
      ON CONFLICT (station_id, day) DO NOTHING
    `, [m.stationId]) // TODO: when we add more fields, we should update the ON CONFLICT clause to update the fields
  }
}
