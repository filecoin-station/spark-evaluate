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

/**
 * @param {import('pg').Client} pgClient
 * @param {Object} filEvent
 */
export const updateDailyFilStats = async (pgClient, filEvent) => {
  console.log('Event:', filEvent)

  await pgClient.query(`
    INSERT INTO daily_fil (day, to_address, amount)
    VALUES (now(), $1, $2)
    ON CONFLICT (day, to_address) DO UPDATE
    SET amount = daily_fil.amount + EXCLUDED.amount
  `, [filEvent.to, filEvent.amount])
}
