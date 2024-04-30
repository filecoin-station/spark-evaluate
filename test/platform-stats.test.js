import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { VALID_MEASUREMENT, VALID_STATION_ID } from './helpers/test-data.js'
import { updateDailyStationStats } from '../lib/platform-stats.js'

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

const VALID_STATION_ID_2 = VALID_STATION_ID.slice(0, -1) + '1'

describe('platform-stats', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  let today
  beforeEach(async () => {
    await pgClient.query('DELETE FROM daily_stations')

    // Run all tests inside a transaction to ensure `now()` always returns the same value
    // See https://dba.stackexchange.com/a/63549/125312
    // This avoids subtle race conditions when the tests are executed around midnight.
    await pgClient.query('BEGIN TRANSACTION')
    today = await getCurrentDate()
  })

  afterEach(async () => {
    await pgClient.query('END TRANSACTION')
  })

  after(async () => {
    await pgClient.end()
  })

  describe('updateDailyStationStats', () => {
    it('updates daily station stats for today with multiple measurements', async () => {
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID_2 }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements)

      const { rows } = await pgClient.query(`
        SELECT station_id, day::TEXT, honest_measurement_count FROM daily_stations
        ORDER BY station_id`
      )
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        { station_id: VALID_STATION_ID, day: today, honest_measurement_count: 1 },
        { station_id: VALID_STATION_ID_2, day: today, honest_measurement_count: 1 }
      ])
    })

    it('counts honest measurements for the same station on the same day', async () => {
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID_2 }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements)

      const { rows } = await pgClient.query(`
        SELECT station_id, day::TEXT, honest_measurement_count FROM daily_stations
        ORDER BY station_id`
      )
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        { station_id: VALID_STATION_ID, day: today, honest_measurement_count: 2 },
        { station_id: VALID_STATION_ID_2, day: today, honest_measurement_count: 1 }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
