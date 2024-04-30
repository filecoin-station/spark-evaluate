import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { VALID_MEASUREMENT, VALID_STATION_ID } from './helpers/test-data.js'
import { updateDailyStationStats, updateDailyFilStats } from '../lib/platform-stats.js'

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('platform-stats', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  let today
  beforeEach(async () => {
    await pgClient.query('DELETE FROM daily_stations')
    await pgClient.query('DELETE FROM daily_fil')

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
      const validStationId2 = VALID_STATION_ID.slice(0, -1) + '1'
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: validStationId2 }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements)

      const { rows } = await pgClient.query(`
        SELECT station_id, day::TEXT FROM daily_stations
        ORDER BY station_id`
      )
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        { station_id: VALID_STATION_ID, day: today },
        { station_id: validStationId2, day: today }
      ])
    })

    it('ignores duplicate measurements for the same station on the same day', async () => {
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements)

      const { rows } = await pgClient.query('SELECT station_id, day::TEXT FROM daily_stations')
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{ station_id: VALID_STATION_ID, day: today }])
    })
  })

  describe('updateDailyFilStats', () => {
    it('should correctly update daily FIL stats with new transfer events', async () => {
      await updateDailyFilStats(pgClient, { to: 'address1', amount: 100 })
      await updateDailyFilStats(pgClient, { to: 'address1', amount: 200 })

      const { rows } = await pgClient.query(`
        SELECT to_address, amount FROM daily_fil
        WHERE to_address = $1
      `, ['address1'])
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].amount, '300')
    })

    it('should handle multiple addresses in daily FIL stats', async () => {
      await updateDailyFilStats(pgClient, { to: 'address1', amount: 50 })
      await updateDailyFilStats(pgClient, { to: 'address2', amount: 150 })

      const { rows } = await pgClient.query(`
        SELECT to_address, amount FROM daily_fil
        ORDER BY to_address
      `)
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        { to_address: 'address1', amount: '50' },
        { to_address: 'address2', amount: '150' }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
