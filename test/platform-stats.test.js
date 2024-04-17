import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { VALID_MEASUREMENT, VALID_STATION_ID } from './helpers/test-data.js'
import { updateDailyNodeMetrics } from '../lib/platform-stats.js'

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
    await pgClient.query('DELETE FROM daily_node_metrics')
    await pgClient.query('BEGIN TRANSACTION')
    today = await getCurrentDate()
  })

  afterEach(async () => {
    await pgClient.query('END TRANSACTION')
  })

  after(async () => {
    await pgClient.end()
  })

  describe('updateDailyNodeMetrics', () => {
    it('updates daily node metrics for today with multiple measurements', async () => {
      const validStationId2 = VALID_STATION_ID.slice(0, -1) + '1'
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: validStationId2 }
      ]

      await updateDailyNodeMetrics(pgClient, honestMeasurements)

      const { rows } = await pgClient.query('SELECT station_id, metric_date::TEXT FROM daily_node_metrics ORDER BY station_id')
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        { station_id: VALID_STATION_ID, metric_date: today },
        { station_id: validStationId2, metric_date: today }
      ])
    })

    it('ignores duplicate measurements for the same station on the same day', async () => {
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID }
      ]

      await updateDailyNodeMetrics(pgClient, honestMeasurements)

      const { rows } = await pgClient.query('SELECT station_id, metric_date::TEXT FROM daily_node_metrics')
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{ station_id: VALID_STATION_ID, metric_date: today }])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
