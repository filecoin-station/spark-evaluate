import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { VALID_MEASUREMENT } from './helpers/test-data.js'
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

  beforeEach(async () => {
    await pgClient.query('DELETE FROM daily_node_metrics')
    await pgClient.query('BEGIN TRANSACTION')
  })

  afterEach(async () => {
    await pgClient.query('END TRANSACTION')
  })

  after(async () => {
    await pgClient.end()
  })

  it('updates daily node metrics with new measurements', async () => {
    const honestMeasurements = [
      { ...VALID_MEASUREMENT, station_id: 'station1' },
      { ...VALID_MEASUREMENT, station_id: 'station2' }
    ]

    await updateDailyNodeMetrics(pgClient, honestMeasurements)

    const { rows } = await pgClient.query('SELECT station_id FROM daily_node_metrics')
    assert.strictEqual(rows.length, 2)
    assert.deepStrictEqual(rows.map(row => row.station_id).sort(), ['station1', 'station2'])
  })

  it('ignores duplicate measurements for the same station on the same day', async () => {
    const honestMeasurements = [
      { ...VALID_MEASUREMENT, station_id: 'station1' },
      { ...VALID_MEASUREMENT, station_id: 'station1' } // Duplicate station_id
    ]

    await updateDailyNodeMetrics(pgClient, honestMeasurements)

    const { rows } = await pgClient.query('SELECT station_id FROM daily_node_metrics')
    assert.strictEqual(rows.length, 1)
    assert.strictEqual(rows[0].station_id, 'station1')
  })
})
