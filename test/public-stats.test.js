import assert from 'node:assert'
import pg from 'pg'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { VALID_MEASUREMENT } from './helpers/test-data.js'
import { updatePublicStats } from '../lib/public-stats.js'
import { beforeEach } from 'mocha'

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('public-stats', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  let today
  beforeEach(async () => {
    await pgClient.query('DELETE FROM retrieval_stats')
    await pgClient.query('DELETE FROM indexer_query_stats')

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

  describe('retrieval_stats', () => {
    it('creates or updates the row for today - one miner only', async () => {
      /** @type {import('../lib/preprocess').Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, retrievalResult: 'TIMEOUT' }
      ]
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 2, successful: 1 }
      ])

      honestMeasurements.push({ ...VALID_MEASUREMENT, retrievalResult: 'UNKNOWN_ERROR' })
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, total: 2 + 3, successful: 1 + 1 }
      ])
    })

    it('creates or updates the row for today - multiple miners', async () => {
      /** @type {import('../lib/preprocess').Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, minerId: 'f1first', retrievalResult: 'OK' },
        { ...VALID_MEASUREMENT, minerId: 'f1first', retrievalResult: 'TIMEOUT' },
        { ...VALID_MEASUREMENT, minerId: 'f1second', retrievalResult: 'OK' }
      ]
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, miner_id, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, miner_id: 'f1first', total: 2, successful: 1 },
        { day: today, miner_id: 'f1second', total: 1, successful: 1 }
      ])

      honestMeasurements.push({ ...VALID_MEASUREMENT, minerId: 'f1first', retrievalResult: 'UNKNOWN_ERROR' })
      honestMeasurements.push({ ...VALID_MEASUREMENT, minerId: 'f1second', retrievalResult: 'UNKNOWN_ERROR' })
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, miner_id, total, successful FROM retrieval_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, miner_id: 'f1first', total: 2 + 3, successful: 1 + 1 },
        { day: today, miner_id: 'f1second', total: 1 + 2, successful: 1 + 1 }
      ])
    })
  })

  describe('indexer_query_stats', () => {
    it('creates or updates the row for today', async () => {
      /** @type {import('../lib/preprocess').Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, indexerResult: 'OK' },
        { ...VALID_MEASUREMENT, cid: 'bafy2', indexerResult: 'HTTP_NOT_ADVERTISED' },
        { ...VALID_MEASUREMENT, cid: 'bafy3', indexerResult: 'ERROR_404' }
      ]
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, deals_tested, deals_advertising_http FROM indexer_query_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, deals_tested: 3, deals_advertising_http: 1 }
      ])

      // Notice: this measurement is for the same task as honestMeasurements[0], therefore it's
      // effectively ignored as the other measurement was successful.
      honestMeasurements.push({ ...VALID_MEASUREMENT, indexerResult: 'UNKNOWN_ERROR' })
      // This is a measurement for a new task.
      honestMeasurements.push({ ...VALID_MEASUREMENT, cid: 'bafy4', indexerResult: 'UNKNOWN_ERROR' })
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, deals_tested, deals_advertising_http FROM indexer_query_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, deals_tested: 3 + 4, deals_advertising_http: 1 + 1 }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
