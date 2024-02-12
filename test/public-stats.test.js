import assert from 'node:assert'
import pg from 'pg'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { VALID_MEASUREMENT } from './helpers/test-data.js'
import { mapParticipantsToIds, updateDailyParticipants, updatePublicStats } from '../lib/public-stats.js'
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
    await pgClient.query('DELETE FROM daily_participants')
    // empty `participants` table in such way that the next participants.id will be always 1
    await pgClient.query('TRUNCATE TABLE participants RESTART IDENTITY CASCADE')

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
    it('creates or updates the row for today', async () => {
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
  })

  describe('daily_participants', () => {
    it('submits daily_participants data for today', async () => {
      /** @type {import('../lib/preprocess').Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, participantAddress: '0x10' },
        { ...VALID_MEASUREMENT, participantAddress: '0x10' },
        { ...VALID_MEASUREMENT, participantAddress: '0x20' }
      ]
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows } = await pgClient.query(
        'SELECT day::TEXT, participant_id FROM daily_participants'
      )
      assert.deepStrictEqual(rows, [
        { day: today, participant_id: 1 },
        { day: today, participant_id: 2 }
      ])
    })

    it('creates a new daily_participants row', async () => {
      await updateDailyParticipants(pgClient, new Set(['0x10', '0x20']))

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, participant_id FROM daily_participants'
      )
      assert.deepStrictEqual(created, [
        { day: today, participant_id: 1 },
        { day: today, participant_id: 2 }
      ])
    })

    it('handles participants already seen today', async () => {
      await updateDailyParticipants(pgClient, new Set(['0x10', '0x20']))
      await updateDailyParticipants(pgClient, new Set(['0x10', '0x30', '0x20']))

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, participant_id FROM daily_participants'
      )
      assert.deepStrictEqual(created, [
        { day: today, participant_id: 1 },
        { day: today, participant_id: 2 },
        { day: today, participant_id: 3 }
      ])
    })

    it('maps new participant addresses to new ids', async () => {
      const ids = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x20']))
      ids.sort()
      assert.deepStrictEqual(ids, [1, 2])
    })

    it('maps existing participants to their existing ids', async () => {
      const participants = new Set(['0x10', '0x20'])
      const first = await mapParticipantsToIds(pgClient, participants)
      first.sort()
      assert.deepStrictEqual(first, [1, 2])

      participants.add('0x30')
      participants.add('0x40')
      const second = await mapParticipantsToIds(pgClient, participants)
      second.sort()
      assert.deepStrictEqual(second, [1, 2, 3, 4])
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
        'SELECT day::TEXT, total, advertising_http FROM indexer_query_stats'
      )
      assert.deepStrictEqual(created, [
        { day: today, total: 3, advertising_http: 1 }
      ])

      // Notice: this measurement is for the same task as honestMeasurements[0], therefore it's
      // effectively ignored as the other measurement was successful.
      honestMeasurements.push({ ...VALID_MEASUREMENT, indexerResult: 'UNKNOWN_ERROR' })
      // This is a measurement for a new task.
      honestMeasurements.push({ ...VALID_MEASUREMENT, cid: 'bafy4', indexerResult: 'UNKNOWN_ERROR' })
      await updatePublicStats({ createPgClient, honestMeasurements })

      const { rows: updated } = await pgClient.query(
        'SELECT day::TEXT, total, advertising_http FROM indexer_query_stats'
      )
      assert.deepStrictEqual(updated, [
        { day: today, total: 3 + 4, advertising_http: 1 + 1 }
      ])
    })
  })

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }
})
