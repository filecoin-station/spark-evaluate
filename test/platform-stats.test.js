import assert from 'node:assert'
import pg from 'pg'
import { beforeEach, describe, it } from 'mocha'

import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import {
  VALID_MEASUREMENT,
  VALID_STATION_ID,
  VALID_PARTICIPANT_ADDRESS,
  VALID_INET_GROUP
} from './helpers/test-data.js'
import {
  mapParticipantsToIds,
  updateDailyParticipants,
  updateDailyStationStats,
  updatePlatformStats,
  updateTopMeasurementParticipants
} from '../lib/platform-stats.js'

/** @typedef {import('../lib/preprocess.js').Measurement} Measurement */

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

  describe('updateDailyStationStats', () => {
    it('updates daily station stats for today with multiple measurements', async () => {
      const stationIdMeasurement1 = { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID }
      const stationIdMeasurement2 = { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID_2 }

      /** @type {Measurement[]} */
      const honestMeasurements = [
        stationIdMeasurement1,
        stationIdMeasurement2,
        { ...stationIdMeasurement1, participantAddress: '0x20' },
        { ...stationIdMeasurement2, inet_group: 'other-group' }
      ]
      /** @type {Measurement[]} */
      const allMeasurements = [
        ...honestMeasurements,
        { ...stationIdMeasurement1, fraudAssessment: 'TASK_NOT_IN_ROUND' },
        { ...stationIdMeasurement2, fraudAssessment: 'TASK_NOT_IN_ROUND' },
        { ...stationIdMeasurement1, participantAddress: '0x20', fraudAssessment: 'TASK_NOT_IN_ROUND' },
        { ...stationIdMeasurement2, inet_group: 'other-group', fraudAssessment: 'TASK_NOT_IN_ROUND' }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements, today)

      const { rows } = await pgClient.query(`
        SELECT
          station_id,
          day::TEXT,
          participant_address,
          inet_group,
          accepted_measurement_count,
          total_measurement_count
        FROM
          daily_stations
        ORDER BY station_id`
      )
      assert.strictEqual(rows.length, 4)
      assert.deepStrictEqual(rows, [
        {
          day: today,
          station_id: VALID_STATION_ID,
          participant_address: VALID_PARTICIPANT_ADDRESS,
          inet_group: VALID_INET_GROUP,
          accepted_measurement_count: 1,
          total_measurement_count: 2
        },
        {
          day: today,
          station_id: VALID_STATION_ID,
          participant_address: '0x20',
          inet_group: VALID_INET_GROUP,
          accepted_measurement_count: 1,
          total_measurement_count: 2
        },
        {
          day: today,
          station_id: VALID_STATION_ID_2,
          participant_address: VALID_PARTICIPANT_ADDRESS,
          inet_group: VALID_INET_GROUP,
          accepted_measurement_count: 1,
          total_measurement_count: 2
        },
        {
          day: today,
          station_id: VALID_STATION_ID_2,
          participant_address: VALID_PARTICIPANT_ADDRESS,
          inet_group: 'other-group',
          accepted_measurement_count: 1,
          total_measurement_count: 2
        }
      ])
    })

    it('counts measurements for the same station on the same day', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID_2 }
      ]

      /** @type {Measurement[]} */
      const allMeasurements = [
        ...honestMeasurements,
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID, fraudAssessment: 'TASK_NOT_IN_ROUND' },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID, fraudAssessment: 'TASK_NOT_IN_ROUND' },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID_2, fraudAssessment: 'TASK_NOT_IN_ROUND' }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements, today)

      const { rows } = await pgClient.query(`
        SELECT station_id, day::TEXT, accepted_measurement_count, total_measurement_count
        FROM daily_stations
        ORDER BY station_id`
      )
      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        {
          station_id: VALID_STATION_ID,
          day: today,
          accepted_measurement_count: 2,
          total_measurement_count: 4
        },
        {
          station_id: VALID_STATION_ID_2,
          day: today,
          accepted_measurement_count: 1,
          total_measurement_count: 2
        }
      ])
    })

    it('ignores measurements without .stationId', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: null },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID }
      ]

      /** @type {Measurement[]} */
      const allMeasurements = [
        ...honestMeasurements,
        { ...VALID_MEASUREMENT, stationId: null, fraudAssessment: 'TASK_NOT_IN_ROUND' },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID, fraudAssessment: 'TASK_NOT_IN_ROUND' }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements, today)

      const { rows } = await pgClient.query('SELECT station_id, day::TEXT FROM daily_stations')
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows, [{ station_id: VALID_STATION_ID, day: today }])
    })

    it('updates top measurements participants yesterday materialized view', async () => {
      const validStationId3 = VALID_STATION_ID.slice(0, -1) + '2'
      const yesterday = await getYesterdayDate()

      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID, participantAddress: 'f1abc' },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID, participantAddress: 'f1abc' },
        { ...VALID_MEASUREMENT, stationId: VALID_STATION_ID_2, participantAddress: 'f1abc' },
        { ...VALID_MEASUREMENT, stationId: validStationId3, participantAddress: 'f2abc' }
      ]

      await updateDailyStationStats(pgClient, honestMeasurements, honestMeasurements, yesterday)
      await pgClient.query('COMMIT')

      await updateTopMeasurementParticipants(createPgClient)
      const { rows } = await pgClient.query('SELECT * FROM top_measurement_participants_yesterday_mv')

      assert.strictEqual(rows.length, 2)
      assert.deepStrictEqual(rows, [
        {
          participant_address: 'f1abc',
          inet_group_count: '1',
          station_count: '2',
          accepted_measurement_count: '3'
        },
        {
          participant_address: 'f2abc',
          inet_group_count: '1',
          station_count: '1',
          accepted_measurement_count: '1'
        }
      ])
    })
  })

  describe('daily_participants', () => {
    it('submits daily_participants data for today', async () => {
      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, participantAddress: '0x10' },
        { ...VALID_MEASUREMENT, participantAddress: '0x10' },
        { ...VALID_MEASUREMENT, participantAddress: '0x20' }
      ]
      await updatePlatformStats(pgClient, honestMeasurements, honestMeasurements)

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

  const getCurrentDate = async () => {
    const { rows: [{ today }] } = await pgClient.query('SELECT now()::DATE::TEXT as today')
    return today
  }

  const getYesterdayDate = async () => {
    const { rows: [{ yesterday }] } = await pgClient.query('SELECT now()::DATE - 1 as yesterday')
    return yesterday
  }
})
