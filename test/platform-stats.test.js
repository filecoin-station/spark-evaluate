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
  updateStationsAndParticipants,
  updatePlatformStats,
  aggregateAndCleanupRecentData,
  updateMonthlyActiveStationCount,
  refreshDatabase,
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
    await pgClient.query('DELETE FROM recent_station_details')
    await pgClient.query('DELETE FROM daily_participants')
    await pgClient.query('DELETE FROM recent_participant_subnets')
    await pgClient.query('DELETE FROM recent_active_stations')
    await pgClient.query('DELETE FROM daily_measurements_summary')
    await pgClient.query('DELETE FROM monthly_active_station_count')

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

      await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements, { day: today })

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
        ORDER BY station_id, participant_address, inet_group`
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
          inet_group: 'other-group',
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

      await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements, { day: today })

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

      await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements, { day: today })

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

      await updateDailyStationStats(pgClient, honestMeasurements, honestMeasurements, { day: yesterday })
      await pgClient.query('COMMIT')

      await updateTopMeasurementParticipants(pgClient)
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

  describe('refreshDatabase', () => {
    it('runs provided functions and handles errors', async () => {
      const executedFunctions = []
      const errorMessages = []

      const successFunction = async () => {
        executedFunctions.push('successFunction')
      }

      const errorFunction = async () => {
        executedFunctions.push('errorFunction')
        throw new Error('Test error')
      }

      const originalConsoleError = console.error
      console.error = (message, error) => {
        errorMessages.push({ message, error })
      }

      await refreshDatabase(createPgClient, {
        functionsToRun: [successFunction, errorFunction]
      })

      console.error = originalConsoleError

      assert.deepStrictEqual(executedFunctions, ['successFunction', 'errorFunction'])
      assert.strictEqual(errorMessages.length, 1)
      assert.strictEqual(errorMessages[0].message, 'Error running function errorFunction:')
      assert.strictEqual(errorMessages[0].error.message, 'Test error')
    })
  })

  describe('updateStationsAndParticipants', () => {
    it('updates recent_station_details, recent_active_stations, and recent_participant_subnets', async () => {
      const participantsMap = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x20']))

      /** @type {Measurement[]} */
      const honestMeasurements = [
        { ...VALID_MEASUREMENT, stationId: 'station1', participantAddress: '0x10', inet_group: 'subnet1' },
        { ...VALID_MEASUREMENT, stationId: 'station1', participantAddress: '0x10', inet_group: 'subnet2' },
        { ...VALID_MEASUREMENT, stationId: 'station2', participantAddress: '0x20', inet_group: 'subnet3' }
      ]

      /** @type {Measurement[]} */
      const allMeasurements = [
        ...honestMeasurements,
        { ...VALID_MEASUREMENT, stationId: 'station1', participantAddress: '0x10', inet_group: 'subnet1', fraudAssessment: 'TASK_NOT_IN_ROUND' }
      ]

      await updateStationsAndParticipants(pgClient, honestMeasurements, allMeasurements, participantsMap, today)

      const { rows: stationDetails } = await pgClient.query(`
        SELECT
          day::TEXT,
          station_id,
          participant_id,
          accepted_measurement_count,
          total_measurement_count
        FROM recent_station_details
        WHERE day = $1::DATE
        ORDER BY station_id, participant_id
      `, [today])

      assert.strictEqual(stationDetails.length, 2)
      assert.deepStrictEqual(stationDetails, [
        {
          day: today,
          station_id: 'station1',
          participant_id: 1,
          accepted_measurement_count: 2,
          total_measurement_count: 3
        },
        {
          day: today,
          station_id: 'station2',
          participant_id: 2,
          accepted_measurement_count: 1,
          total_measurement_count: 1
        }
      ])

      const { rows: activeStations } = await pgClient.query(`
        SELECT day::TEXT, station_id
        FROM recent_active_stations
        WHERE day = $1::DATE
        ORDER BY station_id
      `, [today])

      assert.strictEqual(activeStations.length, 2)
      assert.deepStrictEqual(activeStations, [
        { day: today, station_id: 'station1' },
        { day: today, station_id: 'station2' }
      ])

      // Check recent_participant_subnets
      const { rows: participantSubnets } = await pgClient.query(`
        SELECT day::TEXT, participant_id, subnet
        FROM recent_participant_subnets
        WHERE day = $1::DATE
        ORDER BY participant_id, subnet
      `, [today])

      assert.strictEqual(participantSubnets.length, 3)
      assert.deepStrictEqual(participantSubnets, [
        { day: today, participant_id: 1, subnet: 'subnet1' },
        { day: today, participant_id: 1, subnet: 'subnet2' },
        { day: today, participant_id: 2, subnet: 'subnet3' }
      ])
    })
  })

  describe('aggregateAndCleanupRecentData', () => {
    const assertDailySummary = async () => {
      const { rows } = await pgClient.query("SELECT * FROM daily_measurements_summary WHERE day = CURRENT_DATE - INTERVAL '3 days'")
      assert.strictEqual(rows.length, 1)
      assert.deepStrictEqual(rows[0], {
        day: (await pgClient.query("SELECT (CURRENT_DATE - INTERVAL '3 days') as day")).rows[0].day,
        accepted_measurement_count: 15,
        total_measurement_count: 30,
        distinct_active_station_count: 2,
        distinct_participant_address_count: 2,
        distinct_inet_group_count: 2
      })

      const recentDetailsCount = await pgClient.query("SELECT COUNT(*) FROM recent_station_details WHERE day <= CURRENT_DATE - INTERVAL '2 days'")
      const recentSubnetsCount = await pgClient.query("SELECT COUNT(*) FROM recent_participant_subnets WHERE day <= CURRENT_DATE - INTERVAL '2 days'")
      assert.strictEqual(recentDetailsCount.rows[0].count, '0')
      assert.strictEqual(recentSubnetsCount.rows[0].count, '0')
    }

    it('aggregates and cleans up data older than two days', async () => {
      // need to map participant addresses to ids first
      const participantsMap = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x20']))
      await pgClient.query(`
        INSERT INTO recent_station_details (day, accepted_measurement_count, total_measurement_count, station_id, participant_id)
        VALUES
        (CURRENT_DATE - INTERVAL '3 days', 10, 20, 1, $1),
        (CURRENT_DATE - INTERVAL '3 days', 5, 10, 2, $2);
      `, [participantsMap.get('0x10'), participantsMap.get('0x20')])
      await pgClient.query(`
        INSERT INTO recent_participant_subnets (day, participant_id, subnet)
        VALUES
        (CURRENT_DATE - INTERVAL '3 days', $1, 'subnet1'),
        (CURRENT_DATE - INTERVAL '3 days', $2, 'subnet2');
      `, [participantsMap.get('0x10'), participantsMap.get('0x20')])

      await aggregateAndCleanupRecentData(pgClient)
      await assertDailySummary()
      await aggregateAndCleanupRecentData(pgClient) // Run again and check that nothing changes
      await assertDailySummary()
    })
  })

  describe('updateMonthlyActiveStationCount', () => {
    const assertCorrectMonthlyActiveStationCount = async () => {
      const { rows } = await pgClient.query(`
        SELECT * FROM monthly_active_station_count
        WHERE month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
      `)
      assert.strictEqual(rows.length, 1)
      assert.strictEqual(rows[0].station_count, 2)

      const recentStationsCount = await pgClient.query(`
        SELECT COUNT(*) FROM recent_active_stations
        WHERE day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND day < DATE_TRUNC('month', CURRENT_DATE)
      `)
      assert.strictEqual(recentStationsCount.rows[0].count, '0')
    }

    it('updates monthly active station count for the previous month', async () => {
      await pgClient.query(`
        INSERT INTO recent_active_stations (day, station_id)
        VALUES
        (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '1 day', 1),
        (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') + INTERVAL '2 days', 2);
      `)

      await updateMonthlyActiveStationCount(pgClient)
      await assertCorrectMonthlyActiveStationCount()
      await updateMonthlyActiveStationCount(pgClient) // Run again and check that nothing changes
      await assertCorrectMonthlyActiveStationCount()
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
      const participantsMap = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x20']))
      await updateDailyParticipants(pgClient, Array.from(participantsMap.values()))

      const { rows: created } = await pgClient.query(
        'SELECT day::TEXT, participant_id FROM daily_participants'
      )
      assert.deepStrictEqual(created, [
        { day: today, participant_id: 1 },
        { day: today, participant_id: 2 }
      ])
    })

    it('handles participants already seen today', async () => {
      const participantsMap1 = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x20']))
      await updateDailyParticipants(pgClient, Array.from(participantsMap1.values()))

      const participantsMap2 = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x30', '0x20']))
      await updateDailyParticipants(pgClient, Array.from(participantsMap2.values()))

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
      const participantsMap = await mapParticipantsToIds(pgClient, new Set(['0x10', '0x20']))
      assert.deepStrictEqual(participantsMap, new Map([['0x10', 1], ['0x20', 2]]))
    })

    it('maps existing participants to their existing ids', async () => {
      const participants = new Set(['0x10', '0x20'])
      const first = await mapParticipantsToIds(pgClient, participants)
      assert.deepStrictEqual(first, new Map([['0x10', 1], ['0x20', 2]]))

      participants.add('0x30')
      participants.add('0x40')
      const second = await mapParticipantsToIds(pgClient, participants)
      assert.deepStrictEqual(second, new Map([['0x10', 1], ['0x20', 2], ['0x30', 3], ['0x40', 4]]))
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
