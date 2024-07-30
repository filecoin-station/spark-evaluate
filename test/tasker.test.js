import assert from 'node:assert'
import { getStationKey, getTaskKey, getTasksAllowedForStations } from '../lib/tasker.js'
import { VALID_MEASUREMENT } from './helpers/test-data.js'

/** @import {Measurement} from '../lib/preprocess.js' */

describe('tasker test vectors shared with spark checker impl', () => {
  // test vectors from Spark checker nodes
  // https://github.com/filecoin-station/spark/blob/main/test/tasker.test.js

  const ZINNIA_DEV_STATION_ID = '0'.repeat(88)
  const RANDOMNESS = 'fc90e50dcdf20886b56c038b30fa921a5e57c532ea448dadcc209e44eec0445e'

  it('correctly computes the key for a task', async () => {
    const key = await getTaskKey(
      { cid: 'bafyone', minerId: 'f0123' },
      RANDOMNESS
    )
    assert.strictEqual(key, 19408172415633384483144889917969030396168570904487614072975030553911283422991n)
  })

  it('correctly computes the key for a station id', async () => {
    const key = await getStationKey(ZINNIA_DEV_STATION_ID)
    assert.strictEqual(key, 15730389902218173522122968096857080019341969656147255283496861606681823756880n)
  })

  it('correctly picks tasks allowed for the given station id', async () => {
    const retrievalTasks = [
      { cid: 'bafyone', minerId: 'f010' },
      { cid: 'bafyone', minerId: 'f020' },
      { cid: 'bafyone', minerId: 'f030' },
      { cid: 'bafyone', minerId: 'f040' },

      { cid: 'bafytwo', minerId: 'f010' },
      { cid: 'bafytwo', minerId: 'f020' },
      { cid: 'bafytwo', minerId: 'f030' },
      { cid: 'bafytwo', minerId: 'f040' }
    ]

    /** @type {Measurement[]} */
    const measurements = [
      { ...VALID_MEASUREMENT, stationId: 'some-station-id' }
    ]

    const tasksAllowedForStations = await getTasksAllowedForStations(
      { retrievalTasks, maxTasksPerNode: 3 },
      RANDOMNESS,
      measurements
    )

    const allowedTasks = tasksAllowedForStations.get('some-station-id')

    assert.deepStrictEqual(allowedTasks, [
      { cid: 'bafyone', minerId: 'f020' },
      { cid: 'bafyone', minerId: 'f010' },
      { cid: 'bafytwo', minerId: 'f020' }
    ])
  })
})

describe('tasksAllowedForStations', async () => {
  const RANDOMNESS = 'fc90e50dcdf20886b56c038b30fa921a5e57c532ea448dadcc209e44eec0445e'

  it('handles maxTasksPerNode larger than the number of round tasks', async () => {
    const retrievalTasks = [
      { cid: 'bafyone', minerId: 'f010' },
      { cid: 'bafyone', minerId: 'f020' }
    ]

    /** @type {Measurement[]} */
    const measurements = [VALID_MEASUREMENT]

    const tasksAllowedForStations = await getTasksAllowedForStations(
      { retrievalTasks, maxTasksPerNode: 15 },
      RANDOMNESS,
      measurements
    )

    assert.strictEqual(tasksAllowedForStations.get(VALID_MEASUREMENT.stationId)?.length, 2)
  })
})
