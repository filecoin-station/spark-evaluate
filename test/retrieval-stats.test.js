import assert from 'node:assert'
import createDebug from 'debug'
import { Point } from '../lib/telemetry.js'
import {
  buildRetrievalStats,
  getValueAtPercentile,
  recordCommitteeSizes
} from '../lib/retrieval-stats.js'
import { VALID_MEASUREMENT } from './helpers/test-data.js'
import { assertPointFieldValue } from './helpers/assertions.js'

const debug = createDebug('test')

describe('retrieval statistics', () => {
  it('reports all stats', async () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT
      },
      {
        ...VALID_MEASUREMENT,
        timeout: true,
        retrievalResult: 'TIMEOUT',

        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        first_byte_at: new Date('2023-11-01T09:00:10.000Z').getTime(),
        end_at: new Date('2023-11-01T09:00:50.000Z').getTime(),
        finished_at: new Date('2023-11-01T09:00:30.000Z').getTime(),
        byte_length: 2048
      },
      {
        ...VALID_MEASUREMENT,
        carTooLarge: true,
        retrievalResult: 'CAR_TOO_LARGE',
        byte_length: 200 * 1024 * 1024
      },
      {
        ...VALID_MEASUREMENT,
        status_code: 500,
        retrievalResult: 'ERROR_500',
        participantAddress: '0xcheater',
        inet_group: 'abcd',
        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        first_byte_at: new Date('2023-11-01T09:10:10.000Z').getTime(),
        end_at: new Date('2023-11-01T09:00:20.000Z').getTime(),
        finished_at: new Date('2023-11-01T09:00:30.000Z').getTime(),
        byte_length: 2048,

        // invalid task
        cid: 'bafyinvalid',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      }
    ]

    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'measurements', '4i')
    assertPointFieldValue(point, 'unique_tasks', '2i')
    assertPointFieldValue(point, 'success_rate', '0.25')
    assertPointFieldValue(point, 'participants', '2i')
    assertPointFieldValue(point, 'inet_groups', '2i')
    assertPointFieldValue(point, 'download_bandwidth', '209718272i')

    assertPointFieldValue(point, 'result_rate_OK', '0.25')
    assertPointFieldValue(point, 'result_rate_TIMEOUT', '0.25')
    assertPointFieldValue(point, 'result_rate_CAR_TOO_LARGE', '0.25')
    assertPointFieldValue(point, 'result_rate_ERROR_500', '0.25')

    assertPointFieldValue(point, 'ttfb_min', '1000i')
    assertPointFieldValue(point, 'ttfb_mean', '4000i')
    assertPointFieldValue(point, 'ttfb_p90', '8199i')
    assertPointFieldValue(point, 'ttfb_max', '10000i')

    assertPointFieldValue(point, 'duration_p10', '2000i')
    assertPointFieldValue(point, 'duration_mean', '18500i')
    assertPointFieldValue(point, 'duration_p90', '41000i')

    assertPointFieldValue(point, 'car_size_p10', '1228i')
    assertPointFieldValue(point, 'car_size_mean', '69906090i')
    assertPointFieldValue(point, 'car_size_p90', '167772569i')
    assertPointFieldValue(point, 'car_size_max', '209715200i')

    assertPointFieldValue(point, 'tasks_per_node_p5', '1i')
    assertPointFieldValue(point, 'tasks_per_node_p50', '2i')
    assertPointFieldValue(point, 'tasks_per_node_p95', '2i')
  })

  it('handles first_byte_at set to unix epoch', () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        first_byte_at: new Date('1970-01-01T00:00:00.000Z').getTime()
      }
    ]
    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)
    assertPointFieldValue(point, 'ttfb_min', undefined)
    assertPointFieldValue(point, 'ttfb_mean', undefined)
    assertPointFieldValue(point, 'ttfb_p90', undefined)
  })

  it('handles end_at set to unix epoch', () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
        end_at: new Date('1970-01-01T00:00:00.000Z').getTime()
      }
    ]
    const point = new Point('stats')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)
    assertPointFieldValue(point, 'duration_p10', undefined)
    assertPointFieldValue(point, 'duration_mean', undefined)
    assertPointFieldValue(point, 'duration_p90', undefined)
  })
})

describe('getValueAtPercentile', () => {
  it('interpolates the values', () => {
    assert.strictEqual(
      getValueAtPercentile([10, 20, 30], 90),
      28
    )
  })
})

describe('recordCommitteeSizes', () => {
  it('reports unique subnets', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig1'
      },
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xanother',
        // duplicate measurement in the same subnet, should be ignored
        inet_group: 'ig1'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig2'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig3'
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }

    ]

    const point = new Point('committees')
    recordCommitteeSizes(measurements, point)
    debug(point.name, point.fields)

    assertPointFieldValue(point, 'subnets_min', '1i')
    assertPointFieldValue(point, 'subnets_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'subnets_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'subnets_max', '3i')
  })

  it('reports unique participants', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig1',
        // duplicate measurement by the same participant, should be ignored
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xtwo'
      },
      {
        ...VALID_MEASUREMENT,
        participantAddress: '0xthree'
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }

    ]

    const point = new Point('committees')
    recordCommitteeSizes(measurements, point)
    debug(point.name, point.fields)

    assertPointFieldValue(point, 'participants_min', '1i')
    assertPointFieldValue(point, 'participants_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'participants_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'participants_max', '3i')
  })

  it('reports unique nodes', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT,
        inet_group: 'ig1',
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        // duplicate measurement by the same participant in the same subnet, should be ignored
        inet_group: 'ig1',
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT,
        // same participant address but different subnet
        inet_group: 'ig2',
        participantAddress: '0xone'
      },
      {
        ...VALID_MEASUREMENT
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }

    ]

    const point = new Point('committees')
    recordCommitteeSizes(measurements, point)
    debug(point.name, point.fields)

    assertPointFieldValue(point, 'nodes_min', '1i')
    assertPointFieldValue(point, 'nodes_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'nodes_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'nodes_max', '3i')
  })

  it('reports number of all measurements', async () => {
    const measurements = [
      // task 1
      {
        ...VALID_MEASUREMENT
      },
      {
        ...VALID_MEASUREMENT

      },
      {
        ...VALID_MEASUREMENT
      },

      // task 2
      {
        ...VALID_MEASUREMENT,
        cid: 'bafyanother'
      }

    ]

    const point = new Point('committees')
    recordCommitteeSizes(measurements, point)
    debug(point.name, point.fields)

    assertPointFieldValue(point, 'measurements_min', '1i')
    assertPointFieldValue(point, 'measurements_mean', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'measurements_p50', '2i') // (3+1)/2 rounded down
    assertPointFieldValue(point, 'measurements_max', '3i')
  })
})
