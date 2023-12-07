import assert from 'node:assert'
import createDebug from 'debug'
import { Point } from '../lib/telemetry.js'
import { buildRetrievalStats, getValueAtPercentile } from '../lib/retrieval-stats.js'
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
        first_byte_at: new Date('2023-11-01T09:00:10.000Z').getTime(),
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

    assertPointFieldValue(point, 'duration_p10', '2000i')
    assertPointFieldValue(point, 'duration_mean', '18500i')
    assertPointFieldValue(point, 'duration_p90', '41000i')

    assertPointFieldValue(point, 'car_size_p10', '1228i')
    assertPointFieldValue(point, 'car_size_mean', '69906090i')
    assertPointFieldValue(point, 'car_size_p90', '167772569i')
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
