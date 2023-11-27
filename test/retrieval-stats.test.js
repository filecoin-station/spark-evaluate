import createDebug from 'debug'
import { Point } from '../lib/telemetry.js'
import { buildRetrievalStats } from '../lib/retrieval-stats.js'
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
        status_code: 500,
        retrievalResult: 'ERROR_500',
        participantAddress: '0xcheater',
        inet_group: 'abcd',
        start_at: '2023-11-01T09:00:00.000Z',
        first_byte_at: '2023-11-01T09:00:10.000Z',
        end_at: '2023-11-01T09:00:20.000Z',
        finished_at: '2023-11-01T09:00:30.000Z',
        byte_length: 2048,

        // invalid task
        cid: 'bafyinvalid',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      }
    ]

    const point = new Point('retrieval_stats_all')
    buildRetrievalStats(measurements, point)
    debug('stats', point.fields)

    assertPointFieldValue(point, 'measurements', '2i')
    assertPointFieldValue(point, 'unique_tasks', '2i')
    assertPointFieldValue(point, 'success_rate', '0.5')
    assertPointFieldValue(point, 'participants', '2i')
    assertPointFieldValue(point, 'inet_groups', '2i')
    assertPointFieldValue(point, 'measurements', '2i')
    assertPointFieldValue(point, 'download_bandwidth', '3072i')

    assertPointFieldValue(point, 'result_rate_OK', '0.5')
    assertPointFieldValue(point, 'result_rate_TIMEOUT', '0')
    assertPointFieldValue(point, 'result_rate_ERROR_500', '0.5')

    assertPointFieldValue(point, 'ttfb_min', '1000i')
    assertPointFieldValue(point, 'ttfb_mean', '5500i')
    assertPointFieldValue(point, 'ttfb_p90', '10000i')

    assertPointFieldValue(point, 'duration_p10', '2000i')
    assertPointFieldValue(point, 'duration_mean', '11000i')
    assertPointFieldValue(point, 'duration_p90', '20000i')

    assertPointFieldValue(point, 'car_size_p10', '1024i')
    assertPointFieldValue(point, 'car_size_mean', '1536i')
    assertPointFieldValue(point, 'car_size_p90', '2048i')
  })
})
