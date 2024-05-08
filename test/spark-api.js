import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async function () {
    this.timeout(10_000)
    const { retrievalTasks, maxTasksPerNode, ...details } = await fetchRoundDetails(
      '0xaaef78eaf86dcf34f275288752e892424dda9341',
      407,
      recordTelemetry
    )

    assert.deepStrictEqual(details, {
      roundId: '3405', // BigInt serialized as String,
      startEpoch: '0'
    })

    assert.strictEqual(typeof maxTasksPerNode, 'number')

    assert.strictEqual(retrievalTasks.length, 400)
    assert.deepStrictEqual(retrievalTasks.slice(0, 2), [
      {
        cid: 'Qmcieq8Lf1r4qfifJkKpdpVzajdsbSGZmBiL3vy6syWo5T',
        minerId: null,
        protocol: 'graphsync',
        providerAddress: '/ip4/210.209.69.37/tcp/34568/p2p/12D3KooWSHG9vVStHMi9vhfgD1XaR221Ur6RjWcVsiY7WfVaX4QL'
      },
      {
        cid: 'QmejPgwMo5jzJDsArpGCn9Tz1gSQNXYU722pFe2kcLHzpq',
        minerId: null,
        protocol: 'graphsync',
        providerAddress: '/ip4/138.113.222.200/tcp/19013/p2p/12D3KooWCnJeMnur6ScjXXUo8ptMKac88fDbD8GELmdDrdg3FBk5'
      }
    ])
  })
})
