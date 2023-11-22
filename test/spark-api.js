import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async () => {
    const { retrievalTasks, ...details } = await fetchRoundDetails(
      '0xaaef78eaf86dcf34f275288752e892424dda9341',
      410,
      recordTelemetry
    )

    assert.deepStrictEqual(details, {
      roundId: '3408' // BigInt serialized as String
    })

    assert.strictEqual(retrievalTasks.length, 80)
    assert.deepStrictEqual(retrievalTasks.slice(0, 2), [
      {
        cid: 'QmQmAT34YQZUXWPonEBLYayGPqbfKT7WNVKyejYb8BfbJ8',
        protocol: 'graphsync',
        providerAddress: '/ip4/115.42.169.232/tcp/34568/p2p/12D3KooWH6ZS7VtsG8FWhxopQVSkvjMSbKtdiDgdfx7DR2oKopzV'
      },
      {
        cid: 'bafybeift5kk74nwk6mezbicri23a5yvybdu63ege3c3jexxozfvhdd37s4',
        protocol: 'graphsync',
        providerAddress: '/ip4/103.145.73.12/tcp/41727/p2p/12D3KooWNo9msgn3T1uhmgxCjg5YKghMEHdNGr8uU4EtFC3a9zev'
      }
    ])
  })
})
