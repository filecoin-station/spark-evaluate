import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async () => {
    const { retrievalTasks, ...details } = await fetchRoundDetails(
      '0x381b50e8062757c404dec2bfae4da1b495341af9',
      10,
      recordTelemetry
    )

    assert.deepStrictEqual(details, {
      roundId: '995' // BigInt serialized as String
    })

    assert.strictEqual(retrievalTasks.length, 30)
    assert.deepStrictEqual(retrievalTasks.slice(0, 2), [
      {
        cid: 'QmUen1QdStwcqYaRJKW59GfEk1y8TyqyrvisZ5Mm2ZwMhU',
        protocol: 'bitswap',
        providerAddress: '/dns4/elastic.dag.house/tcp/443/wss/p2p/QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC'
      },
      {
        cid: 'bafybeidijvhvuvizg2ofeqos7l2kd4uanbawzx6qazyko5kbvth6dlhn5e',
        protocol: 'bitswap',
        providerAddress: '/dns4/elastic.dag.house/tcp/443/wss/p2p/QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC'
      }
    ])
  })
})
