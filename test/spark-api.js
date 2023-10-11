import assert from 'node:assert'
import { fetchRoundDetails } from '../lib/spark-api.js'

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

describe('spark-api client', () => {
  it('fetches round details', async () => {
    const { retrievalTasks, ...details } = await fetchRoundDetails(520, recordTelemetry)

    assert.deepStrictEqual(details, {
      roundId: '520' // BigInt serialized as String
    })

    assert.strictEqual(retrievalTasks.length, 30)
    assert.deepStrictEqual(retrievalTasks.slice(0, 2), [
      {
        cid: 'bafkreibngqhl3gaa7daob4i2vccziay2jjlp435cf66vhono7nrvww53ty',
        protocol: 'graphsync',
        providerAddress: '/ip4/89.20.96.58/tcp/24001/p2p/12D3KooWDMJSprsuxhjJVnuQQcyibc5GxanUUxpDzHU74rhknqkU'
      },
      {
        cid: 'bafybeib4bxcp5y6wjz24ckzhxv2nypibvqcldim37pmy2ayrny5rrqboou',
        protocol: 'bitswap',
        providerAddress: '/dns4/elastic.dag.house/tcp/443/wss/p2p/QmQzqxhK82kAmKvARFZSkUVS6fo9sySaiogAnx5EnZ6ZmC'
      }
    ])
  })
})
