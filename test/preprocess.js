import { preprocess } from '../lib/preprocess.js'
import assert from 'node:assert'

describe('preprocess', () => {
  it('fetches measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      wallet_address: '0xB0a808b5C49f5Ed7Af9EcAAaF033B2d937692877'
    }]
    const getCalls = []
    const web3Storage = {
      async get (_cid) {
        getCalls.push(_cid)
        return {
          async files () {
            return [{
              async text () {
                return JSON.stringify(measurements)
              }
            }]
          }
        }
      }
    }
    const logger = { log () {}, error: console.error }
    await preprocess({ rounds, cid, roundIndex, web3Storage, logger })

    assert.deepStrictEqual(rounds, {
      0: measurements.map(
        // Rename "wallet_address" to "peerId"
        // eslint-disable-next-line camelcase
        ({ wallet_address, ...m }) => ({ ...m, peerId: wallet_address })
      )
    })
    assert.deepStrictEqual(getCalls, [cid])
  })
  it('validates measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      wallet_address: '0xinvalid'
    }]
    const web3Storage = {
      async get () {
        return {
          async files () {
            return [{
              async text () {
                return JSON.stringify(measurements)
              }
            }]
          }
        }
      }
    }
    const logger = { log () {}, error () {} }
    await preprocess({ rounds, cid, roundIndex, web3Storage, logger })
    assert.deepStrictEqual(rounds, {
      0: []
    })
  })
})
