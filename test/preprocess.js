import { preprocess } from '../lib/preprocess.js'
import assert from 'node:assert'

describe('preprocess', () => {
  it('fetches measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      peerId: '0xB0a808b5C49f5Ed7Af9EcAAaF033B2d937692877'
    }]
    const web3Storage = {
      async get (_cid) {
        assert.strictEqual(_cid, cid)
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
    const logger = { log () {} }
    await preprocess({ rounds, cid, roundIndex, web3Storage, logger })
    assert.deepStrictEqual(rounds, {
      0: measurements
    })
  })
  it('validates measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      peerId: '0xinvalid'
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
