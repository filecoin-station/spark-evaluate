import { parseParticipantAddress, preprocess } from '../lib/preprocess.js'
import assert from 'node:assert'

describe('preprocess', () => {
  it('fetches measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 'f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i'
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
      0: [{
        participantAddress: '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E'
      }]
    })
    assert.deepStrictEqual(getCalls, [cid])
  })
  it('validates measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      wallet_address: 't1foobar'
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
    const logger = { log () { }, error () { } }
    await preprocess({ rounds, cid, roundIndex, web3Storage, logger })
    assert.deepStrictEqual(rounds, {
      0: []
    })
  })

  it('converts mainnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts testnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('t410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })
})
