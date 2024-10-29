import assert from 'node:assert/strict'
import * as providerRetrievalResultStats from '../lib/provider-retrieval-result-stats.js'
import { CID } from 'multiformats'

describe('Provider Retrieval Result Stats', () => {
  describe('build()', () => {
    it('should build provider retrieval result stats', () => {
      // TODO: Add more committee edge cases
      const stats = providerRetrievalResultStats.build([
        {
          measurements: [
            {
              minerId: '0',
              retrievalResult: 'OK'
            },
            {
              minerId: '1',
              retrievalResult: 'TIMEOUT'
            }
          ]
        }, {
          measurements: [
            {
              minerId: '0',
              retrievalResult: 'OK'
            },
            {
              minerId: '1',
              retrievalResult: 'TIMEOUT'
            }
          ]
        }
      ])
      assert.deepStrictEqual(stats, new Map([
        ['0', { total: 2, successful: 2 }],
        ['1', { total: 2, successful: 0 }]
      ]))
    })
  })
  describe('publishRoundDetails()', () => {
    it('should publish round details', async () => {
      const uploadCARCalls = []
      const storachaClient = {
        uploadCAR: async car => {
          uploadCARCalls.push(car)
        }
      }
      const cid = await providerRetrievalResultStats.publishRoundDetails({
        storachaClient,
        round: {
          details: {
            beep: 'boop'
          }
        }
      })
      assert(cid instanceof CID)
      assert.strictEqual(uploadCARCalls.length, 1)
      // TODO: Assert the CAR content
    })
  })
  describe('prepare()', () => {
    it('should publish round details', async () => {
      const uploadCARCalls = []
      const storachaClient = {
        uploadCAR: async car => {
          uploadCARCalls.push(car)
        }
      }
      const createPgClient = async () => {
        return {
          query: async () => {},
          end: async () => {}
        }
      }
      await providerRetrievalResultStats.prepare({
        storachaClient,
        round: {
          details: {
            beep: 'boop'
          }
        },
        createPgClient,
        committees: [],
        sparkEvaluateVersion: 'v0',
        ieContractAddress: '0x0'
      })
      assert.strictEqual(uploadCARCalls.length, 1)
    })
  })
})
