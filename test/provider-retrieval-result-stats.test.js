import assert from 'node:assert/strict'
import * as providerRetrievalResultStats from '../lib/provider-retrieval-result-stats.js'
import { CID } from 'multiformats'
import pg from 'pg'
import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgClient } from '../lib/migrate.js'

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

const MEASUREMENT_BATCH = 'bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4'
const ROUND_DETAILS = 'bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4'

describe('Provider Retrieval Result Stats', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  beforeEach(async () => {
    await pgClient.query('TRUNCATE unpublished_provider_retrieval_result_stats_rounds')
  })

  after(async () => {
    await pgClient.end()
  })

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
    it('should insert into the database', async () => {
      const storachaClient = {
        uploadCAR: async () => {}
      }
      const round = {
        index: 0,
        measurementBatches: ['0x0'],
        details: {
          beep: 'boop'
        }
      }
      const ieContractAddress = '0x'
      const sparkEvaluateVersion = 'v0'
      await providerRetrievalResultStats.prepare({
        storachaClient,
        round,
        createPgClient,
        committees: [
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
        ],
        sparkEvaluateVersion: 'v0',
        ieContractAddress
      })
      const { rows } = await pgClient.query('SELECT * FROM unpublished_provider_retrieval_result_stats_rounds')
      assert.strictEqual(rows.length, 1)
      assert(rows[0].evaluated_at instanceof Date)
      delete rows[0].evaluated_at
      assert.deepStrictEqual(rows, [{
        contract_address: ieContractAddress,
        measurement_batches: round.measurementBatches,
        provider_retrieval_result_stats: {
          0: { successful: 2, total: 2 },
          1: { successful: 0, total: 2 }
        },
        round_details: 'baguqeerawg5jfpiy2g5xp5d422uwa3mpyzkmiguoeecesds7q65mn2hdoa4q',
        round_index: String(round.index),
        spark_evaluate_version: sparkEvaluateVersion
      }])
    })
  })
  describe('publish()', () => {
    it('should upload stats to Storacha', async () => {
      const round = {
        index: 0,
        measurementBatches: [MEASUREMENT_BATCH]
      }
      const ieContractAddress = '0x'
      const sparkEvaluateVersion = 'v0'
      const roundDetailsCid = ROUND_DETAILS
      const yesterday = new Date()
      yesterday.setDate(yesterday.getDate() - 1)
      await pgClient.query(`
        INSERT INTO unpublished_provider_retrieval_result_stats_rounds
        (evaluated_at, round_index, contract_address, spark_evaluate_version, measurement_batches, round_details, provider_retrieval_result_stats)
        VALUES
        ($1, $2, $3, $4, $5, $6, $7)
      `, [
        yesterday,
        round.index,
        ieContractAddress,
        sparkEvaluateVersion,
        round.measurementBatches,
        roundDetailsCid,
        {
          0: { successful: 2, total: 2 },
          1: { successful: 0, total: 2 }
        }
      ])
      const uploadCARCalls = []
      await providerRetrievalResultStats.publish({
        createPgClient,
        storachaClient: {
          uploadCAR: async car => {
            uploadCARCalls.push(car)
          }
        },
        rsrContract: {
          addProviderRetrievalResultStats: async () => {
            return {
              wait: async () => {}
            }
          }
        }
      })
      assert.strictEqual(uploadCARCalls.length, 1)
      // TODO: Assert the CAR content
    })
    it('should add stats to the RSR contract', async () => {
      const round = {
        index: 0,
        measurementBatches: [MEASUREMENT_BATCH]
      }
      const ieContractAddress = '0x'
      const sparkEvaluateVersion = 'v0'
      const roundDetailsCid = ROUND_DETAILS
      const yesterday = new Date()
      yesterday.setDate(yesterday.getDate() - 1)
      await pgClient.query(`
        INSERT INTO unpublished_provider_retrieval_result_stats_rounds
        (evaluated_at, round_index, contract_address, spark_evaluate_version, measurement_batches, round_details, provider_retrieval_result_stats)
        VALUES
        ($1, $2, $3, $4, $5, $6, $7)
      `, [
        yesterday,
        round.index,
        ieContractAddress,
        sparkEvaluateVersion,
        round.measurementBatches,
        roundDetailsCid,
        {
          0: { successful: 2, total: 2 },
          1: { successful: 0, total: 2 }
        }
      ])
      const addProviderRetrievalResultStatsCalls = []
      await providerRetrievalResultStats.publish({
        createPgClient,
        storachaClient: {
          uploadCAR: async () => {}
        },
        rsrContract: {
          addProviderRetrievalResultStats: async cid => {
            addProviderRetrievalResultStatsCalls.push(cid)
            return {
              wait: async () => {}
            }
          }
        }
      })
      assert.deepStrictEqual(
        addProviderRetrievalResultStatsCalls,
        ['baguqeeramqwzxhhqzofl5e56ugixuthw3dyxbvzg2e4efh4guyi55pyvy5sa']
      )
    })
    it('should delete published stats from the database')
    it('should choose the all rounds with the oldest evaluated_at date')
    it('should ignore data from today')
    it('should noop when there is nothing in the database')
  })
})