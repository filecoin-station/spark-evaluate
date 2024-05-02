import createDebug from 'debug'

import { updatePlatformStats } from './platform-stats.js'
import { getTaskId } from './retrieval-stats.js'

const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings').CreatePgClient} args.createPgClient
 * @param {import('./preprocess').Measurement[]} args.honestMeasurements
 */
export const updatePublicStats = async ({ createPgClient, honestMeasurements }) => {
  /** @type {Map<string, {{total: number, successful: number}}>} */
  const minerRetrievalStats = new Map()
  for (const m of honestMeasurements) {
    const minerId = m.minerId
    const retrievalStats = minerRetrievalStats.get(minerId) ?? { total: 0, successful: 0 }
    retrievalStats.total++
    if (m.retrievalResult === 'OK') retrievalStats.successful++
    minerRetrievalStats.set(minerId, retrievalStats)
  }

  const pgClient = await createPgClient()
  try {
    for (const [minerId, retrievalStats] of minerRetrievalStats.entries()) {
      await updateRetrievalStats(pgClient, minerId, retrievalStats)
    }
    await updateIndexerQueryStats(pgClient, honestMeasurements)
    await updatePlatformStats(pgClient, honestMeasurements)
  } finally {
    await pgClient.end()
  }
}

/**
 * @param {import('pg').Client} pgClient
 * @param {string} minerId
 * @param {object} stats
 * @param {number} stats.total
 * @param {number} stats.successful
 */
const updateRetrievalStats = async (pgClient, minerId, { total, successful }) => {
  debug('Updating public retrieval stats for miner %s: total += %s successful += %s', minerId, total, successful)
  await pgClient.query(`
    INSERT INTO retrieval_stats
      (day, miner_id, total, successful)
    VALUES
      (now(), $1, $2, $3)
    ON CONFLICT(day, miner_id) DO UPDATE SET
      total = retrieval_stats.total + $2,
      successful = retrieval_stats.successful + $3
  `, [
    minerId,
    total,
    successful
  ])
}

const updateIndexerQueryStats = async (pgClient, honestMeasurements) => {
  /** @type {Set<string>} */
  const dealsWithHttpAdvertisement = new Set()
  /** @type {Set<string>} */
  const dealsWithIndexerResults = new Set()

  for (const m of honestMeasurements) {
    const dealId = getTaskId(m)
    if (m.indexerResult) dealsWithIndexerResults.add(dealId)
    if (m.indexerResult === 'OK') dealsWithHttpAdvertisement.add(dealId)
  }

  const tested = dealsWithIndexerResults.size
  const advertisingHttp = dealsWithHttpAdvertisement.size
  debug('Updating public stats - indexer queries: deals_tested += %s deals_advertising_http += %s', tested, advertisingHttp)
  await pgClient.query(`
    INSERT INTO indexer_query_stats
      (day, deals_tested, deals_advertising_http)
    VALUES
      (now(), $1, $2)
    ON CONFLICT(day) DO UPDATE SET
    deals_tested = indexer_query_stats.deals_tested + $1,
    deals_advertising_http = indexer_query_stats.deals_advertising_http + $2
  `, [
    tested,
    advertisingHttp
  ])
}
