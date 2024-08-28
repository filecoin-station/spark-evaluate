import createDebug from 'debug'

import { updatePlatformStats } from './platform-stats.js'
import { getTaskId } from './retrieval-stats.js'

/** @import pg from 'pg' */
/** @import { Committee } from './committee.js' */

const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings.js').CreatePgClient} args.createPgClient
 * @param {Iterable<Committee>} args.committees
 * @param {import('./preprocess.js').Measurement[]} args.honestMeasurements
 * @param {import('./preprocess.js').Measurement[]} args.allMeasurements
 */
export const updatePublicStats = async ({ createPgClient, committees, honestMeasurements, allMeasurements }) => {
  /** @type {Map<string, {total: number, successful: number}>} */
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
    await updateIndexerQueryStats(pgClient, committees)
    await updateDailyDealsStats(pgClient, committees)
    await updatePlatformStats(pgClient, honestMeasurements, allMeasurements)
  } finally {
    await pgClient.end()
  }
}

/**
 * @param {pg.Client} pgClient
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

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 */
const updateIndexerQueryStats = async (pgClient, committees) => {
  /** @type {Set<string>} */
  const dealsWithHttpAdvertisement = new Set()
  /** @type {Set<string>} */
  const dealsWithIndexerResults = new Set()

  for (const c of committees) {
    const dealId = getTaskId(c.retrievalTask)
    const evaluation = c.evaluation
    if (!evaluation) continue
    if (evaluation.indexerResult) dealsWithIndexerResults.add(dealId)
    if (evaluation.indexerResult === 'OK') dealsWithHttpAdvertisement.add(dealId)
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

/**
 * @param {pg.Client} pgClient
 * @param {Iterable<Committee>} committees
 */
const updateDailyDealsStats = async (pgClient, committees) => {
  let total = 0
  let indexed = 0
  let retrievable = 0
  for (const c of committees) {
    total++

    const evaluation = c.evaluation
    if (!evaluation) continue
    if (evaluation.indexerResult === 'OK' || evaluation.indexerResult === 'HTTP_NOT_ADVERTISED') {
      indexed++
    }
    if (evaluation.retrievalResult === 'OK') {
      retrievable++
    }
  }

  debug('Updating public stats - daily deals: total += %s indexed += %s retrievable += %s', total, indexed, retrievable)
  await pgClient.query(`
    INSERT INTO daily_deals
      (day, total, indexed, retrievable)
    VALUES
      (now(), $1, $2, $3)
    ON CONFLICT(day) DO UPDATE SET
      total = daily_deals.total + $1,
      indexed = daily_deals.indexed + $2,
      retrievable = daily_deals.retrievable + $3
  `, [
    total,
    indexed,
    retrievable
  ])
}
