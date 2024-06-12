import createDebug from 'debug'

import { updatePlatformStats } from './platform-stats.js'
import { getTaskId } from './retrieval-stats.js'

const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings.js').CreatePgClient} args.createPgClient
 * @param {import('./preprocess.js').Measurement[]} args.honestMeasurements
 * @param {import('./preprocess.js').Measurement[]} args.allMeasurements
 */
export const updatePublicStats = async ({ createPgClient, honestMeasurements, allMeasurements }) => {
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
    await updateIndexerQueryStats(pgClient, honestMeasurements)
    await updateDailyDealsStats(pgClient, honestMeasurements)
    await updatePlatformStats(pgClient, honestMeasurements, allMeasurements)
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

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess.js').Measurement[]} acceptedMeasurements
 */
const updateDailyDealsStats = async (pgClient, acceptedMeasurements) => {
  /** @type {Set<string>} */
  const dealsAll = new Set()
  /** @type {Set<string>} */
  const dealsIndexed = new Set()
  /** @type {Set<string>} */
  const dealsRetrievable = new Set()

  for (const m of acceptedMeasurements) {
    const dealId = getTaskId(m)
    dealsAll.add(dealId)
    // TODO: Use the majority to decide whether a deal is indexed and retrievable.
    // At the moment, we assume a deal is indexed/retrievable if at least one measurement
    // indicates that. Once we implement "honest majority", minority results will be rejected,
    // we won't receive them in the `acceptedMeasurements` array and the logic below will keep
    // working unchanged.
    if (m.indexerResult === 'OK' || m.indexerResult === 'HTTP_NOT_ADVERTISED') dealsIndexed.add(dealId)
    if (m.retrievalResult === 'OK') dealsRetrievable.add(dealId)
  }

  const total = dealsAll.size
  const indexed = dealsIndexed.size
  const retrievable = dealsRetrievable.size
  debug('Updating public stats - daily deals: total += %s indexed += %s retrievable += %s', total, indexed, retrievable)
  await pgClient.query(`
    INSERT INTO daily_deals
      (day, total, indexed, retrievable)
    VALUES
      (now(), $1, $2, $3)
    ON CONFLICT(day) DO UPDATE SET
      total = indexer_query_stats.total + $1,
      indexed = indexer_query_stats.indexed + $2,
      retrievable = indexer_query_stats.retrievable + $3
  `, [
    total,
    indexed,
    retrievable
  ])
}
