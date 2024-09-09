import * as Sentry from '@sentry/node'
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
 * @param {(minerId: string, cid: string) => (string[] | undefined)} args.findDealClients
 */
export const updatePublicStats = async ({ createPgClient, committees, honestMeasurements, allMeasurements, findDealClients }) => {
  /** @type {Map<string, {total: number, successful: number}>} */
  const minerRetrievalStats = new Map()
  for (const c of committees) {
    // IMPORTANT: include minority results in the calculation
    for (const m of c.measurements) {
      const minerId = m.minerId
      const retrievalStats = minerRetrievalStats.get(minerId) ?? { total: 0, successful: 0 }
      retrievalStats.total++
      if (m.retrievalResult === 'OK') retrievalStats.successful++
      minerRetrievalStats.set(minerId, retrievalStats)
    }
  }

  const pgClient = await createPgClient()
  try {
    for (const [minerId, retrievalStats] of minerRetrievalStats.entries()) {
      await updateRetrievalStats(pgClient, minerId, retrievalStats)
    }
    await updateIndexerQueryStats(pgClient, committees)
    await updateDailyDealsStats(pgClient, committees, findDealClients)
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
 * @param {(minerId: string, cid: string) => (string[] | undefined)} findDealClients
 */
const updateDailyDealsStats = async (pgClient, committees, findDealClients) => {
  /** @type {Map<string, Map<string, {
   * tested: number;
   * index_majority_found: number;
   * retrieval_majority_found: number;
   * indexed:  number;
   * indexed_http: number;
   * retrievable: number;
   * }>>} */
  const minerClientDealStats = new Map()
  for (const c of committees) {
    const { minerId, cid } = c.retrievalTask
    const clients = findDealClients(minerId, cid)
    if (!clients || !clients.length) {
      console.warn(`Invalid retrieval task (${minerId}, ${cid}): no deal clients found. Excluding the task from daily per-deal stats.`)
      Sentry.captureException(new Error('Invalid retrieval task: no deal clients found.'), {
        extra: {
          minerId,
          cid
        }
      })
      continue
    }

    let clientDealStats = minerClientDealStats.get(minerId)
    if (!clientDealStats) {
      clientDealStats = new Map()
      minerClientDealStats.set(minerId, clientDealStats)
    }

    for (const clientId of clients) {
      let stats = clientDealStats.get(clientId)
      if (!stats) {
        stats = {
          tested: 0,
          index_majority_found: 0,
          retrieval_majority_found: 0,
          indexed: 0,
          indexed_http: 0,
          retrievable: 0
        }
        clientDealStats.set(clientId, stats)
      }

      stats.tested++

      const evaluation = c.evaluation
      if (!evaluation) continue

      if (evaluation.indexerResult !== 'COMMITTEE_TOO_SMALL' && evaluation.indexerResult !== 'MAJORITY_NOT_FOUND') {
        stats.index_majority_found++
      }

      if (evaluation.indexerResult === 'OK' || evaluation.indexerResult === 'HTTP_NOT_ADVERTISED') {
        stats.indexed++
      }

      if (evaluation.indexerResult === 'OK') {
        stats.indexed_http++
      }

      if (evaluation.retrievalResult !== 'COMMITTEE_TOO_SMALL' && evaluation.indexerResult !== 'MAJORITY_NOT_FOUND') {
        stats.retrieval_majority_found++
      }

      if (evaluation.retrievalResult === 'OK') {
        stats.retrievable++
      }
    }
  }

  // Convert the nested map to an array for the query
  const flatStats = Array.from(minerClientDealStats.entries()).flatMap(
    ([minerId, clientDealStats]) => Array.from(clientDealStats.entries()).flatMap(
      ([clientId, stats]) => ({ minerId, clientId, ...stats })
    )
  )

  if (debug.enabled) {
    debug(
      'Updating public stats - daily deals: tested += %s index_majority_found += %s indexed += %s retrieval_majority_found += %s retrievable += %s',
      flatStats.reduce((sum, stat) => sum + stat.tested, 0),
      flatStats.reduce((sum, stat) => sum + stat.index_majority_found, 0),
      flatStats.reduce((sum, stat) => sum + stat.indexed, 0),
      flatStats.reduce((sum, stat) => sum + stat.retrieval_majority_found, 0),
      flatStats.reduce((sum, stat) => sum + stat.retrievable, 0)
    )
  }

  await pgClient.query(`
    INSERT INTO daily_deals (
      day,
      miner_id,
      client_id,
      tested,
      index_majority_found,
      indexed,
      indexed_http,
      retrieval_majority_found,
      retrievable
    ) VALUES (
      now(),
      unnest($1::text[]),
      unnest($2::text[]),
      unnest($3::int[]),
      unnest($4::int[]),
      unnest($5::int[]),
      unnest($6::int[]),
      unnest($7::int[]),
      unnest($8::int[])
    )
    ON CONFLICT(day, miner_id, client_id) DO UPDATE SET
      tested = daily_deals.tested + EXCLUDED.tested,
      index_majority_found = daily_deals.index_majority_found + EXCLUDED.index_majority_found,
      indexed = daily_deals.indexed + EXCLUDED.indexed,
      indexed_http = daily_deals.indexed_http + EXCLUDED.indexed_http,
      retrieval_majority_found = daily_deals.retrieval_majority_found + EXCLUDED.retrieval_majority_found,
      retrievable = daily_deals.retrievable + EXCLUDED.retrievable
  `, [
    flatStats.map(stat => stat.minerId),
    flatStats.map(stat => stat.clientId),
    flatStats.map(stat => stat.tested),
    flatStats.map(stat => stat.index_majority_found),
    flatStats.map(stat => stat.indexed),
    flatStats.map(stat => stat.indexed_http),
    flatStats.map(stat => stat.retrieval_majority_found),
    flatStats.map(stat => stat.retrievable)
  ])
}
