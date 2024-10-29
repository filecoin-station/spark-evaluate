import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'
import pRetry from 'p-retry'
import { buildProviderRetrievalResultStats } from './public-stats.js'
import { createDagJsonCar } from './car.js'
import { CID } from 'multiformats'

const ONE_HOUR = 60 * 60 * 1000

const withPgClient = fn => async ({ createPgClient, ...args }) => {
  const pgClient = await createPgClient()
  try {
    return await fn({ pgClient, ...args })
  } finally {
    await pgClient.end()
  }
}

const publishRoundDetails = async ({ storachaClient, round }) => {
  const { car, cid } = await createDagJsonCar(round.details)
  await pRetry(() => storachaClient.uploadCAR(car))
  return cid
}

export const prepareProviderRetrievalResultStats = withPgClient(async ({ storachaClient, pgClient, round, committees, sparkEvaluateVersion, ieContractAddress }) => {
  const roundDetailsCid = await publishRoundDetails({ storachaClient, round })
  await pgClient.query(`
    INSERT INTO unpublished_rsr_rounds
    (round_index, contract_address, spark_evaluate_version, measurement_batches, round_details, provider_retrieval_result_stats)
    VALUES
    ($1, $2, $3, $4, $5, $6)
  `, [
    round.index,
    ieContractAddress,
    sparkEvaluateVersion,
    round.measurementBatches,
    roundDetailsCid.toString(),
    buildProviderRetrievalResultStats(committees)
  ])
})

const publishRsr = withPgClient(async ({ pgClient, storachaClient, rsrContract }) => {
  const { rows } = await pgClient.query(`
    WITH oldest_publishable_round AS (
      SELECT *
      FROM unpublished_rsr_rounds
      WHERE evaluated_at < now()::date
      ORDER BY evaluated_at
      LIMIT 1
    ),
    SELECT *, evaluated_at::DATE::TEXT as date
    FROM unpublished_rsr_rounds
    WHERE
      oldest_publishable_round.evaluated_at::date <= evaluated_at
      AND evaluated_at < oldest_publishable_round.evaluated_at::date + interval '1 day'
      AND contract_address = oldest_publishable_round.contract_address
    ORDER BY round_index
  `)

  const providerRetrievalResultStats = new Map()
  for (const row of rows) {
    for (const [providerId, { successful, total }] of Object.entries(row.providerRetrievalResultStats)) {
      if (!providerRetrievalResultStats.has(providerId)) {
        providerRetrievalResultStats.set(providerId, { successful: 0, total: 0 })
      }
      providerRetrievalResultStats.get(providerId).successful += successful
      providerRetrievalResultStats.get(providerId).total += total
    }
  }

  const { cid, car } = await createDagJsonCar({
    date: rows[0].date,
    meta: {
      rounds: rows.map(row => ({
        index: row.round_index,
        contractAddress: row.contract_address,
        sparkEvaluateVersion: {
          gitCommit: row.spark_evaluate_version
        },
        measurementBatches: row.measurement_batches.map(cid => CID.parse(cid)),
        details: CID.parse(row.round_details)
      }))
    },
    providerRetrievalResultStats: providerRetrievalResultStats
      .entries()
      .map(([providerId, { successful, total }]) => ({ providerId, successful, total }))
  })
  await pRetry(() => storachaClient.uploadCAR(car))
  console.log(`https://${cid}.ipfs.w3s.link`)

  const tx = await pRetry(() => rsrContract.addProviderRetrievalResultStats(cid.toString()))
  console.log(tx.hash)
  await tx.wait()

  await pgClient.query(`
    DELETE FROM unpublished_rsr_rounds
    WHERE
      round_index <= $1
      AND contract_address = $2
  `, [
    rows.at(-1).round_index,
    rows[0].contract_address
  ])
})

export const runPublishProviderRetrievalResultStatsLoop = async ({ createPgClient, storachaClient, rsrContract }) => {
  while (true) {
    try {
      await publishRsr({ createPgClient, storachaClient, rsrContract })
    } catch (err) {
      console.error(err)
      Sentry.captureException(err)
    } finally {
      await timers.setTimeout(ONE_HOUR)
    }
  }
}
