import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'
import pRetry from 'p-retry'
import { buildProviderRetrievalResultStats } from './public-stats.js'

const ONE_HOUR = 60 * 60 * 1000

const withPgClient = fn => async ({ createPgClient, ...args }) => {
  const pgClient = await createPgClient()
  try {
    return await fn({ pgClient, ...args })
  } finally {
    await pgClient.end()
  }
}

export const prepareProviderRetrievalResultStats = withPgClient(async ({ pgClient, round, committees, sparkEvaluateVersion, ieContractAddress }) => {
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
    round.details,
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

  const providers = new Map()
  for (const row of rows) {
    for (const [minerId, { successful, total }] of Object.entries(row.providerRetrievalResultStats)) {
      if (!providers.has(minerId)) {
        providers.set(minerId, { successful: 0, total: 0 })
      }
      providers.get(minerId).successful += successful
      providers.get(minerId).total += total
    }
  }

  const directoryCid = await pRetry(() => storachaClient.uploadDirectory([
    new File([JSON.stringify({
      date: rows[0].date,
      meta: {
        rounds: Object.fromEntries(rows.map(row => [
          row.round_index,
          {
            sparkEvaluateVersion: {
              gitCommit: row.spark_evaluate_version
            },
            measurementCommitments: row.measurement_commitments,
            roundDetails: row.round_details
          }
        ]))
      },
      providers
    })], 'commitment.json')
  ]))
  console.log(`https://${directoryCid}.ipfs.w3s.link/commitment.json`)

  const tx = await pRetry(() => rsrContract.addProviderRetrievalResultStats(directoryCid.toString()))
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

export const runPublishRsrLoop = async ({ createPgClient, storachaClient, rsrContract }) => {
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
