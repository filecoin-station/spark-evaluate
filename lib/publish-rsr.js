import * as Sentry from '@sentry/node'
import timers from 'node:timers/promises'
import pRetry from 'p-retry'

const ONE_HOUR = 60 * 60 * 1000

const withPgClient = fn => async ({ createPgClient, ...args }) => {
  const pgClient = await createPgClient()
  try {
    return await fn({ pgClient, ...args })
  } finally {
    await pgClient.end()
  }
}

export const prepareAcceptedRetrievalTaskMeasurementsCommitment = withPgClient(async ({ pgClient, round, sparkEvaluateVersion }) => {
  await pgClient.query(`
    INSERT INTO unpublished_rsr_rounds
    (round_index, spark_evaluate_version, measurement_commitments, round_details, providers)
    VALUES
    ($1, $2, $3, $4, $5)
  `, [
    round.index,
    sparkEvaluateVersion,
    round.measurementCommitments,
    round.details,
    round.measurements.reduce((acc, m) => {
      acc[m.minerId] = acc[m.minerId] || { successful: 0, total: 0 }
      if (m.fraudAssessment === 'OK') {
        acc[m.minerId].total++
        if (m.retrievalResult === 'OK') {
          acc[m.minerId].successful++
        }
      }
      return acc
    }, {})
  ])
})

const publishRsr = withPgClient(async ({ pgClient, storachaClient, rsrContract }) => {
  const { rows } = await pgClient.query(`
    SELECT *
    FROM unpublished_rsr_rounds
    WHERE evaluated_at <= now()::date AND evaluated_at > now()::date - interval '1 day'
    ORDER BY round_index
  `)

  const providers = new Map()
  for (const row of rows) {
    for (const [minerId, { successful, total }] of Object.entries(row.providers)) {
      if (!providers.has(minerId)) {
        providers.set(minerId, { successful: 0, total: 0 })
      }
      providers.get(minerId).successful += successful
      providers.get(minerId).total += total
    }
  }

  const directoryCid = await pRetry(() => storachaClient.uploadDirectory([
    new File([JSON.stringify({
      date: rows[0].evaluated_at.toISOString().split('T')[0],
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
    WHERE round_index < $1
  `, [rows[0].round_index])
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
