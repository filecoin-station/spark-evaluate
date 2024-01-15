import createDebug from 'debug'

const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings').CreatePgClient} args.createPgClient
 * @param {import('./preprocess').Measurement[]} args.measurements
 */
export const updatePublicStats = async ({ createPgClient, measurements }) => {
  let total = 0
  let successful = 0
  for (const m of measurements) {
    total++
    // TODO: take into account fraud detection
    if (m.retrievalResult === 'OK') successful++
  }

  debug('Updating public retrieval stats: total += %s successful += %s', total, successful)
  const pgClient = await createPgClient()
  try {
    await pgClient.query(`
    INSERT INTO retrieval_stats
      (day, total, successful)
    VALUES
      (now(), $1, $2)
    ON CONFLICT(day) DO UPDATE SET
      total = retrieval_stats.total + $1,
      successful = retrieval_stats.total + $2
  `, [
      total,
      successful
    ])
  } finally {
    await pgClient.end()
  }
}
