import createDebug from 'debug'
import pg from 'pg'
import { DATABASE_URL } from './config.js'

const debug = createDebug('spark:public-stats')

/**
 * @param {import('./preprocess').Measurement[]} measurements
 */
export const updatePublicStats = async (measurements) => {
  let total = 0
  let successful = 0
  for (const m of measurements) {
    total++
    // TODO: take into account fraud detection
    if (m.retrievalResult === 'OK') successful++
  }

  debug('Updating public retrieval stats: total += %s successful += %s', total, successful)
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  try {
    await pgClient.query(`
    INSERT INTO retrievals
      (day, total, successful)
    VALUES
      (now(), $1, $2)
    ON CONFLICT(day) DO UPDATE SET
      total = retrievals.total + $1,
      successful = retrievals.total + $2
  `, [
      total,
      successful
    ])
  } finally {
    await pgClient.end()
  }
}
