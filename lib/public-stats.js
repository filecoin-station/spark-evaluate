import assert from 'node:assert'
import createDebug from 'debug'

const debug = createDebug('spark:public-stats')

/**
 * @param {object} args
 * @param {import('./typings').CreatePgClient} args.createPgClient
 * @param {import('./preprocess').Measurement[]} args.honestMeasurements
 */
export const updatePublicStats = async ({ createPgClient, honestMeasurements }) => {
  const retrievalStats = { total: 0, successful: 0 }
  const participants = new Set()
  for (const m of honestMeasurements) {
    retrievalStats.total++
    if (m.retrievalResult === 'OK') retrievalStats.successful++

    participants.add(m.participantAddress)
  }

  const pgClient = await createPgClient()
  try {
    await updateRetrievalStats(pgClient, retrievalStats)
    await updateDailyParticipants(pgClient, Array.from(participants.values()))
  } finally {
    await pgClient.end()
  }
}

/**
 * @param {import('./typings').CreatePgClient} createPgClient
 * @param {object} stats
 * @param {number} stats.total
 * @param {number} stats.successful
 */
const updateRetrievalStats = async (pgClient, { total, successful }) => {
  debug('Updating public retrieval stats: total += %s successful += %s', total, successful)
  await pgClient.query(`
    INSERT INTO retrieval_stats
      (day, total, successful)
    VALUES
      (now(), $1, $2)
    ON CONFLICT(day) DO UPDATE SET
      total = retrieval_stats.total + $1,
      successful = retrieval_stats.successful + $2
  `, [
    total,
    successful
  ])
}

/**
 * @param {import('./typings').CreatePgClient} createPgClient
 * @param {string[]} participants
 */
const updateDailyParticipants = async (pgClient, participants) => {
  debug('Updating daily participants (%s seen)', participants.length)
  for (const participantAddress of participants) {
    const participantId = await mapParticipantToId(pgClient, participantAddress)
    await pgClient.query(`
      INSERT INTO daily_participants
        (day, participant_id)
      VALUES
        (now(), $1)
      ON CONFLICT DO NOTHING
    `, [
      participantId
    ])
  }
}

/**
 * @param {import('./typings').CreatePgClient} createPgClient
 * @param {string} participantAddress
 */
const mapParticipantToId = async (pgClient, participantAddress) => {
  const { rows } = await pgClient.query(`
    INSERT INTO participants (participant_address) VALUES ($1)
    ON CONFLICT(participant_address) DO UPDATE
      -- this no-op update is needed to populate "RETURNING id"
      SET participant_address = EXCLUDED.participant_address
    RETURNING id
  `, [
    participantAddress
  ])

  assert.strictEqual(rows.length, 1)
  const { id } = rows[0]
  assert.strictEqual(typeof id, 'number')
  return id
}
