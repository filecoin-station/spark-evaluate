import { mapParticipantsToIds } from '../../lib/platform-stats.js'

/**
 * Used by spark-stats
 * import { givenDailyParticipants } from 'spark-evaluate/test/helpers/queries'
 * @param {import('pg').Client} pgClient
 * @param {string} day
 * @param {string[]} participantAddresses
 */
export const givenDailyParticipants = async (pgClient, day, participantAddresses) => {
  const ids = await mapParticipantsToIds(pgClient, new Set(participantAddresses))
  await pgClient.query(`
    INSERT INTO daily_participants (day, participant_id)
    SELECT $1 as day, UNNEST($2::INT[]) AS participant_id
    ON CONFLICT DO NOTHING
  `, [
    day,
    ids
  ])
}
