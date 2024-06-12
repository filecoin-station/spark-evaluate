import { mapParticipantsToIds } from '../../lib/platform-stats.js'

// Used by spark-stats
// import { givenDailyParticipants } from 'spark-evaluate/test/helpers/queries'
export const givenDailyParticipants = async (pgPool, day, participantAddresses) => { 
  const ids = await mapParticipantsToIds(pgPool, new Set(participantAddresses)) 
  await pgPool.query(` 
    INSERT INTO daily_participants (day, participant_id) 
    SELECT $1 as day, UNNEST($2::INT[]) AS participant_id 
    ON CONFLICT DO NOTHING 
  `, [ 
    day, 
    ids 
  ]) 
}
