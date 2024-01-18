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
    await updateDailyParticipants(pgClient, participants)
  } finally {
    await pgClient.end()
  }
}

/**
 * @param {import('pg').Client} pgClient
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
 * @param {import('pg').Client} pgClient
 * @param {Set<string>} participants
 */
export const updateDailyParticipants = async (pgClient, participants) => {
  debug('Updating daily participants, count=%s', participants.size)
  const ids = await mapParticipantsToIds(pgClient, participants)
  await pgClient.query(`
    INSERT INTO daily_participants (day, participant_id)
    SELECT now() as day, UNNEST($1::INT[]) AS participant_id
    ON CONFLICT DO NOTHING
  `, [
    ids
  ])
}

/**
 * @param {import('pg').Client} pgClient
 * @param {Set<string>} participantsSet
 * @returns {Promise<string[]>} A list of participant ids. The order of ids is not defined.
 */
export const mapParticipantsToIds = async (pgClient, participantsSet) => {
  debug('Mapping participants to id, count=%s', participantsSet.size)

  /** @type {string[]} */
  const ids = []

  // TODO: We can further optimise performance of this function by using
  // an in-memory LRU cache. Our network has currently ~2k participants,
  // we need ~50 bytes for each (address, id) pair, that's only ~100KB of data.

  // TODO: passing the entire list of participants as a single query parameter
  // will probably not scale beyond several thousands of addresses. We will
  // need to rework the queries to split large arrays into smaller batches.

  // In most rounds, we have already seen most of the participant addresses
  // If we use "INSERT...ON CONFLICT", then PG increments id counter even for
  // existing addresses where we end up skipping the insert. This could quickly
  // exhaust the space of all 32bit integers.
  // Solution: query the table for know records before running the insert.
  //
  // Caveat: In my testing, this query was not able to leverage the (unique)
  // index on participants.participant_address and performed a full table scan
  // after the array grew past ~10 items. If this becomes a problem, we can
  // introduce the LRU cache mentioned above.
  const { rows: found } = await pgClient.query(
    'SELECT * FROM participants WHERE participant_address = ANY($1::TEXT[])',
    [Array.from(participantsSet.values())]
  )
  debug('Known participants count=%s', found.length)

  // eslint-disable-next-line camelcase
  for (const { id, participant_address } of found) {
    ids.push(id)
    participantsSet.delete(participant_address)
  }

  debug('New participant addresses count=%s', participantsSet.size)

  // Register the new addresses. Use "INSERT...ON CONFLICT" to handle the race condition
  // where another client may have registered these addresses between our previous
  // SELECT query and the next INSERT query.
  const newAddresses = Array.from(participantsSet.values())
  debug('Registering new participant addresses, count=%s', newAddresses.length)
  const { rows: created } = await pgClient.query(`
    INSERT INTO participants (participant_address)
    SELECT UNNEST($1::TEXT[]) AS participant_address
    ON CONFLICT(participant_address) DO UPDATE
      -- this no-op update is needed to populate "RETURNING id"
      SET participant_address = EXCLUDED.participant_address
    RETURNING id
  `, [
    newAddresses
  ])

  assert.strictEqual(created.length, newAddresses.length)
  return ids.concat(created.map(r => r.id))
}
