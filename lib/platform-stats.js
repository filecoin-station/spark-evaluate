import assert from 'node:assert'
import createDebug from 'debug'

const debug = createDebug('spark:platform-stats')

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess').Measurement[]} honestMeasurements
 */
export const updatePlatformStats = async (pgClient, honestMeasurements) => {
  const participants = new Set(honestMeasurements.map(m => m.participantAddress))
  await updateDailyParticipants(pgClient, participants)
  await updateDailyStationStats(pgClient, honestMeasurements)
}

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess').Measurement[]} honestMeasurements
 */
export const updateDailyStationStats = async (pgClient, honestMeasurements) => {
  // TODO: when we add more fields, we will update the ON CONFLICT clause
  // to update those fields, and we won't just use a Set for the stationIds
  // which currently removes all granular measurement details
  const stationIds = [
    ...new Set(
      honestMeasurements
        .filter(m => m.stationId !== null)
        .map(m => m.stationId)
    )
  ]
  debug('Updating daily station stats, unique_count=%s', stationIds.length)

  await pgClient.query(`
    INSERT INTO daily_stations (station_id, day)
    SELECT unnest($1::text[]), now()
    ON CONFLICT (station_id, day) DO NOTHING
  `, [stationIds])
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
