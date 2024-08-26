import assert from 'node:assert'
import createDebug from 'debug'
import * as Sentry from '@sentry/node'
import pRetry from 'p-retry'

const debug = createDebug('spark:platform-stats')

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess.js').Measurement[]} honestMeasurements
 * @param {import('./preprocess.js').Measurement[]} allMeasurements
 */
export const updatePlatformStats = async (pgClient, honestMeasurements, allMeasurements) => {
  const participantsMap = await mapParticipantsToIds(pgClient, new Set(honestMeasurements.map(m => m.participantAddress)))
  await updateDailyParticipants(pgClient, Array.from(participantsMap.values()))
  await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements)
  await updateStationsAndParticipants(pgClient, honestMeasurements, allMeasurements, participantsMap)
}

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess.js').Measurement[]} honestMeasurements
 * @param {import('./preprocess.js').Measurement[]} allMeasurements
 * @param {object} options
 * @param {Date} [options.day]
 */
export const updateDailyStationStats = async (
  pgClient,
  honestMeasurements,
  allMeasurements,
  { day = new Date() } = {}
) => {
  /** @type{Map<string, {accepted: number, total: number}>} */
  const statsPerStation = new Map()

  for (const m of honestMeasurements) {
    if (m.stationId == null) continue

    const key = getStatsKey(m)
    const stationStats = statsPerStation.get(key) ?? { accepted: 0, total: 0 }

    stationStats.accepted += 1
    statsPerStation.set(key, stationStats)
  }

  for (const m of allMeasurements) {
    if (m.stationId == null) continue

    const key = getStatsKey(m)
    const stationStats = statsPerStation.get(key) ?? { accepted: 0, total: 0 }

    stationStats.total += 1
    statsPerStation.set(key, stationStats)
  }

  debug('Updating daily station stats, station_count=%s', statsPerStation.size)

  // Convert the map to two arrays for the query
  const keys = Array.from(statsPerStation.keys()).map(k => JSON.parse(k))
  const values = Array.from(statsPerStation.values())

  await pgClient.query(`
    INSERT INTO daily_stations (
      day,
      station_id,
      participant_address,
      inet_group,
      accepted_measurement_count,
      total_measurement_count
    )
    VALUES (
      $1::DATE,
      unnest($2::text[]),
      unnest($3::text[]),
      unnest($4::text[]),
      unnest($5::int[]),
      unnest($6::int[])
    )
    ON CONFLICT (day, station_id, participant_address, inet_group) DO UPDATE
    SET accepted_measurement_count = daily_stations.accepted_measurement_count
                                      + EXCLUDED.accepted_measurement_count,
        total_measurement_count = daily_stations.total_measurement_count
                                      + EXCLUDED.total_measurement_count
  `, [
    day,
    keys.map(k => k.stationId),
    keys.map(k => k.participantAddress),
    keys.map(k => k.inet_group),
    values.map(v => v.accepted),
    values.map(v => v.total)
  ])
}

/**
 * @param {import('pg').Client} pgClient
 * @param {number[]} participantIds
 */
export const updateDailyParticipants = async (pgClient, participantIds) => {
  debug('Updating daily participants, count=%s', participantIds.length)
  await pgClient.query(`
    INSERT INTO daily_participants (day, participant_id)
    SELECT now() as day, UNNEST($1::INT[]) AS participant_id
    ON CONFLICT DO NOTHING
  `, [
    participantIds
  ])
}

/**
 * @param {import('pg').Client} pgClient
 * @param {import('./preprocess.js').Measurement[]} honestMeasurements
 * @param {import('./preprocess.js').Measurement[]} allMeasurements
 * @param {Map<string, number>} participantsMap
 * @param {object} options
 * @param {Date} [options.day]
 */
export const updateStationsAndParticipants = async (
  pgClient,
  honestMeasurements,
  allMeasurements,
  participantsMap,
  { day = new Date() } = {}
) => {
  /** @type {Map<number, Map<string, {accepted: number, total: number}>>} */
  const participantsStationsStats = new Map()

  /** @type {Map<number, Set<string>>} */
  const subnets = new Map()

  /** @type {(m: import('./preprocess.js').Measurement, participantId: number, countType: 'accepted' | 'total') => void} */
  const updateParticipantsStationsStats = (m, participantId, countType) => {
    if (participantId == null) return

    let participant = participantsStationsStats.get(participantId)
    if (!participant) {
      participant = new Map()
      participantsStationsStats.set(participantId, participant)
    }

    let stationStats = participant.get(m.stationId)
    if (!stationStats) {
      stationStats = { accepted: 0, total: 0 }
      participant.set(m.stationId, stationStats)
    }

    stationStats[countType]++
  }

  for (const m of honestMeasurements) {
    if (m.stationId == null) continue
    const participantId = participantsMap.get(m.participantAddress)
    if (participantId == null) continue

    updateParticipantsStationsStats(m, participantId, 'accepted')

    const participantSubnets = subnets.get(participantId) ?? new Set()
    participantSubnets.add(m.inet_group)
    subnets.set(participantId, participantSubnets) // in case new set was created
  }

  for (const m of allMeasurements) {
    if (m.stationId == null) continue
    const participantId = participantsMap.get(m.participantAddress)
    if (participantId == null) continue

    updateParticipantsStationsStats(m, participantId, 'total')
  }

  debug('Updating recent station details, station_participant_count=%s, participant_subnet_count=%s', participantsStationsStats.size, subnets.size)

  // Convert the nested map structure to arrays for the query
  const flattenedStats = Array.from(participantsStationsStats.entries()).flatMap(([participantId, stationMap]) =>
    Array.from(stationMap.entries()).map(([stationId, stationStats]) => ({
      participantId,
      stationId,
      ...stationStats
    }))
  )

  // Convert the subnets map to an array for the query
  const participantsSubnets = Array.from(subnets.entries()).flatMap(([participantId, subnetSet]) =>
    Array.from(subnetSet).map(subnet => ({ participantId, subnet }))
  )

  await pgClient.query(`
    INSERT INTO recent_station_details (
      day,
      station_id,
      participant_id,
      accepted_measurement_count,
      total_measurement_count
    )
    VALUES (
      $1::DATE,
      unnest($2::text[]),
      unnest($3::int[]),
      unnest($4::int[]),
      unnest($5::int[])
    )
    ON CONFLICT (day, station_id, participant_id) DO UPDATE
    SET accepted_measurement_count = recent_station_details.accepted_measurement_count
                                      + EXCLUDED.accepted_measurement_count,
        total_measurement_count = recent_station_details.total_measurement_count
                                      + EXCLUDED.total_measurement_count
  `, [
    day,
    flattenedStats.map(s => s.stationId),
    flattenedStats.map(s => s.participantId),
    flattenedStats.map(s => s.accepted),
    flattenedStats.map(s => s.total)
  ])

  await pgClient.query(`
    INSERT INTO recent_active_stations (day, station_id)
    SELECT $1::DATE, unnest($2::text[])
    ON CONFLICT (day, station_id) DO NOTHING
  `, [
    day,
    flattenedStats.map(s => s.stationId)
  ])

  await pgClient.query(`
    INSERT INTO recent_participant_subnets (day, participant_id, subnet)
    SELECT $1::DATE, unnest($2::int[]), unnest($3::text[])
    ON CONFLICT (day, participant_id, subnet) DO NOTHING
  `, [
    day,
    participantsSubnets.map(ps => ps.participantId),
    participantsSubnets.map(ps => ps.subnet)
  ])
}

/**
 * @param {import('pg').Client} pgClient
 * @param {Set<string>} participantsSet
 * @returns {Promise<Map<string, number>>} A map of participant addresses to ids.
 */
export const mapParticipantsToIds = async (pgClient, participantsSet) => {
  debug('Mapping participants to id, count=%s', participantsSet.size)

  /** @type {Map<string, number>} */
  const participantsMap = new Map()

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
    participantsMap.set(participant_address, id)
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
      -- this no-op update is needed to populate "RETURNING id, participant_address"
      SET participant_address = EXCLUDED.participant_address
    RETURNING id, participant_address
  `, [
    newAddresses
  ])

  assert.strictEqual(created.length, newAddresses.length)
  // eslint-disable-next-line camelcase
  for (const { id, participant_address } of created) {
    participantsMap.set(participant_address, id)
  }

  return participantsMap
}

/**
 * @param {import('./typings.js').CreatePgClient} createPgClient
 * @param {object} options
 * @param {Array<Function>} [options.functionsToRun]
 */
export const refreshDatabase = async (
  createPgClient,
  { functionsToRun = [updateTopMeasurementParticipants, aggregateAndCleanupRecentData, updateMonthlyActiveStationCount] } = {}
) => {
  const pgClient = await createPgClient()

  try {
    const results = await Promise.allSettled(
      functionsToRun.map(func => func(pgClient))
    )

    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        console.error(`Error running function ${functionsToRun[index].name}:`, result.reason)
      }
    })
  } finally {
    await pgClient.end()
  }
}

/**
 * @param {import('pg').Client} pgClient
 */
export const updateTopMeasurementParticipants = async (pgClient) => {
  try {
    await pRetry(async () => {
      await pgClient.query('REFRESH MATERIALIZED VIEW top_measurement_participants_yesterday_mv')
    }, {
      retries: 3,
      onFailedAttempt: (error) => {
        console.warn(`Attempt to refresh materialized view failed: ${error.message}. Retrying...`)
      }
    })
  } catch (err) {
    console.error('Error refreshing top measurement participants', err)
    Sentry.captureException(err)
  }
}

/**
 * @param {import('pg').Client} pgClient
 */
export const aggregateAndCleanupRecentData = async (pgClient) => {
  try {
    await pgClient.query('BEGIN')

    // The ON CONFLICT theoretically should not happen, because it's two days old data,
    // which should not be updated anymore. But it's a good practice to have it.
    await pgClient.query(`
      WITH recent_station_details_summary AS (
        SELECT
          day,
          SUM(accepted_measurement_count) AS accepted_measurement_count,
          SUM(total_measurement_count) AS total_measurement_count,
          COUNT(DISTINCT station_id) AS distinct_active_station_count,
          COUNT(DISTINCT participant_id) AS distinct_participant_address_count
        FROM
          recent_station_details
        WHERE
          day <= CURRENT_DATE - INTERVAL '2 days'
        GROUP BY
          day
      ),
      recent_participant_subnets_summary AS (
        SELECT
          day,
          COUNT(DISTINCT subnet) AS distinct_inet_group_count
        FROM
          recent_participant_subnets
        WHERE
          day <= CURRENT_DATE - INTERVAL '2 days'
        GROUP BY
          day
      )
      INSERT INTO daily_measurements_summary (
        day,
        accepted_measurement_count,
        total_measurement_count,
        distinct_active_station_count,
        distinct_participant_address_count,
        distinct_inet_group_count
      )
      SELECT
        rsd.day,
        rsd.accepted_measurement_count,
        rsd.total_measurement_count,
        rsd.distinct_active_station_count,
        rsd.distinct_participant_address_count,
        rps.distinct_inet_group_count
      FROM
        recent_station_details_summary rsd
      JOIN
        recent_participant_subnets_summary rps ON rsd.day = rps.day
    `)

    // Delete aggregated data from recent_station_details and recent_participant_subnets
    await pgClient.query(`
      DELETE FROM recent_station_details WHERE day <= CURRENT_DATE - INTERVAL '2 days'
    `)
    await pgClient.query(`
      DELETE FROM recent_participant_subnets WHERE day <= CURRENT_DATE - INTERVAL '2 days'
    `)

    await pgClient.query('COMMIT')
  } catch (err) {
    console.error('Error aggregating and cleaning up recent data', err)
    await pgClient.query('ROLLBACK')
    Sentry.captureException(err)
  }
}

/**
 * @param {import('pg').Client} pgClient
 */
export const updateMonthlyActiveStationCount = async (pgClient) => {
  try {
    const result = await pgClient.query(`
      SELECT 1 FROM monthly_active_station_count
      WHERE month = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    `)

    if (result.rowCount === 0) {
      await pgClient.query('BEGIN')
      await pgClient.query(`
        INSERT INTO monthly_active_station_count (month, station_count)
        SELECT
          DATE_TRUNC('month', day) AS month,
          COUNT(DISTINCT station_id) AS station_count
        FROM recent_active_stations
        WHERE day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND day < DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY DATE_TRUNC('month', day)
      `)

      await pgClient.query(`
        DELETE FROM recent_active_stations
        WHERE day >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND day < DATE_TRUNC('month', CURRENT_DATE);
      `)

      await pgClient.query('COMMIT')
    }
  } catch (err) {
    console.error('Error updating monthly active station count', err)
    await pgClient.query('ROLLBACK')
    Sentry.captureException(err)
  }
}

// JSON stringify each compound key to make it comparable, since objects compare by reference
/** @type {(m: import('./preprocess.js').Measurement) => string} */
const getStatsKey = (m) => JSON.stringify({
  stationId: m.stationId,
  participantAddress: m.participantAddress,
  inet_group: m.inet_group
})
