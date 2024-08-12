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
  const participantMap = await mapParticipantsToIds(pgClient, new Set(honestMeasurements.map(m => m.participantAddress)))
  await updateDailyParticipants(pgClient, Array.from(participantMap.values()))
  await updateDailyStationStats(pgClient, honestMeasurements, allMeasurements)
  await updateStationAndParticipantDetails(pgClient, honestMeasurements, allMeasurements, participantMap)
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

  // JSON stringify the key to make it comparable, since objects compare by reference
  /** @type {(m: import('./preprocess.js').Measurement) => string} */
  const getKey = (m) => JSON.stringify({
    stationId: m.stationId,
    participantAddress: m.participantAddress,
    inet_group: m.inet_group
  })

  for (const m of honestMeasurements) {
    if (m.stationId == null) continue

    const key = getKey(m)
    const stationStats = statsPerStation.get(key) ?? { accepted: 0, total: 0 }

    stationStats.accepted += 1
    statsPerStation.set(key, stationStats)
  }

  for (const m of allMeasurements) {
    if (m.stationId == null) continue

    const key = getKey(m)
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
 * @param {Map<string, number>} participantMap
 * @param {object} options
 * @param {Date} [options.day]
 */
export const updateStationAndParticipantDetails = async (
  pgClient,
  honestMeasurements,
  allMeasurements,
  participantMap,
  { day = new Date() } = {}
) => {
  const statsPerStationParticipant = new Map()
  const participantSubnetsSet = new Set()

  // JSON stringify the key to make it comparable, since objects compare by reference
  /** @type {(m: import('./preprocess.js').Measurement, participantId: number) => string} */
  const getStationParticipantKey = (m, participantId) => JSON.stringify({
    stationId: m.stationId,
    participantId
  })
  /** @type {(m: import('./preprocess.js').Measurement, participantId: number) => string} */
  const getParticipantSubnetKey = (m, participantId) => JSON.stringify({
    participantId,
    subnet: m.inet_group
  })

  for (const m of honestMeasurements) {
    if (m.stationId == null) continue
    const participantId = participantMap.get(m.participantAddress)

    const key = getStationParticipantKey(m, participantId)
    const stationStats = statsPerStationParticipant.get(key) ?? { accepted: 0, total: 0 }

    stationStats.accepted += 1
    statsPerStationParticipant.set(key, stationStats)
    participantSubnetsSet.add(getParticipantSubnetKey(m, participantId))
  }

  for (const m of allMeasurements) {
    if (m.stationId == null) continue

    const key = getStationParticipantKey(m, participantMap.get(m.participantAddress))
    const stationStats = statsPerStationParticipant.get(key) ?? { accepted: 0, total: 0 }

    stationStats.total += 1
    statsPerStationParticipant.set(key, stationStats)
  }

  debug('Updating recent station details, station_participant_count=%s, participant_subnet_count=%s', statsPerStationParticipant.size, participantSubnetsSet.size)

  // Convert the map and set to arrays for the query
  const stationParticipants = Array.from(statsPerStationParticipant.keys()).map(k => JSON.parse(k))
  const stats = Array.from(statsPerStationParticipant.values())
  const participantSubnets = Array.from(participantSubnetsSet).map(k => JSON.parse(k))

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
    stationParticipants.map(k => k.stationId),
    stationParticipants.map(k => k.participantId),
    stats.map(v => v.accepted),
    stats.map(v => v.total)
  ])

  await pgClient.query(`
    INSERT INTO recent_active_stations (day, station_id)
    SELECT $1::DATE, unnest($2::text[])
    ON CONFLICT (day, station_id) DO NOTHING
  `, [
    day,
    stationParticipants.map(k => k.stationId)
  ])

  await pgClient.query(`
    INSERT INTO recent_participant_subnets (day, participant_id, subnet)
    SELECT $1::DATE, unnest($2::int[]), unnest($3::text[])
    ON CONFLICT (day, participant_id, subnet) DO NOTHING
  `, [
    day,
    participantSubnets.map(k => k.participantId),
    participantSubnets.map(k => k.subnet)
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
  const participantMap = new Map()

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
    participantMap.set(participant_address, id)
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
    participantMap.set(participant_address, id)
  }

  return participantMap
}

/**
 * @param {import('./typings.js').CreatePgClient} createPgClient
 * @param {object} options
 * @param {Array<Function>} [options.functionsToRun]
 */
export const periodicDatabaseRefresh = async (
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
      WITH rsd_summary AS (
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
      rps_summary AS (
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
        rsd_summary rsd
      JOIN
        rps_summary rps ON rsd.day = rps.day
      ON CONFLICT (day) DO UPDATE SET
        accepted_measurement_count = EXCLUDED.accepted_measurement_count,
        total_measurement_count = EXCLUDED.total_measurement_count,
        distinct_active_station_count = EXCLUDED.distinct_active_station_count,
        distinct_participant_address_count = EXCLUDED.distinct_participant_address_count,
        distinct_inet_group_count = EXCLUDED.distinct_inet_group_count;
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
