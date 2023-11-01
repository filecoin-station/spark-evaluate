import createDebug from 'debug'

const debug = createDebug('spark:evaluate')

export const evaluate = async ({
  rounds,
  roundIndex,
  ieContractWithSigner,
  fetchRoundDetails,
  recordTelemetry,
  logger
}) => {
  // Get measurements
  /** @type {Record<string, any>[]} */
  const measurements = rounds[roundIndex] || []

  // Detect fraud

  const sparkRoundDetails = await fetchRoundDetails(ieContractWithSigner.address, roundIndex, recordTelemetry)
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  await runFraudDetection(roundIndex, measurements, sparkRoundDetails)
  const honestMeasurements = measurements.filter(m => m.fraudAssessment === 'OK')

  // Calculate reward shares
  const participants = {}
  for (const measurement of honestMeasurements) {
    if (!participants[measurement.participantAddress]) {
      participants[measurement.participantAddress] = 0n
    }
    participants[measurement.participantAddress] += 1n
  }
  for (const [participantAddress, participantTotal] of Object.entries(participants)) {
    participants[participantAddress] = participantTotal *
      1_000_000_000_000_000n /
      BigInt(honestMeasurements.length)
  }

  // Calculate aggregates per fraud detection outcome
  // This is used for logging and telemetry
  /** @type {Record<import('./typings').FraudAssesment, number> */
  const fraudAssessments = {
    OK: 0,
    INVALID_TASK: 0,
    NO_FINISHED_AT: 0,
    NO_INET_GROUP: 0,
    DUP_INET_GROUP: 0
  }
  for (const m of measurements) {
    fraudAssessments[m.fraudAssessment] = (fraudAssessments[m.fraudAssessment] ?? 0) + 1
  }
  logger.log(
    'EVALUTE ROUND %s: Evaluated %s measurements, found %s honest entries.\n%o',
    roundIndex,
    measurements.length,
    honestMeasurements.length,
    fraudAssessments
  )

  // Submit scores to IE

  const totalScore = Object.values(participants).reduce((sum, val) => sum + val, 0n)
  logger.log(
    'EVALUTE ROUND %s: Invoking IE.setScores(); number of participants: %s, total score: %s',
    roundIndex,
    Object.keys(participants).length,
    totalScore === 1000000000000000n ? '100%' : totalScore
  )

  const start = new Date()
  const tx = await ieContractWithSigner.setScores(
    roundIndex,
    Object.keys(participants),
    Object.values(participants)
  )
  const setScoresDuration = new Date() - start
  logger.log('EVALUTE ROUND %s: IE.setScores() TX hash: %s', roundIndex, tx.hash)

  // Clean up
  delete rounds[roundIndex]

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_participants', Object.keys(participants).length)
    point.intField('total_measurements', measurements.length)
    point.intField('honest_measurements', honestMeasurements.length)
    point.intField('set_scores_duration_ms', setScoresDuration)

    for (const [type, count] of Object.entries(fraudAssessments)) {
      point.intField(`measurements_${type}`, count)
    }
  })
}

/**
 * @param {number} roundIndex
 * @param {import('./typings').Measurement[]} measurements
 * @param {import('./typings').RoundDetails} sparkRoundDetails
 */
export const runFraudDetection = async (roundIndex, measurements, sparkRoundDetails) => {
  //
  // 1. Filter out measurements not belonging to any valid task in this round
  //    or missing some of the required fields like `inet_group`
  //
  for (const m of measurements) {
    if (!m.inet_group) {
      m.fraudAssessment = 'NO_INET_GROUP'
      console.log('Invalid measurement:', m)
      continue
    }

    if (!m.finished_at) {
      m.fraudAssessment = 'NO_FINISHED_AT'
      console.log('Invalid measurement:', m)
      continue
    }

    const isValidTask = sparkRoundDetails.retrievalTasks.some(t =>
      t.cid === m.cid && t.providerAddress === m.provider_address & t.protocol === m.protocol
    )
    if (!isValidTask) {
      m.fraudAssessment = 'INVALID_TASK'
    }
  }

  //
  // 2. Reward only one participant in each inet group
  //
  /** @type {Map<string, import('./typings').Measurement[]>} */
  const taskGroups = new Map()
  for (const m of measurements) {
    if (m.fraudAssessment) continue

    const key = `${m.inet_group}::${m.cid}::${m.provider_address}`
    let group = taskGroups.get(key)
    if (!group) {
      group = []
      taskGroups.set(key, group)
    }

    group.push(m)
  }

  const getHash = (/** @type {import('./typings').Measurement} */ m) => {
    globalThis.crypto.subtle.digest('SHA-256', Buffer.from(m.finished_at))
  }

  for (const groupMeasurements of taskGroups.values()) {
    // Pick one measurement to reward and mark all others as not eligible for rewards
    // The difficult part: how to choose a measurement randomly but also fairly, so
    // that each measurement has the same chance of being picked for the reward?
    // We also want the selection algorithm to be deterministic.
    //
    // Note that we cannot rely on participant addresses because it's super easy
    // for node operators to use a different address for each measurement.
    //
    // Let's start with a simple algorithm we can later tweak:
    // 1. Hash the `finished_at` timestamp recorded by the server
    // 2. Pick the measurement with the lowest hash value
    // This relies on the fact that the hash function has a random distribution.
    // We are also making the assumption that each measurement has a distinct `finished_at` field.

    const chosen = { m: groupMeasurements[0], h: getHash(groupMeasurements[0]) }
    for (let i = 1; i < groupMeasurements.length; i++) {
      const m = groupMeasurements[i]
      const h = getHash(m)
      if (h < chosen.h) {
        chosen.m = m
        chosen.h = h
      }
    }

    for (const m of groupMeasurements) {
      m.fraudAssessment = m === chosen.m ? 'OK' : 'DUP_INET_GROUP'
    }
  }

  if (debug.enabled) {
    for (const m of measurements) {
      // Print round & participant address & CID together to simplify lookup when debugging
      // Omit the `m` object from the format string to get nicer formatting
      debug(
        'FRAUD ASSESSMENT for round=%s client=%s cid=%s',
        roundIndex,
        m.participantAddress,
        m.cid,
        m)
    }
  }
}
