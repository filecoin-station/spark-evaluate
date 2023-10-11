import debugFactory from 'debug'

const debug = debugFactory('spark:evaluate')

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
  await runFraudDetection(roundIndex, measurements, { fetchRoundDetails, recordTelemetry })
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
  const fraudAssessments = {
    OK: 0,
    INVALID_TASK: 0
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

export const runFraudDetection = async (roundIndex, measurements, { fetchRoundDetails, recordTelemetry }) => {
  const roundDetails = await fetchRoundDetails(roundIndex, recordTelemetry)
  debug(`ROUND DETAILS for round=${roundIndex}`, roundDetails)

  for (const m of measurements) {
    const isValidTask = roundDetails.retrievalTasks.some(t =>
      t.cid === m.cid && t.providerAddress === m.provider_address & t.protocol === m.protocol
    )
    if (!isValidTask) {
      m.fraudAssessment = 'INVALID_TASK'
      continue
    }

    // TODO: add more fraud detections

    m.fraudAssessment = 'OK'
  }

  if (debug.enabled) {
    for (const m of measurements) {
      // Print round & participant address & CID together to simplify lookup when debugging
      debug(`FRAUD ASSESSMENT for round=${roundIndex} client=${m.participantAddress} cid=${m.cid}`, m)
    }
  }
}
