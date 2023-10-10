import { record } from './telemetry.js'
import { SPARK_API } from './config.js'

export const evaluate = async ({
  rounds,
  roundIndex,
  ieContractWithSigner,
  recordTelemetry,
  logger
}) => {
  // Get measurements
  /** @type {Record<string, any>[]} */
  const measurements = rounds[roundIndex] || []

  // Detect fraud
  await runFraudDetection(roundIndex, measurements)
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

  // Submit scores to IE
  logger.log('setScores()')
  const start = new Date()
  const tx = await ieContractWithSigner.setScores(
    roundIndex,
    Object.keys(participants),
    Object.values(participants)
  )
  const setScoresDuration = new Date() - start
  logger.log(`Hash: ${tx.hash}`)

  // Clean up
  delete rounds[roundIndex]

  // Calculate aggregates per fraud detection outcome
  const fraudAssessments = {
    OK: 0,
    INVALID_TASK: 0
  }
  for (const m of measurements) {
    fraudAssessments[m.fraudAssessment] = (fraudAssessments[m.fraudAssessment] ?? 0) + 1
  }

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

export const runFraudDetection = async (roundIndex, measurements) => {
  const roundDetails = await fetchRoundDetails(roundIndex)
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
}

const fetchRoundDetails = async (/** @type {BigInt} */ roundIndex) => {
  const start = new Date()
  let status = 0

  try {
    const res = await fetch(`${SPARK_API}/rounds/${roundIndex}`)
    status = res.status
    if (!res.ok) {
      const msg = `Cannot fetch tasks for round ${roundIndex}: ${status}\n${await res.text()}`
      throw new Error(msg)
    }
    /** @type {import('./typings.d.ts').RoundDetails} */
    const details = res.json()
    return details
  } finally {
    record('fetch_tasks_for_round', point => {
      point.intField('round_index', roundIndex)
      point.intField('fetch_duration_ms', new Date() - start)
      point.intField('status', status)
    })
  }
}
