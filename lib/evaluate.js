export const evaluate = async ({
  rounds,
  roundIndex,
  ieContractWithSigner,
  recordTelemetry,
  logger
}) => {
  // Get measurements
  const measurements = rounds[roundIndex] || []

  // Detect fraud
  const honestMeasurements = measurements.filter(m => isHonestMeasurement(m))

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

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_participants', Object.keys(participants).length)
    point.intField('total_measurements', measurements.length)
    point.intField('honest_measurements', honestMeasurements.length)
    point.intField('set_scores_duration_ms', setScoresDuration)
  })
}

// Detect fraud
// Simulate 10% fraud
const isHonestMeasurement = _measurement => {
  return Math.random() < 0.9
}
