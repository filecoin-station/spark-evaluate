import { record } from './telemetry.js'

export const evaluate = async ({
  rounds,
  roundIndex,
  ieContractWithSigner,
  logger
}) => {
  // Get measurements
  const measurements = rounds[roundIndex] || []

  // Detect fraud
  const honestMeasurements = measurements.filter(m => isHonestMeasurement(m))

  // Calculate reward shares
  const peers = {}
  for (const measurement of honestMeasurements) {
    if (!peers[measurement.peerId]) {
      peers[measurement.peerId] = 0n
    }
    peers[measurement.peerId] += 1n
  }
  for (const [peerId, peerTotal] of Object.entries(peers)) {
    peers[peerId] = peerTotal *
      1_000_000_000_000_000n /
      BigInt(honestMeasurements.length)
  }

  // Submit scores to IE
  logger.log('setScores()')
  const start = new Date()
  const tx = await ieContractWithSigner.setScores(
    roundIndex,
    Object.keys(peers),
    Object.values(peers),
    `${honestMeasurements.length} retrievals`
  )
  const setScoresDuration = new Date() - start
  logger.log(`Hash: ${tx.hash}`)

  // Clean up
  delete rounds[roundIndex]

  record('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_peers', Object.keys(peers).length)
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
