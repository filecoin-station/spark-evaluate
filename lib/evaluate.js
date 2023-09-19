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
    if (!peers[measurement.walletAddress]) {
      peers[measurement.walletAddress] = 0n
    }
    peers[measurement.walletAddress] += 1n
  }
  for (const [walletAddress, peerTotal] of Object.entries(peers)) {
    peers[walletAddress] = peerTotal *
      1_000_000_000_000_000n /
      BigInt(honestMeasurements.length)
  }

  // Submit scores to IE
  logger.log('setScores()')
  const tx = await ieContractWithSigner.setScores(
    roundIndex,
    Object.keys(peers),
    Object.values(peers),
    `${honestMeasurements.length} retrievals`
  )
  logger.log(`Hash: ${tx.hash}`)

  // Clean up
  delete rounds[roundIndex]
}

// Detect fraud
// Simulate 10% fraud
const isHonestMeasurement = _measurement => {
  return Math.random() < 0.9
}
