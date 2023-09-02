import { ethers } from 'ethers'

const { BigNumber } = ethers

export const evaluate = async ({
  rounds,
  roundIndex,
  ieContractWithSigner
}) => {
  // Get measurements
  const measurements = rounds[roundIndex] || []

  // Detect fraud
  const honestMeasurements = measurements.filter(m => isHonestMeasurement(m))

  // Calculate reward shares
  const peers = {}
  for (const measurement of honestMeasurements) {
    if (!peers[measurement.peerId]) {
      peers[measurement.peerId] = 0
    }
    peers[measurement.peerId] += 1
  }
  for (const [peerId, peerTotal] of Object.entries(peers)) {
    peers[peerId] = BigNumber.from(peerTotal)
      .mul(BigNumber.from(1_000_000_000_000_000))
      .div(BigNumber.from(honestMeasurements.length))
  }

  // Submit scores to IE
  console.log('setScores()')
  const tx = await ieContractWithSigner.setScores(
    roundIndex,
    Object.keys(peers),
    Object.values(peers),
    `${honestMeasurements.length} retrievals`
  )
  console.log(`Hash: ${tx.hash}`)

  // Clean up
  delete rounds[roundIndex]
}

// Detect fraud
// Simulate 10% fraud
const isHonestMeasurement = _measurement => {
  return Math.random() < 0.9
}
