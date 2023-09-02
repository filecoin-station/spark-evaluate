import assert from 'node:assert'
import { ethers } from 'ethers'

// Preprocess
export const preprocess = async ({ rounds, cid, roundIndex, web3Storage }) => {
  const measurements = (await fetchMeasurements(web3Storage, cid))
    .filter(measurement => {
      try {
        assertValidMeasurement(measurement)
        return true
      } catch (err) {
        console.error('Invalid measurement', err.message, measurement)
        return false
      }
    })
  console.log(`Fetched ${measurements.length} valid measurements`)

  if (!rounds[roundIndex]) {
    rounds[roundIndex] = []
  }
  rounds[roundIndex].push(...measurements)
}

// Fetch measurements
const fetchMeasurements = async (web3Storage, cid) => {
  const res = await web3Storage.get(cid)
  const files = await res.files()
  const measurements = JSON.parse(await files[0].text())
  return measurements
}

const assertValidMeasurement = measurement => {
  assert(
    typeof measurement === 'object' && measurement !== null,
    'object required'
  )
  assert(ethers.utils.isAddress(measurement.peerId), 'valid peer id required')
}
