import assert from 'node:assert'
import { ethers } from 'ethers'

export const preprocess = async ({
  rounds,
  cid,
  roundIndex,
  web3Storage,
  logger
}) => {
  const measurements = (await fetchMeasurements(web3Storage, cid))
    // Rename "wallet_address" to "participantAddress"
    // eslint-disable-next-line camelcase
    .map(({ wallet_address, ...m }) => ({ ...m, participantAddress: wallet_address }))
    .filter(measurement => {
      try {
        assertValidMeasurement(measurement)
        return true
      } catch (err) {
        logger.error('Invalid measurement', err.message, measurement)
        return false
      }
    })
  logger.log(`Fetched ${measurements.length} valid measurements`)

  if (!rounds[roundIndex]) {
    rounds[roundIndex] = []
  }
  rounds[roundIndex].push(...measurements)
}

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
  assert(ethers.utils.isAddress(measurement.participantAddress), 'valid participant address required')
}
