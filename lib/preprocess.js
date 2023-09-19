import assert from 'node:assert'
import { ethers } from 'ethers'
import { ethAddressFromDelegated } from '@glif/filecoin-address'

export const preprocess = async ({
  rounds,
  cid,
  roundIndex,
  web3Storage,
  logger
}) => {
  const measurements = (await fetchMeasurements(web3Storage, cid))
    .filter(measurement => {
      try {
        // eslint-disable-next-line camelcase
        measurement.participantAddress = parseParticipantAddress(measurement.wallet_address)
        assertValidMeasurement(measurement)
        return true
      } catch (err) {
        logger.error('Invalid measurement:', err.message, measurement)
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

export const parseParticipantAddress = filWalletAddress => {
  try {
    return ethAddressFromDelegated(filWalletAddress)
  } catch (err) {
    err.message = `Invalid participant address ${filWalletAddress}: ${err.message}`
    err.filWalletAddress = filWalletAddress
    throw err
  }
}

const assertValidMeasurement = measurement => {
  assert(
    typeof measurement === 'object' && measurement !== null,
    'object required'
  )
  assert(ethers.utils.isAddress(measurement.participantAddress), 'valid participant address required')
}
