import assert from 'node:assert'
import { ethers } from 'ethers'
import { ethAddressFromDelegated } from '@glif/filecoin-address'
import { record } from './telemetry.js'

export const preprocess = async ({
  rounds,
  cid,
  roundIndex,
  web3Storage,
  logger
}) => {
  const start = new Date()
  const measurements = await fetchMeasurements(web3Storage, cid)
  const fetchDuration = new Date() - start
  const validMeasurements = measurements
    // eslint-disable-next-line camelcase
    .map(({ participant_address, ...measurement }) => {
      try {
        return {
          ...measurement,
          participantAddress: parseParticipantAddress(participant_address)
        }
      } catch (err) {
        logger.error('Invalid measurement:', err.message, measurement)
        return null
      }
    })
    .filter(measurement => {
      if (measurement === null) return false
      try {
        assertValidMeasurement(measurement)
        return true
      } catch (err) {
        logger.error('Invalid measurement:', err.message, measurement)
        return false
      }
    })
  logger.log(`Fetched ${validMeasurements.length} valid measurements`)

  if (!rounds[roundIndex]) {
    rounds[roundIndex] = []
  }
  rounds[roundIndex].push(...validMeasurements)

  record('preprocess', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_measurements', measurements.length)
    point.intField('valid_measurements', validMeasurements.length)
    point.intField('fetch_duration_ms', fetchDuration)
  })
}

const fetchMeasurements = async (web3Storage, cid) => {
  const res = await web3Storage.get(cid)
  const files = await res.files()
  const measurements = JSON.parse(await files[0].text())
  return measurements
}

export const parseParticipantAddress = filWalletAddress => {
  if (filWalletAddress.startsWith('f1') || filWalletAddress.startsWith('t1')) {
    // As a temporary fix to allow us to build & test the fraud detection pipeline,
    // we assign measurements from f1/t1 addresses to the same 0x participant.
    return '0xf100Ac342b7DE48e5c89f7029624eE6c3Cde68aC'
  }

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
