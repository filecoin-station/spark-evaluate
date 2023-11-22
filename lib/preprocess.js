import assert from 'node:assert'
import { ethers } from 'ethers'
import { ethAddressFromDelegated } from '@glif/filecoin-address'
import createDebug from 'debug'

const debug = createDebug('spark:preprocess')

export const preprocess = async ({
  rounds,
  cid,
  roundIndex,
  fetchMeasurements,
  recordTelemetry,
  logger
}) => {
  const start = new Date()
  /** @type import('./typings').Measurement[] */
  const measurements = await fetchMeasurements(cid)
  const fetchDuration = new Date() - start
  const validMeasurements = measurements
    // eslint-disable-next-line camelcase
    .map(({ participant_address, ...measurement }) => {
      try {
        return {
          ...measurement,
          participantAddress: parseParticipantAddress(participant_address),
          retrievalResult: calculateRetrievalResult(measurement)
        }
      } catch (err) {
        logger.error('Invalid measurement:', err.message, measurement)
        return null
      }
    })
    .filter(measurement => {
      if (measurement === null) return false

      if (debug.enabled) {
        // Print round & participant address & CID together to simplify lookup when debugging
        // Omit the `m` object from the format string to get nicer formatting
        debug(
          'RETRIEVAL RESULT for round=%s client=%s cid=%s: %s',
          roundIndex,
          measurement.participantAddress,
          measurement.cid,
          measurement.retrievalResult,
          measurement)
      }

      try {
        assertValidMeasurement(measurement)
        return true
      } catch (err) {
        logger.error('Invalid measurement:', err.message, measurement)
        return false
      }
    })
  logger.log(
    'PREPROCESS ROUND %s: Added measurements from CID %s\n%o',
    roundIndex,
    cid,
    { total: measurements.length, valid: validMeasurements.length }
  )

  if (!rounds[roundIndex]) {
    rounds[roundIndex] = []
  }
  rounds[roundIndex].push(...validMeasurements)

  recordTelemetry('preprocess', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_measurements', measurements.length)
    point.intField('valid_measurements', validMeasurements.length)
    point.intField('fetch_duration_ms', fetchDuration)
  })
}

export const fetchMeasurementsViaClient = async (web3Storage, cid) => {
  const res = await web3Storage.get(cid)
  const files = await res.files()
  const measurements = JSON.parse(await files[0].text())
  return measurements
}

export const fetchMeasurementsViaGateway = async (cid) => {
  const res = await fetch(`https://${encodeURIComponent(cid)}.ipfs.w3s.link/measurements.json`)
  if (!res.ok) {
    const msg = `Cannot fetch measurements ${cid}: ${res.status}\n${await res.text()}`
    throw new Error(msg)
  }
  const measurements = await res.json()
  return measurements
}

export const parseParticipantAddress = filWalletAddress => {
  // ETH addresses don't need any conversion
  if (filWalletAddress.startsWith('0x')) {
    return filWalletAddress
  }

  if (!filWalletAddress || filWalletAddress.startsWith('f1') || filWalletAddress.startsWith('t1')) {
    // As a temporary fix to allow us to build & test the fraud detection pipeline,
    // we assign measurements from f1/t1 addresses to the same 0x participant.
    return '0x000000000000000000000000000000000000dEaD'
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
  assert(typeof measurement.inet_group === 'string', 'valid inet group required')
  assert(typeof measurement.finished_at === 'string', 'field `finished_at` must be set to a string')
}

/**
 * @param {import('./typings').Measurement} measurement
 * @return {import('./typings').RetrievalResult}
 */
export const calculateRetrievalResult = measurement => {
  if (measurement.timeout) return 'TIMEOUT'
  if (measurement.car_too_large) return 'CAR_TOO_LARGE'
  switch (measurement.status_code) {
    case 502: return 'BAD_GATEWAY'
    case 504: return 'GATEWAY_TIMEOUT'
  }
  if (measurement.status_code >= 500) return 'SERVER_ERROR'

  const ok = measurement.status_code >= 200 &&
    measurement.status_code < 300 &&
    typeof measurement.end_at === 'string'

  return ok ? 'OK' : 'UNKNOWN_ERROR'
}
