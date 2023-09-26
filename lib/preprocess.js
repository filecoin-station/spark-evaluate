import assert from 'node:assert'
import { ethers } from 'ethers'
import { record } from './telemetry.js'

export const preprocess = async ({
  rounds,
  cid,
  roundIndex,
  web3Storage,
  logger
}) => {
  const {
    measurements,
    fetchDuration
  } = await fetchMeasurements(web3Storage, cid)
  const validMeasurements = measurements.filter(measurement => {
    try {
      assertValidMeasurement(measurement)
      return true
    } catch (err) {
      logger.error('Invalid measurement', err.message, measurement)
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
  const start = new Date()
  const res = await web3Storage.get(cid)
  const files = await res.files()
  const measurements = JSON.parse(await files[0].text())
  return { measurements, fetchDuration: new Date() - start }
}

const assertValidMeasurement = measurement => {
  assert(
    typeof measurement === 'object' && measurement !== null,
    'object required'
  )
  assert(ethers.utils.isAddress(measurement.peerId), 'valid peer id required')
}
