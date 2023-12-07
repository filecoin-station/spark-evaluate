import assert from 'node:assert'
import { ethers } from 'ethers'
import { ethAddressFromDelegated } from '@glif/filecoin-address'
import { CarReader } from '@ipld/car'
import { validateBlock } from '@web3-storage/car-block-validator'
import { recursive as exporter } from 'ipfs-unixfs-exporter'
import createDebug from 'debug'
import { RoundData } from './round.js'

const debug = createDebug('spark:preprocess')

export class Measurement {
  /**
   *
   * @param {import('./typings.js').RawMeasurement} m
   * @param {(string) => string} pointerize
   */
  constructor (m, pointerize = (v) => v) {
    this.participantAddress = pointerize(parseParticipantAddress(m.participant_address))
    this.retrievalResult = pointerize(getRetrievalResult(m))
    this.cid = pointerize(m.cid)
    this.spark_version = pointerize(m.spark_version)
    this.fraudAssessment = null
    this.inet_group = pointerize(m.inet_group)
    this.finished_at = parseDateTime(m.finished_at, 'finished_at')
    this.provider_address = pointerize(m.provider_address)
    this.protocol = pointerize(m.protocol)

    if (
      m.byte_length !== undefined &&
      !(Number.isInteger(m.byte_length) && m.byte_length >= 0)
    ) {
      throw new Error(`Invalid byte_length value, expected non-negative integer: ${m.byte_length}`)
    }
    this.byteLength = m.byte_length

    const startAt = parseDateTime(m.start_at, 'start_at')
    const firstByteAt = parseDateTime(m.first_byte_at, 'first_byte_at')
    const endAt = parseDateTime(m.end_at, 'end_at')

    if (firstByteAt !== undefined && startAt === undefined) {
      throw new Error('Invalid measurement: first_byte_at is set, but start_at is not.')
    }
    this.ttfb = firstByteAt - startAt
    if (this.ttfb < 0) {
      throw new Error('Invalid measurement: first_byte_at is before start_at.')
    }

    if (endAt !== undefined && startAt === undefined) {
      throw new Error('Invalid measurement: end_at is set, but start_at is not.')
    }
    this.duration = endAt - startAt
    if (this.duration < 0) {
      throw new Error('Invalid measurement: end_at is before start_at.')
    }
  }
}

const parseDateTime = (str, fieldName) => {
  if (!str) return undefined
  const value = new Date(str)
  if (Number.isNaN(value.getTime())) {
    throw new Error(`Invalid ${fieldName} value, expected a Date string: ${str}`)
  }
  return value.getTime()
}

export const preprocess = async ({
  rounds,
  cid,
  roundIndex,
  fetchMeasurements,
  recordTelemetry,
  logger
}) => {
  if (!rounds[roundIndex]) {
    rounds[roundIndex] = new RoundData()
  }

  const start = new Date()
  /** @type import('./typings').RawMeasurement[] */
  const measurements = await fetchMeasurements(cid)
  const fetchDuration = new Date() - start
  const validMeasurements = measurements
    // eslint-disable-next-line camelcase
    .map(measurement => {
      try {
        return new Measurement(measurement, rounds[roundIndex].pointerize)
      } catch (err) {
        logger.error('Invalid measurement:', err.message, measurement)
        return null
      }
    })
    .filter(measurement => {
      if (measurement === null) return false

      // Print round & participant address & CID together to simplify lookup when debugging
      // Omit the `m` object from the format string to get nicer formatting
      debug(
        'RETRIEVAL RESULT for round=%s client=%s cid=%s: %s',
        roundIndex,
        measurement.participantAddress,
        measurement.cid,
        measurement.retrievalResult,
        measurement)

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

  const okCount = validMeasurements.reduce((c, m) => m.retrievalResult === 'OK' ? c + 1 : c, 0)
  const total = validMeasurements.length
  logger.log('Retrieval Success Rate: %s%s (%s of %s)', Math.round(100 * okCount / total), '%', okCount, total)

  rounds[roundIndex].measurements.push(...validMeasurements)

  recordTelemetry('preprocess', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_measurements', measurements.length)
    point.intField('valid_measurements', validMeasurements.length)
    point.intField('fetch_duration_ms', fetchDuration)
  })

  /** @type {Map<string,number>} */
  const sparkVersions = new Map()
  for (const m of validMeasurements) {
    if (typeof m.spark_version !== 'string') continue
    const oldCount = sparkVersions.get(m.spark_version) ?? 0
    sparkVersions.set(m.spark_version, oldCount + 1)
  }
  recordTelemetry('spark_versions', point => {
    point.intField('round_index', roundIndex)
    let total = 0
    for (const [version, count] of sparkVersions.entries()) {
      point.intField(`v${version}`, count)
      total += count
    }
    point.intField('total', total)
  })

  return validMeasurements
}

export const fetchMeasurements = async cid => {
  const res = await fetch(
    `https://${encodeURIComponent(cid)}.ipfs.w3s.link?format=car`
  )
  if (!res.ok) {
    const msg = `Cannot fetch measurements ${cid}: ${res.status}\n${await res.text()}`
    throw new Error(msg)
  }
  const reader = await CarReader.fromIterable(res.body)
  const entries = exporter(cid, {
    async get (cid) {
      const block = await reader.get(cid)
      await validateBlock(block)
      return block.bytes
    }
  })
  for await (const entry of entries) {
    // Depending on size, entries might be packaged as `file` or `raw`
    // https://github.com/web3-storage/w3up/blob/e8bffe2ee0d3a59a977d2c4b7efe425699424e19/packages/upload-client/src/unixfs.js#L11
    if (entry.type === 'file' || entry.type === 'raw') {
      const bufs = []
      for await (const buf of entry.content()) {
        bufs.push(buf)
      }
      return parseMeasurements(Buffer.concat(bufs).toString())
    }
  }
  throw new Error('No measurements found')
}

export const parseMeasurements = str => {
  // Supports
  // - NDJSON (new format)
  // - JSON array on a single line (old format)
  const ret = str.split('\n').filter(Boolean).map(line => JSON.parse(line))
  if (ret.length === 1 && Array.isArray(ret[0])) return ret[0]
  return ret
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

/**
 * @param {Measurement} measurement
 */
const assertValidMeasurement = measurement => {
  assert(
    typeof measurement === 'object' && measurement !== null,
    'object required'
  )
  assert(ethers.utils.isAddress(measurement.participantAddress), 'valid participant address required')
  assert(typeof measurement.inet_group === 'string', 'valid inet group required')
  assert(typeof measurement.finished_at === 'number', 'field `finished_at` must be set to a number')
}

/**
 * @param {import('./typings').RawMeasurement} measurement
 * @return {import('./typings').RetrievalResult}
 */
export const getRetrievalResult = measurement => {
  if (measurement.timeout) return 'TIMEOUT'
  if (measurement.car_too_large) return 'CAR_TOO_LARGE'
  switch (measurement.status_code) {
    case 502: return 'BAD_GATEWAY'
    case 504: return 'GATEWAY_TIMEOUT'
  }
  if (measurement.status_code >= 300) return `ERROR_${measurement.status_code}`

  const ok = measurement.status_code >= 200 && typeof measurement.end_at === 'string'

  return ok ? 'OK' : 'UNKNOWN_ERROR'
}
