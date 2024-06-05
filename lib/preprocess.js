import assert from 'node:assert'
import { ethers } from 'ethers'
import { ethAddressFromDelegated } from '@glif/filecoin-address'
import { CarReader } from '@ipld/car'
import { validateBlock } from '@web3-storage/car-block-validator'
import { recursive as exporter } from 'ipfs-unixfs-exporter'
import createDebug from 'debug'
import pRetry from 'p-retry'

const debug = createDebug('spark:preprocess')

export class Measurement {
  /**
   * @param {import('./typings.js').RawMeasurement} m
   * @param {(string) => string} pointerize
   */
  constructor (m, pointerize = (v) => v) {
    this.participantAddress = pointerize(parseParticipantAddress(m.participant_address))
    this.retrievalResult = pointerize(getRetrievalResult(m))
    this.cid = pointerize(m.cid)
    this.minerId = pointerize(m.miner_id)
    // Note: providerId is recorded by spark-publish but we don't use it for evaluations yet
    this.providerId = pointerize(m.provider_id)
    this.spark_version = pointerize(m.spark_version)
    /** @type {import('./typings.js').FraudAssesment} */
    this.fraudAssessment = null
    this.inet_group = pointerize(m.inet_group)
    this.finished_at = parseDateTime(m.finished_at)
    this.provider_address = pointerize(m.provider_address)
    this.protocol = pointerize(m.protocol)
    this.byte_length = m.byte_length
    this.start_at = parseDateTime(m.start_at)
    this.first_byte_at = parseDateTime(m.first_byte_at)
    this.end_at = parseDateTime(m.end_at)
    this.status_code = m.status_code
    this.indexerResult = pointerize(m.indexer_result)
    this.stationId = pointerize(m.station_id)
  }
}

const parseDateTime = (str) => {
  if (!str) return undefined
  const value = new Date(str)
  if (Number.isNaN(value.getTime())) return undefined
  return value.getTime()
}

export const preprocess = async ({
  round,
  cid,
  roundIndex,
  fetchMeasurements,
  recordTelemetry,
  logger,
  fetchRetries = 14
}) => {
  const start = Date.now()
  /** @type import('./typings.js').RawMeasurement[] */
  const measurements = await pRetry(
    attempt => fetchMeasurements(cid, { noCache: attempt > 1 }),
    {
      retries: fetchRetries,
      onFailedAttempt: err => {
        if (!fetchRetries) return
        console.error(err)
        console.error(`Retrying ${cid} ${err.retriesLeft} more times`)
      }
    }
  )

  const fetchDuration = Date.now() - start
  const validMeasurements = measurements
    // eslint-disable-next-line camelcase
    .map(measurement => {
      try {
        return new Measurement(measurement, round.pointerize)
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
        'RETRIEVAL RESULT for round=%s client=%s cid=%s minerId=%s: %s',
        roundIndex,
        measurement.participantAddress,
        measurement.cid,
        measurement.minerId,
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

  round.measurements.push(...validMeasurements)

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

/**
 * @param {string} cid
 * @param {object} options
 * @param {AbortSignal} [options.signal]
 * @param {boolean} [options.noCache]
 * @returns {Promise<import('./typings.js').RawMeasurement[]>}
 */
export const fetchMeasurements = async (cid, { signal, noCache = false } = {}) => {
  const res = await fetch(
    `https://${encodeURIComponent(cid)}.ipfs.w3s.link?format=car`,
    {
      signal,
      headers: {
        'Cache-Control': noCache ? 'no-cache' : 'default'
      }
    }
  )
  if (!res.ok) {
    const msg = `Cannot fetch measurements ${cid}: ${res.status}\n${await res.text()}`
    throw new Error(msg)
  }
  const reader = await CarReader.fromIterable(res.body)
  const entries = exporter(cid, {
    async get (blockCid) {
      signal?.throwIfAborted()
      // The cast to `any` is a workaround for the following TypeScript error
      // The types of 'toV0()[Symbol.toStringTag]' are incompatible between these types.
      //   Type 'string' is not assignable to type '"CID"'
      const block = await reader.get(/** @type {any} */(blockCid))
      try {
        await validateBlock(block)
      } catch (err) {
        throw new Error(
          `Invalid block ${blockCid} of root ${cid}`, { cause: err }
        )
      }
      return block.bytes
    }
  })
  for await (const entry of entries) {
    signal?.throwIfAborted()
    // Depending on size, entries might be packaged as `file` or `raw`
    // https://github.com/web3-storage/w3up/blob/e8bffe2ee0d3a59a977d2c4b7efe425699424e19/packages/upload-client/src/unixfs.js#L11
    if (entry.type === 'file' || entry.type === 'raw') {
      const bufs = []
      for await (const buf of entry.content()) {
        signal?.throwIfAborted()
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
  assert(ethers.isAddress(measurement.participantAddress), 'valid participant address required')
  assert(typeof measurement.inet_group === 'string', 'valid inet group required')
  assert(typeof measurement.finished_at === 'number', 'field `finished_at` must be set to a number')
  if (measurement.stationId) {
    assert(
      typeof measurement.stationId === 'string' &&
      measurement.stationId.match(/^[0-9a-fA-F]{88}$/),
      'stationId must be a hex string with 88 characters'
    )
  }
}

/**
 * @param {import('./typings.js').RawMeasurement} measurement
 * @return {import('./typings.js').RetrievalResult}
 */
export const getRetrievalResult = measurement => {
  switch (measurement.indexer_result) {
    case 'OK':
    case 'HTTP_NOT_ADVERTISED':
      break
    case undefined:
    case null:
      return 'IPNI_NOT_QUERIED'
    default:
      return `IPNI_${measurement.indexer_result}`
  }

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
