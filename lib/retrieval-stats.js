import createDebug from 'debug'
import * as hdr from 'hdr-histogram-js'

const debug = createDebug('spark:retrieval-stats')

await hdr.initWebAssembly()
const createHistogram = () => hdr.build({
  numberOfSignificantValueDigits: 5,
  bitBucketSize: 'packed',
  useWebAssembly: true
})

/**
 * @param {import('./typings').Measurement[]} measurements
 * @param {import('./typings').Point} telemetryPoint
 */
export const buildRetrievalStats = (measurements, telemetryPoint) => {
  const totalCount = measurements.length
  if (totalCount < 1) {
    telemetryPoint.intField('measurements', 0)
    telemetryPoint.intField('unique_tasks', 0)
    return
  }

  const uniqueTasksCount = countUniqueTasks(measurements)

  // Calculate aggregates per retrieval result

  // We are intentionally not initializing all possible keys here.
  // Example of omitted keys: UNDEFINED, ERROR_500 and ERROR_404.
  // The idea is that if we don't explicitly initialise them here and there is no measurement with
  // such retrieval result, then the Grafana dashboard will not show these results at all.
  /** @type {Record<import('./typings').RetrievalResult, number> */
  const resultBreakdown = {
    OK: 0,
    TIMEOUT: 0,
    CAR_TOO_LARGE: 0,
    BAD_GATEWAY: 0,
    GATEWAY_TIMEOUT: 0
  }

  const participants = new Set()
  const inetGroups = new Set()
  let downloadBandwidth = 0

  const ttfbHistogram = createHistogram()
  const durationHistogram = createHistogram()
  const sizeHistogram = createHistogram()

  for (const m of measurements) {
    // `retrievalResult` should be always set by lib/preprocess.js, so we should never encounter
    // `UNDEFINED` result. However, I am still handling that edge case for extra robustness.
    const result = m.retrievalResult ?? 'UNDEFINED'
    const oldCount = resultBreakdown[result] ?? 0
    resultBreakdown[result] = oldCount + 1

    participants.add(m.participantAddress)
    inetGroups.add(m.inet_group)

    // don't trust the checker to submit a positive integers
    // TODO: reject measurements with invalid values during the preprocess phase?
    const byteLength = typeof m.byte_length === 'number' && m.byte_length >= 0
      ? m.byte_length
      : undefined
    const startAt = parseDateTime(m.start_at)
    const firstByteAt = parseDateTime(m.first_byte_at)
    const endAt = parseDateTime(m.end_at)
    const ttfb = startAt && firstByteAt && (firstByteAt - startAt)
    const duration = startAt && endAt && (endAt - startAt)

    debug('size=%s ttfb=%s duration=%s valid? %s', byteLength, ttfb, duration, m.fraudAssessment === 'OK')
    if (byteLength !== undefined) {
      downloadBandwidth += byteLength
      sizeHistogram.recordValue(byteLength)
    }
    if (ttfb !== undefined) ttfbHistogram.recordValue(ttfb)
    if (duration !== undefined) durationHistogram.recordValue(duration)
  }
  const successRate = resultBreakdown.OK / totalCount

  telemetryPoint.intField('unique_tasks', uniqueTasksCount)
  telemetryPoint.floatField('success_rate', successRate)
  telemetryPoint.intField('participants', participants.size)
  telemetryPoint.intField('inet_groups', inetGroups.size)
  telemetryPoint.intField('measurements', totalCount)
  telemetryPoint.intField('download_bandwidth', downloadBandwidth)

  addHistogramToPoint(telemetryPoint, 'ttfb', ttfbHistogram)
  ttfbHistogram.destroy()

  addHistogramToPoint(telemetryPoint, 'duration', durationHistogram)
  durationHistogram.destroy()

  addHistogramToPoint(telemetryPoint, 'car_size', sizeHistogram)
  sizeHistogram.destroy()

  for (const [result, count] of Object.entries(resultBreakdown)) {
    telemetryPoint.floatField(`result_rate_${result}`, count / totalCount)
  }
}

const parseDateTime = (str) => {
  if (!str) return undefined
  const value = new Date(str)
  if (Number.isNaN(value.getTime())) return undefined
  return value
}

/**
 *
 * @param {import('./typings').Point} point
 * @param {string} fieldNamePrefix
 * @param {hdr.Histogram} histogram
 */
const addHistogramToPoint = (point, fieldNamePrefix, histogram) => {
  point.intField(`${fieldNamePrefix}_min`, histogram.minNonZeroValue)
  point.intField(`${fieldNamePrefix}_mean`, histogram.mean)
  point.intField(`${fieldNamePrefix}_max`, histogram.maxValue)
  for (const p of [10, 50, 90, 95]) {
    point.intField(`${fieldNamePrefix}_p${p}`, histogram.getValueAtPercentile(p))
  }
}

/**
 * @param {import('./typings').Measurement[]} measurements
 * @returns {number}
 */
const countUniqueTasks = (measurements) => {
  const getTaskId = (/** @type {import('./typings').Measurement} */m) =>
    `${m.cid}::${m.protocol}::${m.provider_address}`

  const uniqueTasks = new Set()
  for (const m of measurements) {
    const id = getTaskId(m)
    uniqueTasks.add(id)
  }

  return uniqueTasks.size
}
