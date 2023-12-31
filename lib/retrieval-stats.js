import createDebug from 'debug'

const debug = createDebug('spark:retrieval-stats')

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
  const tasksPerNode = new Map()
  let downloadBandwidth = 0

  const ttfbValues = []
  const durationValues = []
  const sizeValues = []

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
    const startAt = m.start_at
    const firstByteAt = m.first_byte_at
    const endAt = m.end_at
    const ttfb = startAt && firstByteAt && (firstByteAt - startAt)
    const duration = startAt && endAt && (endAt - startAt)

    debug('size=%s ttfb=%s duration=%s status=%s valid? %s', byteLength, ttfb, duration, m.status_code, m.fraudAssessment === 'OK')
    if (byteLength !== undefined && m.status_code === 200) {
      downloadBandwidth += byteLength
      sizeValues.push(byteLength)
    }
    if (ttfb !== undefined && ttfb > 0 && m.status_code === 200) ttfbValues.push(ttfb)
    if (duration !== undefined && duration > 0) durationValues.push(duration)

    const node = `${m.inet_group}::${m.participantAddress}`
    tasksPerNode.set(node, (tasksPerNode.get(node) ?? 0) + 1)
  }
  const successRate = resultBreakdown.OK / totalCount

  telemetryPoint.intField('unique_tasks', uniqueTasksCount)
  telemetryPoint.floatField('success_rate', successRate)
  telemetryPoint.intField('participants', participants.size)
  telemetryPoint.intField('inet_groups', inetGroups.size)
  telemetryPoint.intField('measurements', totalCount)
  telemetryPoint.intField('download_bandwidth', downloadBandwidth)

  addHistogramToPoint(telemetryPoint, ttfbValues, 'ttfb_')
  addHistogramToPoint(telemetryPoint, durationValues, 'duration_')
  addHistogramToPoint(telemetryPoint, sizeValues, 'car_size_')
  addHistogramToPoint(
    telemetryPoint,
    Array.from(tasksPerNode.values()),
    'tasks_per_node_'
  )

  for (const [result, count] of Object.entries(resultBreakdown)) {
    telemetryPoint.floatField(`result_rate_${result}`, count / totalCount)
  }
}

/**
 *
 * @param {import('./typings').Point} point
 * @param {string} fieldNamePrefix
 * @param {number[]} values
 */
const addHistogramToPoint = (point, values, fieldNamePrefix = '') => {
  const count = values.length
  if (count < 1) return
  values.sort((a, b) => a - b)
  point.intField(`${fieldNamePrefix}min`, values[0])
  point.intField(`${fieldNamePrefix}mean`, values.reduce((sum, v) => sum + BigInt(v), 0n) / BigInt(count))
  point.intField(`${fieldNamePrefix}max`, values[count - 1])
  for (const p of [1, 5, 10, 50, 90, 95, 99]) {
    point.intField(`${fieldNamePrefix}p${p}`, getValueAtPercentile(values, p))
  }
}

export const getValueAtPercentile = (values, p) => {
  // See https://www.calculatorsoup.com/calculators/statistics/percentile-calculator.php
  // 1. Arrange n number of data points in ascending order: x1, x2, x3, ... xn
  const n = values.length

  // 2. Calculate the rank r for the percentile p you want to find: r = (p/100) * (n - 1) + 1
  const r = (n - 1) * p / 100 + 1

  // 3. If r is an integer then the data value at location r, xr, is the percentile p: p = xr
  if (Number.isInteger(r)) {
    // Remember that we are indexing from 0, i.e. x1 is stored in values[0]
    return values[r - 1]
  }

  const ri = Math.floor(r)
  const rf = r - ri

  // 4. If r is not an integer, p is interpolated using ri, the integer part of r,
  // and rf, the fractional part of r:
  // p = xri + rf * (xri+1 - xri)
  return values[ri - 1] + rf * (values[ri] - values[ri - 1])
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

/**
 * @param {import('./typings').Measurement[]} measurements
 * @param {import('./typings').Point} telemetryPoint
 */
export const recordCommitteeSizes = (measurements, point) => {
  /** @type {Map<string, {
   * subnets: Set<string>;
   * participants: Set<string>;
   * nodes: Set<string>;
   * measurements: number
   * }>} */
  const tasks = new Map()
  for (const m of measurements) {
    const key = `${m.cid}::${m.protocol}::${m.provider_address}`
    let data = tasks.get(key)
    if (!data) {
      data = {
        subnets: new Set(),
        participants: new Set(),
        nodes: new Set(),
        measurements: 0
      }
      tasks.set(key, data)
    }
    data.subnets.add(m.inet_group)
    data.participants.add(m.participantAddress)
    // We don't have Station instance identifier in the measurement.
    // The pair (inet_group, participant_address) is a good approximation.
    data.nodes.add(`${m.inet_group}::${m.participantAddress}`)
    data.measurements++
  }

  /** @type {Array<number>} */
  const subnetCounts = []
  /** @type {Array<number>} */
  const participantCounts = []
  /** @type {Array<number>} */
  const nodeCounts = []
  /** @type {Array<number>} */
  const measurementCounts = []
  for (const { subnets, participants, nodes, measurements } of tasks.values()) {
    subnetCounts.push(subnets.size)
    participantCounts.push(participants.size)
    nodeCounts.push(nodes.size)
    measurementCounts.push(measurements)
  }

  addHistogramToPoint(point, subnetCounts, 'subnets_')
  addHistogramToPoint(point, participantCounts, 'participants_')
  addHistogramToPoint(point, nodeCounts, 'nodes_')
  addHistogramToPoint(point, measurementCounts, 'measurements_')
}
