import createDebug from 'debug'
import assert from 'node:assert'
import * as Sentry from '@sentry/node'
import { buildRetrievalStats, recordCommitteeSizes } from './retrieval-stats.js'

const debug = createDebug('spark:evaluate')

export const MAX_SCORE = 1_000_000_000_000_000n
export const MAX_SET_SCORES_PARTICIPANTS = 500

export class SetScoresBucket {
  constructor () {
    this.participants = []
    this.scores = []
  }

  add (participant, score) {
    this.participants.push(participant)
    this.scores.push(score)
  }

  get size () {
    return this.participants.length
  }
}

export const createSetScoresBuckets = participants => {
  const buckets = [new SetScoresBucket()]
  for (const [participant, score] of Object.entries(participants)) {
    let currentBucket = buckets[buckets.length - 1]
    if (currentBucket.size === MAX_SET_SCORES_PARTICIPANTS) {
      currentBucket = new SetScoresBucket()
      buckets.push(currentBucket)
    }
    currentBucket.add(participant, score)
  }
  return buckets
}

/**
 * @param {object} args
 * @param {any} args.rounds (TODO: replace `any` with proper type)
 * @param {BigInt} args.roundIndex
 * @param {any} args.ieContractWithSigner
 * @param {import('./spark-api').fetchRoundDetails} args.fetchRoundDetails,
 * @param {import('./typings').RecordTelemetryFn} args.recordTelemetry
 * @param {Console} args.logger
 */
export const evaluate = async ({
  rounds,
  roundIndex,
  ieContractWithSigner,
  fetchRoundDetails,
  recordTelemetry,
  logger
}) => {
  // Get measurements
  /** @type {Record<string, any>[]} */
  const measurements = rounds[roundIndex]?.measurements || []

  // Detect fraud

  const sparkRoundDetails = await fetchRoundDetails(ieContractWithSigner.address, roundIndex, recordTelemetry)
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  const started = new Date()

  const fraudDetectionStats = await runFraudDetection(roundIndex, measurements, sparkRoundDetails)
  const honestMeasurements = measurements.filter(m => m.fraudAssessment === 'OK')

  // Calculate reward shares
  const participants = {}
  let sum = 0n
  for (const measurement of honestMeasurements) {
    if (!participants[measurement.participantAddress]) {
      participants[measurement.participantAddress] = 0n
    }
    participants[measurement.participantAddress] += 1n
  }
  for (const [participantAddress, participantTotal] of Object.entries(participants)) {
    const score = participantTotal *
      MAX_SCORE /
      BigInt(honestMeasurements.length)
    participants[participantAddress] = score
    sum += score
  }

  if (sum < MAX_SCORE) {
    const delta = MAX_SCORE - sum
    const score = (participants['0x000000000000000000000000000000000000dEaD'] ?? 0n) + delta
    participants['0x000000000000000000000000000000000000dEaD'] = score
    logger.log('EVALUATE ROUND %s: added %s as rounding to MAX_SCORE', roundIndex, delta)
  }

  // Calculate aggregates per fraud detection outcome
  // This is used for logging and telemetry
  /** @type {Record<import('./typings').FraudAssesment, number> */
  const fraudAssessments = {
    OK: 0,
    INVALID_TASK: 0,
    DUP_INET_GROUP: 0,
    TOO_MANY_TASKS: 0
  }
  for (const m of measurements) {
    fraudAssessments[m.fraudAssessment] = (fraudAssessments[m.fraudAssessment] ?? 0) + 1
  }
  logger.log(
    'EVALUATE ROUND %s: Evaluated %s measurements, found %s honest entries.\n%o',
    roundIndex,
    measurements.length,
    honestMeasurements.length,
    fraudAssessments
  )
  logger.log(
    'EVALUATE ROUND %s: Success rate of winning per-inet-group task reward:\n%o',
    roundIndex,
    fraudDetectionStats.groupWinning
  )

  const fraudDetectionDuration = new Date() - started

  // Submit scores to IE

  const totalScore = Object.values(participants).reduce((sum, val) => sum + val, 0n)
  logger.log(
    'EVALUATE ROUND %s: Invoking IE.setScores(); number of participants: %s, total score: %s',
    roundIndex,
    Object.keys(participants).length,
    totalScore === 1000000000000000n ? '100%' : totalScore
  )

  const start = new Date()
  const buckets = createSetScoresBuckets(participants)
  for (const [bucketIndex, bucket] of Object.entries(buckets)) {
    const tx = await ieContractWithSigner.setScores(
      roundIndex,
      bucket.participants,
      bucket.scores
    )
    logger.log(
      'EVALUATE ROUND %s: IE.setScores() TX hash: %s CALL: %s/%s',
      roundIndex,
      tx.hash,
      bucketIndex,
      buckets.length
    )
  }
  const setScoresDuration = new Date() - start

  // Clean up
  delete rounds[roundIndex]

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_participants', Object.keys(participants).length)
    point.intField('total_measurements', measurements.length)
    point.intField('honest_measurements', honestMeasurements.length)
    point.intField('set_scores_duration_ms', setScoresDuration)
    point.intField('fraud_detection_duration_ms', fraudDetectionDuration)

    for (const [type, count] of Object.entries(fraudAssessments)) {
      point.intField(`measurements_${type}`, count)
    }

    for (const [key, value] of Object.entries(fraudDetectionStats.groupWinning)) {
      // At some point, we had a bug in the evaluation pipeline which caused some group winning
      // rates to be undefined or NaN. InfluxDB client rejected such values, which triggered
      // unhandled error. Let's avoid that situation by ignoring such data points.
      try {
        point.floatField(`group_winning_${key}`, value)
      } catch (err) {
        console.error(err)
        Sentry.captureException(err, { extra: { roundIndex } })
      }
    }
  })

  try {
    recordTelemetry('retrieval_stats_honest', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(honestMeasurements, point)
    })
  } catch (err) {
    console.error('Cannot record retrieval stats (honest).', err)
    Sentry.captureException(err)
  }

  try {
    recordTelemetry('retrieval_stats_all', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(measurements, point)
    })
  } catch (err) {
    console.error('Cannot record retrieval stats (all).', err)
    Sentry.captureException(err)
  }

  try {
    recordTelemetry('committees', (point) => {
      point.intField('round_index', roundIndex)
      recordCommitteeSizes(measurements, point)
    })
  } catch (err) {
    console.error('Cannot record committees.', err)
    Sentry.captureException(err)
  }
}

/**
 * @param {number} roundIndex
 * @param {import('./typings').Measurement[]} measurements
 * @param {import('./typings').RoundDetails} sparkRoundDetails
 * @returns {Promise<import('./typings').FraudDetectionStats>}
 */
export const runFraudDetection = async (roundIndex, measurements, sparkRoundDetails) => {
  //
  // 1. Filter out measurements not belonging to any valid task in this round
  //    or missing some of the required fields like `inet_group`
  //
  for (const m of measurements) {
    // sanity checks to get nicer errors if we forget to set required fields in unit tests
    assert(typeof m.inet_group === 'string', 'missing inet_group')
    assert(typeof m.finished_at === 'number', 'missing finished_at')

    const isValidTask = sparkRoundDetails.retrievalTasks.some(t =>
      t.cid === m.cid && t.providerAddress === m.provider_address & t.protocol === m.protocol
    )
    if (!isValidTask) {
      m.fraudAssessment = 'INVALID_TASK'
    }
  }

  //
  // 2. Reward only one participant in each inet group
  //
  /** @type {Map<string, import('./typings').Measurement[]>} */
  const taskGroups = new Map()
  for (const m of measurements) {
    if (m.fraudAssessment) continue

    const key = `${m.inet_group}::${m.cid}::${m.provider_address}`
    let group = taskGroups.get(key)
    if (!group) {
      group = []
      taskGroups.set(key, group)
    }

    group.push(m)
  }

  const getHash = async (/** @type {import('./typings').Measurement} */ m) => {
    const bytes = await globalThis.crypto.subtle.digest('SHA-256', Buffer.from(new Date(m.finished_at).toISOString()))
    return Buffer.from(bytes).toString('hex')
  }

  for (const [key, groupMeasurements] of taskGroups.entries()) {
    debug('Evaluating measurements in group %s', key)

    // Pick one measurement to reward and mark all others as not eligible for rewards
    // The difficult part: how to choose a measurement randomly but also fairly, so
    // that each measurement has the same chance of being picked for the reward?
    // We also want the selection algorithm to be deterministic.
    //
    // Note that we cannot rely on participant addresses because it's super easy
    // for node operators to use a different address for each measurement.
    //
    // Let's start with a simple algorithm we can later tweak:
    // 1. Hash the `finished_at` timestamp recorded by the server
    // 2. Pick the measurement with the lowest hash value
    // This relies on the fact that the hash function has a random distribution.
    // We are also making the assumption that each measurement has a distinct `finished_at` field.

    const chosen = { m: groupMeasurements[0], h: await getHash(groupMeasurements[0]) }
    debug('  m[0] pa: %s h: %s', chosen.m.participantAddress, chosen.h)
    for (let i = 1; i < groupMeasurements.length; i++) {
      const m = groupMeasurements[i]
      const h = await getHash(m)
      debug('  m[%s] pa: %s h: %s', i, m.participantAddress, h)
      if (h < chosen.h) {
        debug('  ^^ new winner')
        chosen.m = m
        chosen.h = h
      }
    }

    for (const m of groupMeasurements) {
      m.fraudAssessment = m === chosen.m ? 'OK' : 'DUP_INET_GROUP'
    }
  }

  //
  // 3. Reward only first TN measurements
  //
  const tasksPerNode = new Map()
  for (const m of measurements) {
    if (m.fraudAssessment && m.fraudAssessment !== 'OK') continue
    const node = `${m.inet_group}::${m.participantAddress}`
    tasksPerNode.set(node, (tasksPerNode.get(node) ?? 0) + 1)
    if (tasksPerNode.get(node) > sparkRoundDetails.maxTasksPerNode) {
      m.fraudAssessment = 'TOO_MANY_TASKS'
    }
  }

  if (debug.enabled) {
    for (const m of measurements) {
      // Print round & participant address & CID together to simplify lookup when debugging
      // Omit the `m` object from the format string to get nicer formatting
      debug(
        'FRAUD ASSESSMENT for round=%s client=%s cid=%s',
        roundIndex,
        m.participantAddress,
        m.cid,
        m)
    }
  }

  return {
    groupWinning: calculateInetGroupSuccessRates(taskGroups)
  }
}

/**
 * For each participant, calculate how many valid measurements were rewarded or
 * rejected by our inet_group algorithm. Multiple measurements submitted for the same task
 * are considered as one measurement.
 *
 * @param {Map<string, import('./typings').Measurement[]>} taskGroups
 * @returns {import('./typings').GroupWinningStats}
 */
const calculateInetGroupSuccessRates = (taskGroups) => {
  debug('calculateInetGroupSuccessRates')

  /** @type {Map<string, {won: number, lost: number}> */
  const participantStats = new Map()

  for (const groupMeasurements of taskGroups.values()) {
    debug(
      '  measurements for inet_group=%s cid=%s',
      groupMeasurements[0].inet_group,
      groupMeasurements[0].cid
    )
    // First, find participants that submitted some valid measurements for this task
    // and find out whether they were rewarded or not

    /** @type {Map<string, boolean>} */
    const taskParticipants = new Map()
    for (const m of groupMeasurements) {
      if (m.fraudAssessment === 'OK') {
        taskParticipants.set(m.participantAddress, true)
        debug('    %s -> won', m.participantAddress)
      } else if (m.fraudAssessment === 'DUP_INET_GROUP') {
        if (!taskParticipants.has(m.participantAddress)) {
          taskParticipants.set(m.participantAddress, false)
          debug('    %s -> lost', m.participantAddress)
        } else {
          debug('    %s -> skip (redundant)', m.participantAddress)
        }
      }
    }

    // Next, update each participant's winning score
    for (const [participantAddress, wasPickedForReward] of taskParticipants.entries()) {
      let s = participantStats.get(participantAddress)
      if (!s) {
        s = { won: 0, lost: 0 }
        participantStats.set(participantAddress, s)
      }

      s[wasPickedForReward ? 'won' : 'lost']++
    }
  }

  if (!participantStats.size) {
    console.log('This round has no participants with OK or DUP_INET_GROUP measurements.')
    return { min: 1, mean: 1, max: 1 }
  }

  // Finally, calculate the aggregate statistics
  const result = { min: 1.0, max: 0.0, mean: undefined }
  let sum = 0
  for (const [pa, s] of participantStats.entries()) {
    const successRate = s.won / (s.won + s.lost)
    if (successRate < result.min) result.min = successRate
    if (successRate > result.max) result.max = successRate
    sum += successRate
    debug('Winning rate for %s: won %s lost %s rate %s', pa, s.won, s.lost, successRate)
  }
  result.mean = sum / participantStats.size

  debug('Winning rates stats: %o', result)
  return result
}
