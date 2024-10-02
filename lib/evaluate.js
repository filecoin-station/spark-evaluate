import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import assert from 'node:assert'
import { getRandomnessForSparkRound } from './drand-client.js'
import { updatePublicStats } from './public-stats.js'
import { buildRetrievalStats, getTaskId, recordCommitteeSizes } from './retrieval-stats.js'
import { getTasksAllowedForStations } from './tasker.js'
import { groupMeasurementsToCommittees } from './committee.js'
import { ethers } from 'ethers'
import pRetry from 'p-retry'

/** @import {Measurement} from './preprocess.js' */

const debug = createDebug('spark:evaluate')

export const MAX_SCORE = 1_000_000_000_000_000n
export const MAX_SET_SCORES_PARTICIPANTS = 700
export const REQUIRED_COMMITTEE_SIZE = 30

/**
 * @param {object} args
 * @param {import('./round.js').RoundData} args.round
 * @param {bigint} args.roundIndex
 * @param {number} [args.requiredCommitteeSize]
 * @param {any} args.ieContract
 * @param {import('ethers').Signer} args.signer
 * @param {typeof fetch} [args.fetch]
 * @param {import('./spark-api.js').fetchRoundDetails} args.fetchRoundDetails,
 * @param {import('./typings.js').RecordTelemetryFn} args.recordTelemetry
 * @param {import('./typings.js').CreatePgClient} [args.createPgClient]
 * @param {Pick<Console, 'log' | 'error'>} args.logger
 */
export const evaluate = async ({
  round,
  roundIndex,
  requiredCommitteeSize,
  ieContract,
  signer,
  fetch = globalThis.fetch,
  fetchRoundDetails,
  recordTelemetry,
  createPgClient,
  logger
}) => {
  requiredCommitteeSize ??= REQUIRED_COMMITTEE_SIZE

  // Get measurements
  /** @type {Measurement[]} */
  const measurements = round.measurements || []

  // Detect fraud

  const sparkRoundDetails = await fetchRoundDetails(await ieContract.getAddress(), roundIndex, recordTelemetry)
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)

  const started = Date.now()

  const { committees } = await runFraudDetection({
    roundIndex,
    measurements,
    sparkRoundDetails,
    requiredCommitteeSize,
    logger
  })
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
  /** @type {Partial<Record<import('./typings.js').FraudAssesment, number>>} */
  const fraudAssessments = {
    OK: 0,
    TASK_NOT_IN_ROUND: 0,
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

  const fraudDetectionDuration = Date.now() - started

  // Submit scores to IE

  const totalScore = Object.values(participants).reduce((sum, val) => sum + val, 0n)
  logger.log(
    'EVALUATE ROUND %s: Invoking IE.setScores(); number of participants: %s, total score: %s',
    roundIndex,
    Object.keys(participants).length,
    totalScore === 1000000000000000n ? '100%' : totalScore
  )

  const start = Date.now()
  const addresses = Object.keys(participants)
  const values = Object.values(participants)
  const digest = ethers.solidityPackedKeccak256(
    ['address[]', 'uint256[]'],
    [addresses, values]
  )
  const signed = await signer.signMessage(digest)
  const { v, r, s } = ethers.Signature.from(signed)

  try {
    const res = await fetch('https://spark-rewards.fly.dev/scores', {
      method: 'POST',
      body: JSON.stringify({
        participants: addresses,
        values: values.map(n => String(n)),
        signature: { v, r, s }
      })
    })
    if (res.status !== 200) {
      throw new Error(
        `spark-rewards responded with ${res.status}: ${await res.text().catch(err => err)}`
      )
    }
    logger.log('EVALUATE ROUND %s: POST /scores', roundIndex)
  } catch (err) {
    logger.error('CANNOT SUBMIT SCORES FOR ROUND %s: %s',
      roundIndex,
      err
    )
    Sentry.captureException(err, { extra: { roundIndex } })
  }
  const setScoresDuration = Date.now() - start

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_participants', Object.keys(participants).length)
    point.intField('total_measurements', measurements.length)
    point.intField('total_nodes', countUniqueNodes(measurements))
    point.intField('honest_measurements', honestMeasurements.length)
    point.intField('set_scores_duration_ms', setScoresDuration)
    point.intField('fraud_detection_duration_ms', fraudDetectionDuration)

    for (const [type, count] of Object.entries(fraudAssessments)) {
      point.intField(`measurements_${type}`, count)
    }
  })

  const ignoredErrors = []

  try {
    recordTelemetry('retrieval_stats_honest', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(honestMeasurements, point)
    })
  } catch (err) {
    console.error('Cannot record retrieval stats (honest).', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  try {
    recordTelemetry('retrieval_stats_all', (point) => {
      point.intField('round_index', roundIndex)
      buildRetrievalStats(measurements, point)
    })
  } catch (err) {
    console.error('Cannot record retrieval stats (all).', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  try {
    recordTelemetry('committees', (point) => {
      point.intField('round_index', roundIndex)
      point.intField('committees_all', committees.length)
      point.intField('committees_too_small',
        committees
          .filter(c => c.evaluation?.retrievalResult === 'COMMITTEE_TOO_SMALL')
          .length
      )
      recordCommitteeSizes(committees, point)
    })
  } catch (err) {
    console.error('Cannot record committees.', err)
    ignoredErrors.push(err)
    Sentry.captureException(err)
  }

  if (createPgClient) {
    try {
      await updatePublicStats({
        createPgClient,
        committees,
        honestMeasurements,
        allMeasurements: measurements,
        findDealClients: (minerId, cid) => sparkRoundDetails.retrievalTasks
          .find(t => t.cid === cid && t.minerId === minerId)?.clients
      })
    } catch (err) {
      console.error('Cannot update public stats.', err)
      ignoredErrors.push(err)
      Sentry.captureException(err)
    }
  }

  return { ignoredErrors }
}

/**
 * @param {object} args
 * @param {bigint} args.roundIndex
 * @param {Measurement[]} args.measurements
 * @param {import('./typings.js').RoundDetails} args.sparkRoundDetails
 * @param {number} args.requiredCommitteeSize
 * @param {Pick<Console, 'log' | 'error'>} args.logger
 */
export const runFraudDetection = async ({
  roundIndex,
  measurements,
  sparkRoundDetails,
  requiredCommitteeSize,
  logger
}) => {
  const randomness = await pRetry(
    () => getRandomnessForSparkRound(Number(sparkRoundDetails.startEpoch)),
    {
      async onFailedAttempt (error) {
        console.warn(error)
        console.warn('Drand request failed. Retrying...')
      }
    }
  )

  const taskBuildingStarted = Date.now()
  const tasksAllowedForStations = await getTasksAllowedForStations(
    sparkRoundDetails,
    randomness,
    measurements
  )

  logger.log(
    'EVALUATE ROUND %s: built per-node task lists in %sms [Tasks=%s;TN=%s;Nodes=%s]',
    roundIndex,
    Date.now() - taskBuildingStarted,
    sparkRoundDetails.retrievalTasks.length,
    sparkRoundDetails.maxTasksPerNode,
    tasksAllowedForStations.size
  )

  //
  // 0. Filter out measurements missing required fields.
  //
  for (const m of measurements) {
    if (!m.indexerResult) {
      m.fraudAssessment = 'IPNI_NOT_QUERIED'
    }
  }

  //
  // 1. Filter out measurements not belonging to any valid task in this round
  //    or missing some of the required fields like `inet_group`
  //
  for (const m of measurements) {
    if (m.fraudAssessment) continue

    // sanity checks to get nicer errors if we forget to set required fields in unit tests
    assert(typeof m.inet_group === 'string', 'missing inet_group')
    assert(typeof m.finished_at === 'number', 'missing finished_at')

    const isValidTask = sparkRoundDetails.retrievalTasks.some(
      t => t.cid === m.cid && t.minerId === m.minerId
    )
    if (!isValidTask) {
      m.fraudAssessment = 'TASK_NOT_IN_ROUND'
      continue
    }

    const isValidTaskForNode = tasksAllowedForStations.get(m.stationId).some(
      t => t.cid === m.cid && t.minerId === m.minerId
    )
    if (!isValidTaskForNode) {
      m.fraudAssessment = 'TASK_WRONG_NODE'
    }
  }

  //
  // 2. Accept only maxTasksPerNode measurements from each inet group
  //
  /** @type {Map<string, Measurement[]>} */
  const inetGroups = new Map()
  for (const m of measurements) {
    if (m.fraudAssessment) continue

    const key = m.inet_group
    let group = inetGroups.get(key)
    if (!group) {
      group = []
      inetGroups.set(key, group)
    }

    group.push(m)
  }

  const getHash = async (/** @type {Measurement} */ m) => {
    const bytes = await globalThis.crypto.subtle.digest('SHA-256', Buffer.from(new Date(m.finished_at).toISOString()))
    return Buffer.from(bytes).toString('hex')
  }

  for (const [key, groupMeasurements] of inetGroups.entries()) {
    debug('Evaluating measurements from inet group %s', key)

    // Pick one measurement per task to reward and mark all others as not eligible for rewards.
    // We also want to reward at most `maxTasksPerNode` measurements in each inet group.
    //
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
    //
    const measurementsWithHash = await Promise.all(
      groupMeasurements.map(async (m) => ({ m, h: await getHash(m) }))
    )
    // Sort measurements using the computed hash
    measurementsWithHash.sort((a, b) => a.h.localeCompare(b.h, 'en'))

    const tasksSeen = new Set()
    for (const { m, h } of measurementsWithHash) {
      const taskId = getTaskId(m)

      if (tasksSeen.has(taskId)) {
        debug('  pa: %s h: %s task: %s - task was already rewarded', m.participantAddress, h, taskId)
        m.fraudAssessment = 'DUP_INET_GROUP'
        continue
      }

      if (tasksSeen.size >= sparkRoundDetails.maxTasksPerNode) {
        debug('  pa: %s h: %s task: %s - already rewarded max tasks', m.participantAddress, h, taskId)
        m.fraudAssessment = 'TOO_MANY_TASKS'
        continue
      }

      tasksSeen.add(taskId)
      m.fraudAssessment = 'OK'
      debug('  pa: %s h: %s task: %s - REWARD', m.participantAddress, h, taskId)
    }
  }

  //
  // 3. Group measurements to per-task committees and find majority result
  //

  // PERFORMANCE: Avoid duplicating the array of measurements because there are
  // hundreds of thousands of them. All the function groupMeasurementsToCommittees
  // needs is to iterate over the accepted measurements once.
  const iterateAcceptedMeasurements = function * () {
    for (const m of measurements) {
      if (m.fraudAssessment !== 'OK') continue
      yield m
    }
  }

  const committees = groupMeasurementsToCommittees(iterateAcceptedMeasurements())

  for (const c of committees.values()) {
    c.evaluate({ requiredCommitteeSize })
  }

  if (debug.enabled) {
    for (const m of measurements) {
      // Print round & participant address & CID together to simplify lookup when debugging
      // Omit the `m` object from the format string to get nicer formatting
      debug(
        'FRAUD ASSESSMENT for round=%s client=%s cid=%s minerId=%s',
        roundIndex,
        m.participantAddress,
        m.cid,
        m.minerId,
        m)
    }
  }

  return { committees: Array.from(committees.values()) }
}

/**
 * @param {Measurement[]} measurements
 */
const countUniqueNodes = (measurements) => {
  const nodes = new Set()
  for (const m of measurements) {
    const key = `${m.inet_group}::${m.participantAddress}`
    nodes.add(key)
  }
  return nodes.size
}
