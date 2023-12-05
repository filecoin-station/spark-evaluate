import createDebug from 'debug'
// import * as Sentry from '@sentry/node'
// import { buildRetrievalStats } from './retrieval-stats.js'

const debug = createDebug('spark:evaluate')

export const MAX_SCORE = 1_000_000_000_000_000n

export const storeRoundDetails = async ({
  fetchRoundDetails,
  roundIndex,
  ieContractWithSigner,
  recordTelemetry,
  db
}) => {
  const sparkRoundDetails = await fetchRoundDetails(
    ieContractWithSigner.address,
    roundIndex,
    recordTelemetry
  )
  // Omit the roundDetails object from the format string to get nicer formatting
  debug('ROUND DETAILS for round=%s', roundIndex, sparkRoundDetails)
  const insertRetrievalTask = db.prepare(`
    INSERT INTO retrieval_tasks
    (round_index, cid, provider_address, protocol)
    VALUES
    (@roundIndex, @cid, @providerAddress, @protocol)
  `)
  await db.transaction(tasks => {
    for (const task of tasks) {
      insertRetrievalTask.run({
        roundIndex,
        ...task
      })
    }
  })(sparkRoundDetails.retrievalTasks)
}

/**
 * @param {object} args
 * @param {import('sqlite').Database} args.db
 * @param {BigInt} args.roundIndex
 * @param {any} args.ieContractWithSigner
 * @param {import('./spark-api').fetchRoundDetails} args.fetchRoundDetails,
 * @param {import('./typings').RecordTelemetryFn} args.recordTelemetry
 * @param {Console} args.logger
 */
export const evaluate = async ({
  db,
  roundIndex,
  ieContractWithSigner,
  fetchRoundDetails,
  recordTelemetry,
  logger
}) => {
  await storeRoundDetails({
    fetchRoundDetails,
    roundIndex,
    ieContractWithSigner,
    recordTelemetry,
    db
  })

  // Detect fraud

  const fraudDetectionResults = await runFraudDetection(
    roundIndex,
    db
  )

  // Calculate aggregates per fraud detection outcome
  // This is used for logging and telemetry
  /** @type {import('./typings').FraudAssesmentCounts} */
  const fraudAssessmentCounts = {
    OK: 0n,
    INVALID_TASK: 0n,
    DUP_INET_GROUP: 0n
  }
  for (const counts of Object.values(fraudDetectionResults)) {
    fraudAssessmentCounts.OK += counts.OK
    fraudAssessmentCounts.INVALID_TASK += counts.INVALID_TASK
    fraudAssessmentCounts.DUP_INET_GROUP += counts.DUP_INET_GROUP
  }

  // Calculate reward shares
  let sum = 0n
  const participants = {}
  for (const [participantAddress, counts] of Object.entries(fraudDetectionResults)) {
    if (counts.OK === 0n) continue
    const score = counts.OK *
      MAX_SCORE /
      BigInt(fraudAssessmentCounts.OK)
    participants[participantAddress] = score
    sum += score
  }

  if (sum < MAX_SCORE) {
    const delta = MAX_SCORE - sum
    const score = (participants['0x000000000000000000000000000000000000dEaD'] ?? 0n) + delta
    participants['0x000000000000000000000000000000000000dEaD'] = score
    logger.log('EVALUATE ROUND %s: added %s as rounding to MAX_SCORE', roundIndex, delta)
  }
  logger.log(
    'EVALUATE ROUND %s: Evaluated %s measurements, found %s honest entries.\n%o',
    roundIndex,
    fraudAssessmentCounts.OK +
      fraudAssessmentCounts.INVALID_TASK +
      fraudAssessmentCounts.DUP_INET_GROUP,
    fraudAssessmentCounts.OK,
    fraudAssessmentCounts
  )
  // TODO
  // logger.log(
  //   'EVALUATE ROUND %s: Success rate of winning per-inet-group task reward:\n%o',
  //   roundIndex,
  //   fraudDetectionStats.groupWinning
  // )

  // Submit scores to IE

  const totalScore = Object.values(participants).reduce((sum, val) => sum + val, 0n)
  logger.log(
    'EVALUATE ROUND %s: Invoking IE.setScores(); number of participants: %s, total score: %s',
    roundIndex,
    Object.keys(participants).length,
    totalScore === 1000000000000000n ? '100%' : totalScore
  )

  const start = new Date()
  const tx = await ieContractWithSigner.setScores(
    roundIndex,
    Object.keys(participants),
    Object.values(participants)
  )
  const setScoresDuration = new Date() - start
  logger.log('EVALUATE ROUND %s: IE.setScores() TX hash: %s', roundIndex, tx.hash)

  // Clean up
  const deleteMeasurements = db.prepare(`
    DELETE FROM measurements WHERE round_index = @roundIndex;
  `)
  const deleteRetrievalTasks = db.prepare(`
    DELETE FROM retrieval_tasks WHERE round_index = @roundIndex;
  `)
  await db.transaction(() => {
    deleteMeasurements.run({ roundIndex })
    deleteRetrievalTasks.run({ roundIndex })
  })()

  recordTelemetry('evaluate', point => {
    point.intField('round_index', roundIndex)
    point.intField('total_participants', Object.keys(participants).length)
    point.intField(
      'total_measurements',
      fraudAssessmentCounts.OK +
        fraudAssessmentCounts.INVALID_TASK +
        fraudAssessmentCounts.DUP_INET_GROUP
    )
    point.intField('honest_measurements', fraudAssessmentCounts.OK)
    point.intField('set_scores_duration_ms', setScoresDuration)

    for (const [type, count] of Object.entries(fraudAssessmentCounts)) {
      point.intField(`measurements_${type}`, count)
    }

    // TODO
    // for (const [key, value] of Object.entries(fraudDetectionStats.groupWinning)) {
    //   // At some point, we had a bug in the evaluation pipeline which caused some group winning
    //   // rates to be undefined or NaN. InfluxDB client rejected such values, which triggered
    //   // unhandled error. Let's avoid that situation by ignoring such data points.
    //   try {
    //     point.floatField(`group_winning_${key}`, value)
    //   } catch (err) {
    //     console.error(err)
    //     Sentry.captureException(err, { extra: { roundIndex } })
    //   }
    // }
  })

  // TODO
  // try {
  //   recordTelemetry('retrieval_stats_honest', (point) => {
  //     point.intField('round_index', roundIndex)
  //     buildRetrievalStats(honestMeasurements, point)
  //   })
  // } catch (err) {
  //   console.error('Cannot record retrieval stats (honest).', err)
  //   Sentry.captureException(err)
  // }

  // try {
  //   recordTelemetry('retrieval_stats_all', (point) => {
  //     point.intField('round_index', roundIndex)
  //     buildRetrievalStats(measurements, point)
  //   })
  // } catch (err) {
  //   console.error('Cannot record retrieval stats (all).', err)
  //   Sentry.captureException(err)
  // }
}

/**
 * @param {number} roundIndex
 * @param {import('better-sqlite3').Database} db
 * @returns {Promise<import('./typings').fraudDetectionResults>}
 */
export const runFraudDetection = async (roundIndex, db) => {
  // 1. Filter out measurements not belonging to any valid task in this round
  // 2. Reward only one participant in each inet group
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
  const rows = await db.prepare(`
    SELECT
      m.participant_address,
      SUM(CASE WHEN rt.cid IS NULL THEN 1 else 0 END) AS INVALID_TASK,
      SUM(CASE WHEN rt.cid IS NOT NULL AND mLowerHash.cid IS NOT NULL THEN 1 else 0 END) AS DUP_INET_GROUP,
      SUM(CASE WHEN rt.cid IS NOT NULL AND mLowerHash.cid IS NULL THEN 1 else 0 END) AS OK
    FROM measurements m
    LEFT JOIN retrieval_tasks rt
      ON m.cid = rt.cid
      AND m.provider_address = rt.provider_address
      AND m.protocol = rt.protocol
    LEFT OUTER JOIN measurements mLowerHash
      ON (m.task_group = mLowerHash.task_group AND mLowerHash.hash < m.hash)
    WHERE m.round_index = @roundIndex
    GROUP BY m.participant_address;
  `).all({
    roundIndex
  })

  const res = {}
  for (const { participant_address: participantAddress, ...counts } of rows) {
    res[participantAddress] = {
      INVALID_TASK: BigInt(counts.INVALID_TASK),
      DUP_INET_GROUP: BigInt(counts.DUP_INET_GROUP),
      OK: BigInt(counts.OK)
    }
  }
  return res

  // TODO
  // return {
  //   groupWinning: calculateInetGroupSuccessRates(taskGroups)
  // }
}

// TODO
// /**
//  * For each participant, calculate how many valid measurements were rewarded or
//  * rejected by our inet_group algorithm. Multiple measurements submitted for the same task
//  * are considered as one measurement.
//  *
//  * @param {Map<string, import('./typings').Measurement[]>} taskGroups
//  * @returns {import('./typings').GroupWinningStats}
//  */
// const calculateInetGroupSuccessRates = (taskGroups) => {
//   /** @type {Map<string, {won: number, lost: number}> */
//   const participantStats = new Map()

//   for (const groupMeasurements of taskGroups.values()) {
//     // First, find participants that submitted some valid measurements for this task
//     // and find out whether they were rewarded or not

//     /** @type {Map<string, boolean>} */
//     const taskParticipants = new Map()
//     for (const m of groupMeasurements) {
//       if (m.fraudAssessment === 'OK') {
//         taskParticipants.set(m.participantAddress, true)
//       } else if (m.fraudAssessment === 'DUP_INET_GROUP') {
//         if (!taskParticipants.has(m.participantAddress)) {
//           taskParticipants.set(m.participantAddress, false)
//         }
//       }
//     }

//     // Next, update each participant's winning score
//     for (const [participantAddress, wasPickedForReward] of taskParticipants.entries()) {
//       let s = participantStats.get(participantAddress)
//       if (!s) {
//         s = { won: 0, lost: 0 }
//         participantStats.set(participantAddress, s)
//       }

//       s[wasPickedForReward ? 'won' : 'lost']++
//     }
//   }

//   if (!participantStats.size) {
//     console.log('This round has no participants with OK or DUP_INET_GROUP measurements.')
//     return { min: 1, mean: 1, max: 1 }
//   }

//   // Finally, calculate the aggregate statistics
//   const result = { min: 1.0, max: 0.0, mean: undefined }
//   let sum = 0
//   for (const [participantAddress, {won, lost}] of participantStats.entries()) {
//     const successRate = won / (won + lost)
//     if (successRate < result.min) result.min = successRate
//     if (successRate > result.max) result.max = successRate
//     sum += successRate
//     debug(
//       'Winning rate for %s: won %s lost %s rate %s',
//       participantAddress,
//       won,
//       lost,
//       successRate
//     )
//   }
//   result.mean = sum / participantStats.size

//   debug('Winning rates stats: %o', result)
//   return result
// }
