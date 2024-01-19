import { MAX_SCORE, evaluate, runFraudDetection, MAX_SET_SCORES_PARTICIPANTS, createSetScoresBuckets, SetScoresBucket } from '../lib/evaluate.js'
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import createDebug from 'debug'
import { VALID_MEASUREMENT, VALID_TASK, today } from './helpers/test-data.js'
import { assertPointFieldValue } from './helpers/assertions.js'
import { RoundData } from '../lib/round.js'
import { DATABASE_URL } from '../lib/config.js'
import pg from 'pg'
import { beforeEach } from 'mocha'
import { migrateWithPgClient } from '../lib/migrate.js'

const debug = createDebug('test')
const logger = { log: debug, error: debug }

const telemetry = []
const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  debug('recordTelemetry(%s): %o', measurementName, point.fields)
  telemetry.push(point)
}
beforeEach(() => telemetry.splice(0))

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('evaluate', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  beforeEach(async () => {
    await pgClient.query('DELETE FROM retrieval_stats')
  })

  after(async () => {
    await pgClient.end()
  })

  it('evaluates measurements', async () => {
    const round = new RoundData()
    for (let i = 0; i < 10; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT })
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      },
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    await evaluate({
      round,
      roundIndex: 0,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [VALID_MEASUREMENT.participantAddress])
    assert.strictEqual(setScoresCalls[0].scores.length, 1)
    assert.strictEqual(
      setScoresCalls[0].scores[0],
      1000000000000000n
    )

    const point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'total_nodes', '1i')
    // TODO: assert more point fields

    const { rows: publicStats } = await pgClient.query('SELECT * FROM retrieval_stats')
    assert.deepStrictEqual(publicStats, [{
      day: today(),
      total: 1,
      successful: 1
    }])
  })

  it('handles empty rounds', async () => {
    const round = new RoundData()
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      },
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [
      '0x000000000000000000000000000000000000dEaD'
    ])
    assert.deepStrictEqual(setScoresCalls[0].scores, [
      MAX_SCORE
    ])

    let point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)

    assertPointFieldValue(point, 'group_winning_min', '1')
    assertPointFieldValue(point, 'group_winning_mean', '1')
    assertPointFieldValue(point, 'group_winning_max', '1')
    // TODO: assert point fields

    point = telemetry.find(p => p.name === 'retrieval_stats_honest')
    assert(!!point,
          `No telemetry point "retrieval_stats_honest" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'measurements', '0i')
    assertPointFieldValue(point, 'unique_tasks', '0i')
    // no more fields are set for empty rounds
    assert.deepStrictEqual(Object.keys(point.fields), [
      'round_index',
      'measurements',
      'unique_tasks'
    ])
  })
  it('handles unknown rounds', async () => {
    const round = new RoundData()
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      },
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [
      '0x000000000000000000000000000000000000dEaD'
    ])
    assert.deepStrictEqual(setScoresCalls[0].scores, [
      MAX_SCORE
    ])
  })
  it('calculates reward shares', async () => {
    const round = new RoundData()
    for (let i = 0; i < 5; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x123' })
      round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x234', inet_group: 'group2' })
      round.measurements.push({
        ...VALID_MEASUREMENT,
        inet_group: 'group3',
        // invalid task
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      })
    }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, participantAddresses, scores) {
        setScoresCalls.push({ participantAddresses, scores })
        return { hash: '0x345' }
      },
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0,
      ieContractWithSigner,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses.sort(), ['0x123', '0x234'])
    const sum = (
      setScoresCalls[0].scores[0] +
      setScoresCalls[0].scores[1]
    ).toString()
    assert(
      ['1000000000000000', '999999999999999'].includes(sum),
      `Sum of scores not close enough. Got ${sum}`
    )
    assert.strictEqual(setScoresCalls[0].scores.length, 2)

    const point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'group_winning_min', '1')
    assertPointFieldValue(point, 'group_winning_mean', '1')
    assertPointFieldValue(point, 'group_winning_max', '1')

    assertPointFieldValue(point, 'total_nodes', '3i')
  })

  it('adds a dummy entry to ensure scores add up exactly to MAX_SCORE', async () => {
    const round = new RoundData()
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x123', inet_group: 'ig1' })
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x234', inet_group: 'ig2' })
    round.measurements.push({ ...VALID_MEASUREMENT, participantAddress: '0x456', inet_group: 'ig3' })

    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, participantAddresses, scores) {
        setScoresCalls.push({ participantAddresses, scores })
        return { hash: '0x345' }
      },
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    const logger = { log: debug, error: debug }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0,
      ieContractWithSigner,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    const { scores, participantAddresses } = setScoresCalls[0]
    assert.strictEqual(scores.length, 4)
    const sum = scores.reduce((prev, score) => (prev ?? 0) + score)
    assert.strictEqual(sum, MAX_SCORE)
    assert.strictEqual(participantAddresses.sort()[0], '0x000000000000000000000000000000000000dEaD')
  })

  it('reports retrieval stats - honest & all', async () => {
    const round = new RoundData()
    for (let i = 0; i < 5; i++) {
      round.measurements.push({ ...VALID_MEASUREMENT })
      round.measurements.push({
        ...VALID_MEASUREMENT,
        inet_group: 'group3',
        // invalid task
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap',
        retrievalResult: 'TIMEOUT'
      })
    }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, participantAddresses, scores) {
        setScoresCalls.push({ participantAddresses, scores })
        return { hash: '0x345' }
      },
      async getAddress () {
        return '0x811765AccE724cD5582984cb35f5dE02d587CA12'
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      round,
      roundIndex: 0,
      ieContractWithSigner,
      recordTelemetry,
      fetchRoundDetails,
      createPgClient,
      logger
    })

    let point = telemetry.find(p => p.name === 'retrieval_stats_honest')
    assert(!!point,
      `No telemetry point "retrieval_stats_honest" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'measurements', '1i')
    assertPointFieldValue(point, 'unique_tasks', '1i')
    assertPointFieldValue(point, 'success_rate', '1')

    point = telemetry.find(p => p.name === 'retrieval_stats_all')
    assert(!!point,
      `No telemetry point "retrieval_stats_all" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'measurements', '10i')
    assertPointFieldValue(point, 'unique_tasks', '2i')
    assertPointFieldValue(point, 'success_rate', '0.5')
  })
})

describe('fraud detection', () => {
  it('checks if measurements are for a valid task', async () => {
    const sparkRoundDetails = {
      roundId: 1234, // doesn't matter
      retrievalTasks: [
        {
          cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
          providerAddress: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
          protocol: 'bitswap'
        }
      ]
    }

    const measurements = [
      {
        ...VALID_MEASUREMENT,
        // valid task
        cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      },
      {
        ...VALID_MEASUREMENT,
        // invalid task
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      }
    ]

    await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.deepStrictEqual(
      measurements.map(m => m.fraudAssessment),
      ['OK', 'INVALID_TASK']
    )
  })

  it('rejects redundant measurements from the same inet group', async () => {
    const sparkRoundDetails = { roundId: 1234, retrievalTasks: [VALID_TASK] }
    const measurements = [
      { ...VALID_MEASUREMENT },
      { ...VALID_MEASUREMENT }
    ]

    const stats = await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.deepStrictEqual(
      measurements.map(m => m.fraudAssessment),
      ['OK', 'DUP_INET_GROUP']
    )
    assert.deepStrictEqual(stats, {
      groupWinning: {
        min: 1.0,
        max: 1.0,
        mean: 1.0
      }
    })
  })

  it('picks different inet-group member to reward for each task', async () => {
    // We have two participants in the same inet group
    // They both complete the same valid tasks
    // Ideally, our algorithm should assign one reward to each one
    const sparkRoundDetails = {
      roundId: 1234,
      retrievalTasks: [
        { ...VALID_TASK, cid: 'cid1' },
        { ...VALID_TASK, cid: 'cid2' }
      ]
    }
    // hard-coded to get deterministic results
    // the values are crafted to get distribute rewards among pa2 and pa3
    const timestamps = {
      pa1: {
        cid1: new Date('2023-11-01T09:00:01.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:21.000Z').getTime()
      },
      pa2: {
        cid1: new Date('2023-11-01T09:00:04.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:22.000Z').getTime()
      }
    }
    const measurements = []
    for (const participantAddress of Object.keys(timestamps)) {
      for (const task of sparkRoundDetails.retrievalTasks) {
        measurements.push({
          ...VALID_MEASUREMENT,
          ...task,
          participantAddress,
          // eslint-disable-next-line camelcase
          finished_at: timestamps[participantAddress][task.cid]
        })
      }
    }

    const stats = await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.deepStrictEqual(
      measurements.map(m => `${m.participantAddress}::${m.fraudAssessment}`),
      [
        'pa1::OK',
        'pa1::DUP_INET_GROUP',
        'pa2::DUP_INET_GROUP',
        'pa2::OK'
      ]
    )
    assert.deepStrictEqual(stats, {
      groupWinning: {
        min: 0.5,
        max: 0.5,
        mean: 0.5
      }
    })
  })

  it('calculates aggregate stats of participant group-winning rate', async () => {
    // Let's create three different tasks and three participants where two share the same inet group.
    // All three participants measure all three tasks.
    const sparkRoundDetails = {
      roundId: 1234,
      retrievalTasks: [
        { ...VALID_TASK, cid: 'cid1' },
        { ...VALID_TASK, cid: 'cid2' },
        { ...VALID_TASK, cid: 'cid3' }
      ]
    }

    const participantSubnets = {
      pa1: 'ig1',
      pa2: 'ig2',
      pa3: 'ig2' // same as above!
    }
    // hard-coded to get deterministic results
    // the values are crafted to distribute rewards between pa2 and pa3
    const timestamps = {
      pa1: {
        cid1: new Date('2023-11-01T09:00:01.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:21.000Z').getTime(),
        cid3: new Date('2023-11-01T09:00:41.000Z').getTime()
      },
      pa2: {
        cid1: new Date('2023-11-01T09:00:04.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:22.000Z').getTime(),
        cid3: new Date('2023-11-01T09:00:42.000Z').getTime()
      },
      pa3: {
        cid1: new Date('2023-11-01T09:00:03.000Z').getTime(),
        cid2: new Date('2023-11-01T09:00:23.000Z').getTime(),
        cid3: new Date('2023-11-01T09:03:43.000Z').getTime()
      }
    }

    /** @type {import('../lib/typings').Measurement[]} */
    const measurements = []

    // eslint-disable-next-line camelcase
    for (const [participantAddress, inet_group] of Object.entries(participantSubnets)) {
      for (const task of sparkRoundDetails.retrievalTasks) {
        measurements.push({
          ...VALID_MEASUREMENT,
          ...task,
          participantAddress,
          // eslint-disable-next-line camelcase
          inet_group,
          finished_at: timestamps[participantAddress][task.cid]
        })
      }
    }

    const stats = await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.deepStrictEqual(
      measurements.map(m => `${m.participantAddress}::${m.fraudAssessment}`),
      [
        'pa1::OK',
        'pa1::OK',
        'pa1::OK',

        'pa2::DUP_INET_GROUP',
        'pa2::OK',
        'pa2::DUP_INET_GROUP',

        'pa3::OK',
        'pa3::DUP_INET_GROUP',
        'pa3::OK'
      ]
    )
    assert.deepStrictEqual(stats, {
      groupWinning: {
        min: 0.3333333333333333,
        max: 1.0,
        mean: 0.6666666666666666
      }
    })
  })

  it('rejects measurements above maxTasksPerNode', async () => {
    const sparkRoundDetails = {
      roundId: 1234,
      retrievalTasks: [
        {
          ...VALID_TASK,
          cid: 'cid1'
        }, {
          ...VALID_TASK,
          cid: 'cid2'
        }, {
          ...VALID_TASK,
          cid: 'cid3'
        }
      ],
      maxTasksPerNode: 1
    }
    const measurements = [
      {
        ...VALID_MEASUREMENT,
        cid: 'cid1',
        inet_group: 'group1'
      },
      {
        ...VALID_MEASUREMENT,
        cid: 'cid2',
        inet_group: 'group1'
      },
      {
        ...VALID_MEASUREMENT,
        cid: 'cid3'
      }
    ]

    await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.deepStrictEqual(
      measurements.map(m => m.fraudAssessment),
      ['OK', 'TOO_MANY_TASKS', 'OK']
    )
  })
})

describe('set scores buckets', () => {
  it('splits contract calls with many participants', async () => {
    const participants = {}
    for (let i = 0; i < MAX_SET_SCORES_PARTICIPANTS + 1; i++) {
      participants[`0x${i}`] = 1000000000000000n
    }
    const buckets = createSetScoresBuckets(participants)
    assert.strictEqual(buckets.length, 2)
    assert.strictEqual(buckets[0].size, MAX_SET_SCORES_PARTICIPANTS)
    assert.strictEqual(buckets[1].size, 1)
    const bucket = new SetScoresBucket()
    bucket.add(`0x${MAX_SET_SCORES_PARTICIPANTS}`, 1000000000000000n)
    assert.deepStrictEqual(buckets[1], bucket)
  })
})
