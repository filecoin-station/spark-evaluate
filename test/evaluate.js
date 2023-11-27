import { MAX_SCORE, evaluate, runFraudDetection } from '../lib/evaluate.js'
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import createDebug from 'debug'

const { BigNumber } = ethers

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

const VALID_PARTICIPANT_ADDRESS = '0x000000000000000000000000000000000000dEaD'
const VALID_TASK = {
  cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
  providerAddress: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
  protocol: 'bitswap'
}
Object.freeze(VALID_TASK)

/** @type {import('../lib/typings').Measurement} */
const VALID_MEASUREMENT = {
  cid: VALID_TASK.cid,
  provider_address: VALID_TASK.providerAddress,
  protocol: VALID_TASK.protocol,
  participantAddress: VALID_PARTICIPANT_ADDRESS,
  inet_group: 'some-group-id',
  status_code: 200,
  timeout: false,
  car_too_large: false,
  start_at: '2023-11-01T09:00:00.000Z',
  first_byte_at: '2023-11-01T09:00:01.000Z',
  end_at: '2023-11-01T09:00:02.000Z',
  finished_at: '2023-11-01T09:00:10.000Z',
  byte_length: 1024,
  retrievalResult: 'OK'
}

// Fraud detection is mutating the measurements parsed from JSON
// To prevent tests from accidentally mutating data used by subsequent tests,
// we freeze this test data object. If we forget to clone this default measurement
// then such test will immediately fail.
Object.freeze(VALID_MEASUREMENT)

describe('evaluate', () => {
  it('evaluates measurements', async () => {
    const rounds = { 0: [] }
    for (let i = 0; i < 10; i++) {
      rounds[0].push({ ...VALID_MEASUREMENT })
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      }
    }
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
      logger
    })
    assert.deepStrictEqual(rounds, {})
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [VALID_MEASUREMENT.participantAddress])
    assert.strictEqual(setScoresCalls[0].scores.length, 1)
    assert.strictEqual(
      setScoresCalls[0].scores[0].toString(),
      BigNumber.from(1_000_000_000_000_000).toString()
    )

    let point = telemetry.find(p => p.name === 'evaluate')
    assert(!!point,
      `No telemetry point "evaluate" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    // TODO: assert point fields

    point = telemetry.find(p => p.name === 'retrieval_stats_honest')
    assert(!!point,
      `No telemetry point "retrieval_stats_honest" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    assertPointFieldValue(point, 'unique_tasks', '1i')
    assertPointFieldValue(point, 'success_rate', '1')
  })
  it('handles empty rounds', async () => {
    const rounds = { 0: [] }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
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
    const rounds = {}
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
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
    const rounds = { 0: [] }
    for (let i = 0; i < 5; i++) {
      rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x123' })
      rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x234', inet_group: 'group2' })
      rounds[0].push({
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
      }
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      recordTelemetry,
      fetchRoundDetails,
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
  })

  it('adds a dummy entry to ensure scores add up exactly to MAX_SCORE', async () => {
    const rounds = { 0: [] }
    rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x123', inet_group: 'ig1' })
    rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x234', inet_group: 'ig2' })
    rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x456', inet_group: 'ig3' })

    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, participantAddresses, scores) {
        setScoresCalls.push({ participantAddresses, scores })
        return { hash: '0x345' }
      }
    }
    const logger = { log: debug, error: debug }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      recordTelemetry,
      fetchRoundDetails,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    const { scores, participantAddresses } = setScoresCalls[0]
    assert.strictEqual(scores.length, 4)
    const sum = scores.reduce((prev, score) => (prev ?? 0) + score)
    assert.strictEqual(sum, MAX_SCORE)
    assert.strictEqual(participantAddresses.sort()[0], '0x000000000000000000000000000000000000dEaD')
  })

  it('reports retrieval stats for honest and for all measurements', async () => {
    const measurements = [
      {
        ...VALID_MEASUREMENT
      },
      {
        ...VALID_MEASUREMENT,
        status_code: 500,
        retrievalResult: 'ERROR_500',
        participantAddress: '0xcheater',
        inet_group: 'abcd',
        start_at: '2023-11-01T09:00:00.000Z',
        first_byte_at: '2023-11-01T09:00:10.000Z',
        end_at: '2023-11-01T09:00:20.000Z',
        finished_at: '2023-11-01T09:00:30.000Z',
        byte_length: 2048,

        // invalid task
        cid: 'bafyinvalid',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      }
    ]

    const rounds = { 0: [...measurements] }

    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, participantAddresses, scores) {
        setScoresCalls.push({ participantAddresses, scores })
        return { hash: '0x345' }
      }
    }
    const logger = { log: debug, error: debug }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      recordTelemetry,
      fetchRoundDetails,
      logger
    })

    let point = telemetry.find(p => p.name === 'retrieval_stats_honest')
    assert(!!point,
      `No telemetry point "retrieval_stats_honest" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    debug(point.name, point.fields)

    assertPointFieldValue(point, 'unique_tasks', '1i')
    assertPointFieldValue(point, 'success_rate', '1')
    assertPointFieldValue(point, 'participants', '1i')
    assertPointFieldValue(point, 'inet_groups', '1i')
    assertPointFieldValue(point, 'measurements', '1i')
    assertPointFieldValue(point, 'download_bandwidth', '1024i')

    assertPointFieldValue(point, 'result_rate_OK', '1')
    assertPointFieldValue(point, 'result_rate_TIMEOUT', '0')

    assertPointFieldValue(point, 'ttfb_p10', '1000i')
    assertPointFieldValue(point, 'ttfb_mean', '1000i')
    assertPointFieldValue(point, 'ttfb_p90', '1000i')

    assertPointFieldValue(point, 'duration_p10', '2000i')
    assertPointFieldValue(point, 'duration_mean', '2000i')
    assertPointFieldValue(point, 'duration_p90', '2000i')

    assertPointFieldValue(point, 'car_size_p10', '1024i')
    assertPointFieldValue(point, 'car_size_mean', '1024i')
    assertPointFieldValue(point, 'car_size_p90', '1024i')

    point = telemetry.find(p => p.name === 'retrieval_stats_all')
    assert(!!point,
      `No telemetry point "retrieval_stats_all" was recorded. Actual points: ${JSON.stringify(telemetry.map(p => p.name))}`)
    debug(point.name, point.fields)

    assertPointFieldValue(point, 'unique_tasks', '2i')
    assertPointFieldValue(point, 'success_rate', '0.5')
    assertPointFieldValue(point, 'participants', '2i')
    assertPointFieldValue(point, 'inet_groups', '2i')
    assertPointFieldValue(point, 'measurements', '2i')
    assertPointFieldValue(point, 'download_bandwidth', '3072i')

    assertPointFieldValue(point, 'result_rate_OK', '0.5')
    assertPointFieldValue(point, 'result_rate_TIMEOUT', '0')
    assertPointFieldValue(point, 'result_rate_ERROR_500', '0.5')

    assertPointFieldValue(point, 'ttfb_min', '1000i')
    assertPointFieldValue(point, 'ttfb_mean', '5500i')
    assertPointFieldValue(point, 'ttfb_p90', '10000i')

    assertPointFieldValue(point, 'duration_p10', '2000i')
    assertPointFieldValue(point, 'duration_mean', '11000i')
    assertPointFieldValue(point, 'duration_p90', '20000i')

    assertPointFieldValue(point, 'car_size_p10', '1024i')
    assertPointFieldValue(point, 'car_size_mean', '1536i')
    assertPointFieldValue(point, 'car_size_p90', '2048i')
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
        cid1: '2023-11-01T09:00:01.000Z',
        cid2: '2023-11-01T09:00:21.000Z'
      },
      pa2: {
        cid1: '2023-11-01T09:00:04.000Z',
        cid2: '2023-11-01T09:00:22.000Z'
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
        cid1: '2023-11-01T09:00:01.000Z',
        cid2: '2023-11-01T09:00:21.000Z',
        cid3: '2023-11-01T09:00:41.000Z'
      },
      pa2: {
        cid1: '2023-11-01T09:00:04.000Z',
        cid2: '2023-11-01T09:00:22.000Z',
        cid3: '2023-11-01T09:00:42.000Z'
      },
      pa3: {
        cid1: '2023-11-01T09:00:03.000Z',
        cid2: '2023-11-01T09:00:23.000Z',
        cid3: '2023-11-01T09:03:43.000Z'
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
})

const assertPointFieldValue = (point, fieldName, expectedValue) => {
  const actualValue = point.fields[fieldName]
  assert.strictEqual(
    actualValue,
    expectedValue,
   `Expected ${point.name}.fields.${fieldName} to equal ${expectedValue} but found ${actualValue}`
  )
}
