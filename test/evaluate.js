import { evaluate, runFraudDetection } from '../lib/evaluate.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import createDebug from 'debug'

const { BigNumber } = ethers

const debug = createDebug('test')
const logger = { log: debug, error: debug }

const recordTelemetry = (measurementName, fn) => {
  /* no-op */
  debug('recordTelemetry(%s)', measurementName)
}

const VALID_PARTICIPANT_ADDRESS = '0x000000000000000000000000000000000000dEaD'
const VALID_TASK = {
  cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
  providerAddress: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
  protocol: 'bitswap'
}
Object.freeze(VALID_TASK)

const VALID_MEASUREMENT = {
  cid: VALID_TASK.cid,
  provider_address: VALID_TASK.providerAddress,
  protocol: VALID_TASK.protocol,
  participantAddress: VALID_PARTICIPANT_ADDRESS,
  inet_group: 'some-group-id',
  finished_at: '2023-11-01T09:00:00.000Z'
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
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [])
    assert.strictEqual(setScoresCalls[0].scores.length, 0)
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
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [])
    assert.strictEqual(setScoresCalls[0].scores.length, 0)
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

  it('rejects tasks with no `inet_group` field', async () => {
    const sparkRoundDetails = { roundId: 1234, retrievalTasks: [VALID_TASK] }

    // eslint-disable-next-line camelcase
    const { inet_group, ...fields } = VALID_MEASUREMENT
    const measurements = [{
      ...fields
      // missing inet_group
    }]

    await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.strictEqual(measurements[0].fraudAssessment, 'NO_INET_GROUP')
  })

  it('rejects tasks with no `finished_at` field', async () => {
    const sparkRoundDetails = { roundId: 1234, retrievalTasks: [VALID_TASK] }

    // eslint-disable-next-line camelcase
    const { finished_at, ...fields } = VALID_MEASUREMENT
    const measurements = [{
      ...fields
      // missing finished_at
    }]

    await runFraudDetection(1, measurements, sparkRoundDetails)
    assert.strictEqual(measurements[0].fraudAssessment, 'NO_FINISHED_AT')
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
        cid1: new Date('2023-11-01T09:00:01.000Z'),
        cid2: new Date('2023-11-01T09:00:21.000Z')
      },
      pa2: {
        cid1: new Date('2023-11-01T09:00:04.000Z'),
        cid2: new Date('2023-11-01T09:00:22.000Z')
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
        cid1: new Date('2023-11-01T09:00:01.000Z'),
        cid2: new Date('2023-11-01T09:00:21.000Z'),
        cid3: new Date('2023-11-01T09:00:41.000Z')
      },
      pa2: {
        cid1: new Date('2023-11-01T09:00:04.000Z'),
        cid2: new Date('2023-11-01T09:00:22.000Z'),
        cid3: new Date('2023-11-01T09:00:42.000Z')
      },
      pa3: {
        cid1: new Date('2023-11-01T09:00:03.000Z'),
        cid2: new Date('2023-11-01T09:00:23.000Z'),
        cid3: new Date('2023-11-01T09:03:43.000Z')
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
        'pa2::DUP_INET_GROUP',
        'pa2::OK',

        'pa3::OK',
        'pa3::OK',
        'pa3::DUP_INET_GROUP'
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
