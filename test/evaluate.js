import { MAX_SCORE, evaluate, runFraudDetection } from '../lib/evaluate.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import createDebug from 'debug'

const { BigNumber } = ethers

const debug = createDebug('test')

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
const VALID_MEASUREMENT = {
  cid: VALID_TASK.cid,
  provider_address: VALID_TASK.providerAddress,
  protocol: VALID_TASK.protocol,
  participantAddress: VALID_PARTICIPANT_ADDRESS
}

describe('evaluate', () => {
  it('evaluates measurements', async () => {
    const rounds = { 0: [] }
    for (let i = 0; i < 10; i++) {
      rounds[0].push(VALID_MEASUREMENT)
    }
    const fetchRoundDetails = () => ({ retrievalTasks: [VALID_TASK] })
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores })
        return { hash: '0x234' }
      }
    }
    const logger = { log: debug, error: debug }
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
    const logger = { log: debug, error: debug }
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
    const logger = { log: debug, error: debug }
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
      rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x234' })
      rounds[0].push({
        participantAddress: '0x567',
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
    rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x123' })
    rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x234' })
    rounds[0].push({ ...VALID_MEASUREMENT, participantAddress: '0x456' })

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
})

describe('fraud detection', () => {
  it('checks if measurements are for a valid task', async () => {
    const sparkRoundDetails = {
      roundId: 1234, // doesn't matte
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
        participantAddress: VALID_PARTICIPANT_ADDRESS,
        // valid task
        cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      },
      {
        participantAddress: VALID_PARTICIPANT_ADDRESS,
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
})
