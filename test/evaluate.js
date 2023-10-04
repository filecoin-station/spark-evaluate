import { evaluate, runFraudDetection } from '../lib/evaluate.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import * as telemetry from '../lib/telemetry.js'

const { BigNumber } = ethers

after(telemetry.close)

// Get the details using this command:
//   curl https://spark.fly.dev/rounds/520 | jq .
const ROUND_WITH_KNOWN_DETAILS = 520

const VALID_PARTICIPANT_ADDRESS = '0xf100Ac342b7DE48e5c89f7029624eE6c3Cde68aC'
const VALID_MEASUREMENT = {
  participantAddress: VALID_PARTICIPANT_ADDRESS,
  cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
  provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
  protocol: 'bitswap'
}

describe('evaluate', () => {
  it('evaluates measurements', async () => {
    const roundIndex = ROUND_WITH_KNOWN_DETAILS
    const rounds = { [roundIndex]: [] }
    for (let i = 0; i < 10; i++) {
      rounds[roundIndex].push(VALID_MEASUREMENT)
    }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores, summary) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores, summary })
        return { hash: '0x234' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex,
      ieContractWithSigner,
      logger
    })
    assert.deepStrictEqual(rounds, {})
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, roundIndex)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [VALID_MEASUREMENT.participantAddress])
    assert.strictEqual(setScoresCalls[0].scores.length, 1)
    assert.strictEqual(
      setScoresCalls[0].scores[0].toString(),
      BigNumber.from(1_000_000_000_000_000).toString()
    )
    assert.match(setScoresCalls[0].summary, /^\d+ retrievals$/)
  })
  it('handles empty rounds', async () => {
    const roundIndex = ROUND_WITH_KNOWN_DETAILS
    const rounds = { [roundIndex]: [] }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores, summary) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores, summary })
        return { hash: '0x234' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex,
      ieContractWithSigner,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, roundIndex)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [])
    assert.strictEqual(setScoresCalls[0].scores.length, 0)
    assert.strictEqual(setScoresCalls[0].summary, '0 retrievals')
  })
  it('handles unknown rounds', async () => {
    const roundIndex = ROUND_WITH_KNOWN_DETAILS
    const rounds = {}
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, participantAddresses, scores, summary) {
        setScoresCalls.push({ roundIndex, participantAddresses, scores, summary })
        return { hash: '0x234' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex,
      ieContractWithSigner,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, roundIndex)
    assert.deepStrictEqual(setScoresCalls[0].participantAddresses, [])
    assert.strictEqual(setScoresCalls[0].scores.length, 0)
    assert.strictEqual(setScoresCalls[0].summary, '0 retrievals')
  })
  it('calculates reward shares', async () => {
    const roundIndex = ROUND_WITH_KNOWN_DETAILS
    const rounds = { [roundIndex]: [] }
    for (let i = 0; i < 5; i++) {
      rounds[roundIndex].push({ ...VALID_MEASUREMENT, participantAddress: '0x123' })
      rounds[roundIndex].push({ ...VALID_MEASUREMENT, participantAddress: '0x234' })
      rounds[roundIndex].push({
        participantAddress: '0x567',
        // invalid task
        cid: 'bafyreicnokmhmrnlp2wjhyk2haep4tqxiptwfrp2rrs7rzq7uk766chqvq',
        provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
        protocol: 'bitswap'
      })
    }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, participantAddresses, scores, summary) {
        setScoresCalls.push({ participantAddresses, scores, summary })
        return { hash: '0x345' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex,
      ieContractWithSigner,
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
    assert.match(setScoresCalls[0].summary, /^\d+ retrievals$/)
  })
})

describe('fraud detection', () => {
  it('checks if measurements are for a valid task', async () => {
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

    await runFraudDetection(ROUND_WITH_KNOWN_DETAILS, measurements)
    assert.deepStrictEqual(
      measurements.map(m => m.fraudAssessment),
      ['OK', 'INVALID_TASK']
    )
  })
})
