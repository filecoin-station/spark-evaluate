import { evaluate } from '../lib/evaluate.js'
import assert from 'node:assert'
import { ethers } from 'ethers'

const { BigNumber } = ethers

describe('evaluate', () => {
  it('evaluates measurements', async () => {
    const rounds = { 0: [] }
    for (let i = 0; i < 10; i++) {
      rounds[0].push({ peerId: '0x123' })
    }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, peerIds, scores, summary) {
        setScoresCalls.push({ roundIndex, peerIds, scores, summary })
        return { hash: '0x234' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      logger
    })
    assert.deepStrictEqual(rounds, {})
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].peerIds, ['0x123'])
    assert.strictEqual(setScoresCalls[0].scores.length, 1)
    assert.strictEqual(
      setScoresCalls[0].scores[0].toString(),
      BigNumber.from(1_000_000_000_000_000).toString()
    )
    assert.match(setScoresCalls[0].summary, /^\d+ retrievals$/)
  })
  it('handles empty rounds', async () => {
    const rounds = { 0: [] }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, peerIds, scores, summary) {
        setScoresCalls.push({ roundIndex, peerIds, scores, summary })
        return { hash: '0x234' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].peerIds, [])
    assert.strictEqual(setScoresCalls[0].scores.length, 0)
    assert.strictEqual(setScoresCalls[0].summary, '0 retrievals')
  })
  it('handles unknown rounds', async () => {
    const rounds = {}
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (roundIndex, peerIds, scores, summary) {
        setScoresCalls.push({ roundIndex, peerIds, scores, summary })
        return { hash: '0x234' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].roundIndex, 0)
    assert.deepStrictEqual(setScoresCalls[0].peerIds, [])
    assert.strictEqual(setScoresCalls[0].scores.length, 0)
    assert.strictEqual(setScoresCalls[0].summary, '0 retrievals')
  })
  it('calculates reward shares', async () => {
    const rounds = { 0: [] }
    for (let i = 0; i < 5; i++) {
      rounds[0].push({ peerId: '0x123' })
      rounds[0].push({ peerId: '0x234' })
    }
    const setScoresCalls = []
    const ieContractWithSigner = {
      async setScores (_, peerIds, scores, summary) {
        setScoresCalls.push({ peerIds, scores, summary })
        return { hash: '0x345' }
      }
    }
    const logger = { log () {} }
    await evaluate({
      rounds,
      roundIndex: 0,
      ieContractWithSigner,
      logger
    })
    assert.strictEqual(setScoresCalls.length, 1)
    assert.deepStrictEqual(setScoresCalls[0].peerIds.sort(), ['0x123', '0x234'])
    const sum = setScoresCalls[0].scores[0]
      .add(setScoresCalls[0].scores[1]).toString()
    assert(
      ['1000000000000000', '999999999999999'].includes(sum),
      `Sum of scores not close enough. Got ${sum}`
    )
    assert.strictEqual(setScoresCalls[0].scores.length, 2)
    assert.match(setScoresCalls[0].summary, /^\d+ retrievals$/)
  })
})
