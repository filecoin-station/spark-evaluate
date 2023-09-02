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
    const ieContractWithSigner = {
      async setScores (roundIndex, peerIds, scores, summary) {
        assert.strictEqual(roundIndex, 0)
        assert.deepStrictEqual(peerIds, ['0x123'])
        assert.strictEqual(scores.length, 1)
        assert.strictEqual(
          scores[0].toString(),
          BigNumber.from(1_000_000_000_000_000).toString()
        )
        assert.match(summary, /^\d+ retrievals$/)
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
  })
  it('handles empty rounds', async () => {
    const rounds = { 0: [] }
    const ieContractWithSigner = {
      async setScores (roundIndex, peerIds, scores, summary) {
        assert.strictEqual(roundIndex, 0)
        assert.deepStrictEqual(peerIds, [])
        assert.deepStrictEqual(scores, [])
        assert.strictEqual(summary, '0 retrievals')
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
  })
  it('handles unknown rounds', async () => {
    const rounds = { 0: [] }
    const ieContractWithSigner = {
      async setScores (roundIndex, peerIds, scores, summary) {
        assert.strictEqual(roundIndex, 0)
        assert.deepStrictEqual(peerIds, [])
        assert.deepStrictEqual(scores, [])
        assert.strictEqual(summary, '0 retrievals')
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
  })
  it('calculates reward shares', async () => {
    const rounds = { 0: [] }
    for (let i = 0; i < 5; i++) {
      rounds[0].push({ peerId: '0x123' })
      rounds[0].push({ peerId: '0x234' })
    }
    const ieContractWithSigner = {
      async setScores (_, peerIds, scores, summary) {
        assert.deepStrictEqual(peerIds.sort(), ['0x123', '0x234'])
        const sum = scores[0].add(scores[1]).toString()
        assert(
          ['1000000000000000', '999999999999999'].includes(sum),
          `Sum of scores not close enough. Got ${sum}`
        )
        assert.strictEqual(scores.length, 2)
        assert.match(summary, /^\d+ retrievals$/)
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
  })
})
