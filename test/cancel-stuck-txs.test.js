import assert from 'node:assert'
import {
  getMessagesInMempool,
  getRecentSendMessage
} from '../lib/cancel-stuck-txs.js'

describe('cancel stuck transactions', function () {
  this.timeout(10_000)

  describe('getMessagesInMempool(addr)', () => {
    it('should return messages in mempool', async () => {
      const messages = await getMessagesInMempool(
        '0x000000000000000000000000000000000000dEaD'
      )
      assert(Array.isArray(messages))
    })
  })
  describe('getRecentSendMessage()', () => {
    it('should return a recent send message', async () => {
      const recentSendMessage = await getRecentSendMessage()
      assert(recentSendMessage.receipt.gasUsed)
      assert(recentSendMessage.gasFeeCap)
    })
  })
})
