import * as Sentry from '@sentry/node'
import { StuckTransactionsCanceller } from 'cancel-stuck-transactions'
import ms from 'ms'
import timers from 'node:timers/promises'

const ROUND_LENGTH_MS = ms('20 minutes')
const CHECK_STUCK_TXS_DELAY = ms('1 minute')

export const createStuckTransactionsCanceller = ({ pgClient, signer }) => {
  return new StuckTransactionsCanceller({
    store: {
      async set ({ hash, timestamp, from, maxPriorityFeePerGas, gasLimit, nonce }) {
        await pgClient.query(`
          INSERT INTO transactions_pending (hash, timestamp, from_address, max_priority_fee_per_gas, gas_limit, nonce)
          VALUES ($1, $2, $3, $4, $5, $6)
          `,
        [hash, timestamp, from, maxPriorityFeePerGas, gasLimit, nonce]
        )
      },
      async list () {
        const { rows } = await pgClient.query(`
          SELECT
            hash,
            timestamp,
            from_address as "from",
            max_priority_fee_per_gas as "maxPriorityFeePerGas",
            gas_limit as "gasLimit",
            nonce
          FROM transactions_pending
        `)
        return rows.map(row => ({
          ...row,
          maxPriorityFeePerGas: BigInt(row.maxPriorityFeePerGas),
          gasLimit: BigInt(row.gasLimit)
        }))
      },
      async remove (hash) {
        await pgClient.query(
          'DELETE FROM transactions_pending WHERE hash = $1',
          [hash]
        )
      }
    },
    log (str) {
      console.log(str)
    },
    sendTransaction (tx) {
      return signer.sendTransaction(tx)
    }
  })
}

export const startCancelStuckTransactions = async stuckTransactionsCanceller => {
  while (true) {
    (async () => {
      const res = await stuckTransactionsCanceller.cancelOlderThan(
        2 * ROUND_LENGTH_MS
      )
      if (res !== undefined) {
        for (const { status, reason } of res) {
          if (status === 'rejected') {
            console.error('Failed to cancel transaction:', reason)
            Sentry.captureException(reason)
          }
        }
      }
    })().catch(err => {
      console.error(err)
      if (err.code !== 'FILFOX_REQUEST_FAILED') {
        Sentry.captureException(err)
      }
    })
    await timers.setTimeout(CHECK_STUCK_TXS_DELAY)
  }
}
