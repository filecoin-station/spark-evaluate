import * as Sentry from '@sentry/node'
import { DATABASE_URL } from '../lib/config.js'
import { startEvaluate } from '../index.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import { CoinType, newDelegatedEthAddress } from '@glif/filecoin-address'
import { recordTelemetry } from '../lib/telemetry.js'
import { fetchMeasurements } from '../lib/preprocess.js'
import { migrateWithPgConfig } from '../lib/migrate.js'
import pg from 'pg'
import { createMeridianContract } from '../lib/ie-contract.js'
import { CancelStuckTransactions } from 'cancel-stuck-transactions'
import ms from 'ms'
import timers from 'node:timers/promises'

const {
  SENTRY_ENVIRONMENT = 'development',
  WALLET_SEED
} = process.env

const ROUND_LENGTH_MS = ms('20 minutes')
const CHECK_STUCK_TXS_DELAY = ms('1 minute')

Sentry.init({
  dsn: 'https://d0651617f9690c7e9421ab9c949d67a4@o1408530.ingest.sentry.io/4505906069766144',
  environment: SENTRY_ENVIRONMENT,
  // Performance Monitoring
  tracesSampleRate: 0.1 // Capture 10% of the transactions
})

assert(WALLET_SEED, 'WALLET_SEED required')

await migrateWithPgConfig({ connectionString: DATABASE_URL })

const { ieContract, provider } = await createMeridianContract()

const wallet = ethers.Wallet.fromPhrase(WALLET_SEED, provider)
const signer = new ethers.NonceManager(wallet)
const walletDelegatedAddress = newDelegatedEthAddress(/** @type {any} */(wallet.address), CoinType.MAIN).toString()

console.log('Wallet address:', wallet.address, walletDelegatedAddress)
const ieContractWithSigner = ieContract.connect(signer)

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

const pgClient = await createPgClient()
const cancelStuckTransactions = new CancelStuckTransactions({
  async store ({ hash, timestamp, from, maxPriorityFeePerGas, nonce }) {
    await pgClient.query(`
      INSERT INTO transactions_pending (hash, timestamp, from, max_priority_fee_per_gas, nonce)
      VALUES ($1, $2, $3, $4, $5)
      `,
      [hash, timestamp, from, maxPriorityFeePerGas, nonce]
    )
  },
  async list () {
    const { rows } = await pgClient.query(`SELECT * FROM transactions_pending`)
    return rows.map(row => ({
      hash: row.hash,
      timestamp: row.timestamp,
      from: row.from,
      maxPriorityFeePerGas: row.max_priority_fee_per_gas,
      nonce: row.nonce
    }))
  },
  async resolve (hash) {
    await pgClient.query(
      'DELETE FROM transactions_pending WHERE hash = $1',
      [hash]
    )
  },
  log (str) {
    console.log(str)
  },
  sendTransaction (tx) {
    return signer.sendTransaction(tx)
  }
})

await Promise.all([
  startEvaluate({
    ieContract,
    ieContractWithSigner,
    fetchMeasurements,
    fetchRoundDetails,
    recordTelemetry,
    createPgClient,
    logger: console,
    cancelStuckTransactions
  }),
  (async () => {
    while (true) {
      try {
        await cancelStuckTransactions.olderThan(2 * ROUND_LENGTH_MS)
      } catch (err) {
        console.error(err)
        Sentry.captureException(err)
      }
      await timers.setTimeout(CHECK_STUCK_TXS_DELAY)
    }
  })()
])
