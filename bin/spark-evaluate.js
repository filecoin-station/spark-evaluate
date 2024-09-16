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
import { createMeridianContract, provider } from '../lib/contracts.js'
import { startCancelStuckTxs } from '../lib/cancel-stuck-txs.js'

const {
  SENTRY_ENVIRONMENT = 'development',
  WALLET_SEED
} = process.env

Sentry.init({
  dsn: 'https://d0651617f9690c7e9421ab9c949d67a4@o1408530.ingest.sentry.io/4505906069766144',
  environment: SENTRY_ENVIRONMENT,
  // Performance Monitoring
  tracesSampleRate: 0.1 // Capture 10% of the transactions
})

assert(WALLET_SEED, 'WALLET_SEED required')

await migrateWithPgConfig({ connectionString: DATABASE_URL })

const ieContract = await createMeridianContract()

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

await Promise.all([
  startEvaluate({
    ieContract,
    ieContractWithSigner,
    fetchMeasurements,
    fetchRoundDetails,
    recordTelemetry,
    createPgClient,
    logger: console
  }),
  startCancelStuckTxs({
    walletDelegatedAddress,
    address: wallet.address,
    signer
  })
])
