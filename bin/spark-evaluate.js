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
import { setScores } from '../lib/submit-scores.js'

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

const { ieContract, provider } = await createMeridianContract()

const signer = ethers.Wallet.fromPhrase(WALLET_SEED, provider)
const walletDelegatedAddress = newDelegatedEthAddress(/** @type {any} */(signer.address), CoinType.MAIN).toString()

console.log('Wallet address:', signer.address, walletDelegatedAddress)

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

await startEvaluate({
  ieContract,
  fetchMeasurements,
  fetchRoundDetails,
  recordTelemetry,
  createPgClient,
  logger: console,
  setScores: (participants, values) => setScores(signer, participants, values)
})
