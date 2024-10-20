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
import { createContracts } from '../lib/contracts.js'
import { setScores } from '../lib/submit-scores.js'
import { runPublishRsrLoop } from '../lib/publish-rsr.js'
import * as Client from '@web3-storage/w3up-client'
import { ed25519 } from '@ucanto/principal'
import { CarReader } from '@ipld/car'
import { importDAG } from '@ucanto/core/delegation'

const {
  SENTRY_ENVIRONMENT = 'development',
  WALLET_SEED,
  STORACHA_PRIVATE_KEY,
  STORACHA_PROOF,
  GIT_COMMIT
} = process.env

Sentry.init({
  dsn: 'https://d0651617f9690c7e9421ab9c949d67a4@o1408530.ingest.sentry.io/4505906069766144',
  environment: SENTRY_ENVIRONMENT,
  // Performance Monitoring
  tracesSampleRate: 0.1 // Capture 10% of the transactions
})

assert(WALLET_SEED, 'WALLET_SEED required')
assert(STORACHA_PRIVATE_KEY, 'STORACHA_PRIVATE_KEY required')
assert(STORACHA_PROOF, 'STORACHA_PROOF required')

await migrateWithPgConfig({ connectionString: DATABASE_URL })

async function parseProof (data) {
  const blocks = []
  const reader = await CarReader.fromBytes(Buffer.from(data, 'base64'))
  for await (const block of reader.blocks()) {
    blocks.push(block)
  }
  return importDAG(blocks)
}

const principal = ed25519.Signer.parse(STORACHA_PRIVATE_KEY)
const storachaClient = await Client.create({ principal })
const proof = await parseProof(STORACHA_PROOF)
const space = await storachaClient.addSpace(proof)
await storachaClient.setCurrentSpace(space.did())

const { ieContract, rsrContract, provider } = createContracts()

const signer = ethers.Wallet.fromPhrase(WALLET_SEED, provider)
const walletDelegatedAddress = newDelegatedEthAddress(/** @type {any} */(signer.address), CoinType.MAIN).toString()

console.log('Wallet address:', signer.address, walletDelegatedAddress)

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

await Promise.all([
  startEvaluate({
    ieContract,
    fetchMeasurements,
    fetchRoundDetails,
    recordTelemetry,
    createPgClient,
    logger: console,
    setScores: (participants, values) => setScores(signer, participants, values),
    gitCommit: GIT_COMMIT
  }),
  runPublishRsrLoop({
    createPgClient,
    storachaClient,
    rsrContract: rsrContract.connect(signer)
  })
])
