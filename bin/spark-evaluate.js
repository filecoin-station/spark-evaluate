import * as Sentry from '@sentry/node'
import { startEvaluate } from '../index.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import { fileURLToPath } from 'node:url'
import { newDelegatedEthAddress } from '@glif/filecoin-address'
import { recordTelemetry } from '../lib/telemetry.js'
import fs from 'node:fs/promises'
import { fetchMeasurements } from '../lib/preprocess.js'

const {
  SENTRY_ENVIRONMENT = 'development',
  IE_CONTRACT_ADDRESS = '0xaaef78eaf86dcf34f275288752e892424dda9341',
  RPC_URL = 'https://api.node.glif.io/rpc/v0',
  WALLET_SEED
} = process.env

Sentry.init({
  dsn: 'https://d0651617f9690c7e9421ab9c949d67a4@o1408530.ingest.sentry.io/4505906069766144',
  environment: SENTRY_ENVIRONMENT,
  // Performance Monitoring
  tracesSampleRate: 0.1 // Capture 10% of the transactions
})

assert(WALLET_SEED, 'WALLET_SEED required')

const provider = new ethers.providers.JsonRpcProvider(RPC_URL)
const signer = ethers.Wallet.fromMnemonic(WALLET_SEED).connect(provider)
console.log(
  'Wallet address:',
  signer.address,
  newDelegatedEthAddress(signer.address, 'f').toString()
)
const ieContract = new ethers.Contract(
  IE_CONTRACT_ADDRESS,
  JSON.parse(
    await fs.readFile(
      fileURLToPath(new URL('../lib/abi.json', import.meta.url)),
      'utf8'
    )
  ),
  provider
)
const ieContractWithSigner = ieContract.connect(signer)

startEvaluate({
  ieContract,
  ieContractWithSigner,
  fetchMeasurements,
  fetchRoundDetails,
  recordTelemetry,
  logger: console
})
