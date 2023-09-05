import { startEvaluate } from '../index.js'
import assert from 'node:assert'
import { ethers } from 'ethers'
import { fileURLToPath } from 'node:url'
import { newDelegatedEthAddress } from '@glif/filecoin-address'
import { Web3Storage } from 'web3.storage'
import fs from 'node:fs/promises'

const {
  IE_CONTRACT_ADDRESS = '0x816830a1e536784ecb37cf97dfd7a98a82c86643',
  RPC_URL = 'https://api.calibration.node.glif.io/rpc/v0',
  WALLET_SEED,
  WEB3_STORAGE_API_TOKEN
} = process.env

assert(WALLET_SEED, 'WALLET_SEED required')
assert(WEB3_STORAGE_API_TOKEN, 'WEB3_STORAGE_API_TOKEN required')

const provider = new ethers.providers.JsonRpcProvider(RPC_URL)
const signer = ethers.Wallet.fromMnemonic(WALLET_SEED).connect(provider)
console.log(
  'Wallet address:',
  signer.address,
  newDelegatedEthAddress(signer.address, 't').toString()
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
const web3Storage = new Web3Storage({ token: WEB3_STORAGE_API_TOKEN })

startEvaluate({
  ieContract,
  ieContractWithSigner,
  web3Storage
})
