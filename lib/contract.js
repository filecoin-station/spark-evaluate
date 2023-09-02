import { ethers } from 'ethers'
import fs from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { newDelegatedEthAddress } from '@glif/filecoin-address'
import assert from 'node:assert'

const {
  RPC_URL = 'https://api.calibration.node.glif.io/rpc/v0',
  IE_CONTRACT_ADDRESS = '0x816830a1e536784ecb37cf97dfd7a98a82c86643',
  WALLET_SEED
} = process.env

assert(WALLET_SEED, 'WALLET_SEED required')

const provider = new ethers.providers.JsonRpcProvider(RPC_URL)
const signer = ethers.Wallet.fromMnemonic(WALLET_SEED).connect(provider)
console.log(
  'Wallet address:',
  signer.address,
  newDelegatedEthAddress(signer.address, 't').toString()
)

export const ieContract = new ethers.Contract(
  IE_CONTRACT_ADDRESS,
  JSON.parse(
    await fs.readFile(
      fileURLToPath(new URL('./abi.json', import.meta.url)),
      'utf8'
    )
  ),
  provider
)
export const ieContractWithSigner = ieContract.connect(signer)
