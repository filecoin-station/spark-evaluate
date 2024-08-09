// dotenv must be imported before importing anything else
import 'dotenv/config'

import assert from 'node:assert'
import { ethers } from 'ethers'
import { CoinType, newDelegatedEthAddress } from '@glif/filecoin-address'

import { createMeridianContract } from '../lib/ie-contract.js'

const {
  WALLET_SEED
} = process.env

const [_node, _script, tx] = process.argv

assert(WALLET_SEED, 'WALLET_SEED required')
assert(tx, 'Transaction hash must be provided as the first argument')

const { ieContract, provider } = await createMeridianContract()

const signer = ethers.Wallet.fromPhrase(WALLET_SEED, provider)
console.log(
  'Wallet address:',
  signer.address,
  newDelegatedEthAddress(/** @type {any} */(signer.address), CoinType.MAIN).toString()
)
const ieContractWithSigner = ieContract.connect(signer)

console.log('Going to revoke %s', tx)
