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

/*
https://filecoinproject.slack.com/archives/C0179RNEMU4/p1607269156412600?thread_ts=1606987775.021900&cid=C0179RNEMU4
while you can not cancel a message, you have an option to replace it with a different one, in this
case you would replace it with the cheapest possible one ( a self-send of 0 ) in order to "convince"
the network to take the new message all you have to do is to increase your --gas-premium by 25.2%
from the previous value ( technically it is 25% + 1 attofil, but 25.2 is easier )

you also must LOWER your gas-limit to match the new gas requirements of the message you are sending:
if you do not do so, the network will penalize you for lying ( you said this will cost X gas but
used 1/10th of X to execute: no good )

all in all the steps to replace a message (currently to be fixed soon) are: - you go to some
explorer, find a recent send message, that just landed on chain - you note what is its gas-used
(RECENTGU) and gas-feecap (RECENTFC) - you find your own message you want to replace and find its
current gas-premium (OLDGP) - then you do magic:

lotus send
  --gas-feecap $RECENTFC-from-above \
  --gas-limit $RECENTGU*1.1 \
  --gas-premium $OLDGP*1.252  \
  --from {{address-you-are-unblocking}} \
  --nonce {{nonce-you-are-replacing}} \
  {{address-you-are-unblocking-as-destination}} \
  0

that should be all, and should not cost you much at all ( we just cleared ~80 messages in the thread
above by paying ~0.05 fil
 */
