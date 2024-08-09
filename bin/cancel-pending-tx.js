// dotenv must be imported before importing anything else
import 'dotenv/config'

import assert from 'node:assert'
import { RPC_URL, rpcHeaders } from '../lib/config.js'
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
const walletDelegatedAddress = newDelegatedEthAddress(/** @type {any} */(signer.address), CoinType.MAIN).toString()
console.log(
  'Wallet address:',
  signer.address,
  walletDelegatedAddress
)
const ieContractWithSigner = ieContract.connect(signer)

console.log('Going to revoke %s', tx)

const messages = await getMessagesInMempool(walletDelegatedAddress)
const txDetails = messages.find(m => m.cid === tx)
if (!txDetails) {
  console.log('Transaction message not found in the mempool.')
  console.log('https://filfox.info/en/message/%s', tx)
  console.log('Note: we have access to 50 oldest messages only.')
  for (const { cid, createTimestamp } of messages) {
    console.log(' %s (created at %s)', cid, new Date(createTimestamp * 1000).toISOString())
  }
  process.exit(1)
}
console.log('TX message:', txDetails)
/* example data:
TX message: {
  cid: 'bafy2bzacecblagyenhtbrri4vwslq4ybv42nqmu7j7ltagfd2sefg4sne72eo',
  from: 'f410fj3g4re56wcisdzhvzo5enhjt6x7wdbccfitupvq',
  to: 'f410fqrqhm3w4mk2sl7a7utlcr7dzeko4ombrjptgwui',
  nonce: 74574,
  value: '0',
  gasLimit: 4704651802,
  gasFeeCap: '368268',
  gasPremium: '233112',
  method: 'InvokeContract',
  methodNumber: 3844450837,
  evmMethod: '',
  createTimestamp: 1723077205
}
*/

/*
TODO:

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

/**
 * @param {string} f4addr
 * @returns 50 oldest messages
 */
async function getMessagesInMempool (f4addr) {
  const res = await fetch(`https://filfox.info/api/v1/message/mempool/filtered-list?address=${f4addr}`)
  if (!res.ok) {
    throw new Error(`Filfox request failed with ${res.status}: ${(await res.text()).trimEnd()}`)
  }
  /** @type {{
     messages: {
       cid: string;
       from: string;
       to: string;
       nonce: number;
       value: string;
       gasLimit: number;
       gasFeeCap: string;
       gasPremium: string;
       method: string;
       methodNumber: number;
       evmMethod: string;
       createTimestamp: number;
     }[];
   }}
   */
  const { messages } = /** @type {any} */(await res.json())
  return messages
}

/**
 * @param {string} method
 * @param {unknown[]} params
 */
async function rpc (method, ...params) {
  const req = new Request(RPC_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accepts: 'application/json',
      ...rpcHeaders
    },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method,
      params
    })
  })
  const res = await fetch(req, {
    signal: AbortSignal.timeout(60_000)
  })

  if (!res.ok) {
    throw new Error(`JSON RPC failed with ${res.status}: ${(await res.text()).trimEnd()}`)
  }

  const body = /** @type {any} */(await res.json())
  if (body.error) {
    const err = new Error(body.error.message)
    err.name = 'FilecoinRpcError'
    Object.assign(err, { code: body.code })
    throw err
  }

  return body.result
}
