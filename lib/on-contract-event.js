// eth_getFilterChanges isn't fully supported by lotus, see
// https://filecoinproject.slack.com/archives/CRK2LKYHW/p1705404161018769
// Therefore we need to poll for events

import { ethers } from 'ethers'
import timers from 'node:timers/promises'

const MIN_POLL_INTERVAL = 10_000

const makeRequest = async ({
  contract,
  address,
  rpcUrl,
  rpcHeaders,
  fromBlock
}) => {
  const req = {
    jsonrpc: '2.0',
    id: 1,
    method: 'eth_getLogs',
    params: [
      {
        address,
        fromBlock: ethers.toBeHex(fromBlock)
      }
    ]
  }
  const res = await fetch(rpcUrl, {
    method: 'POST',
    headers: {
      ...rpcHeaders,
      'content-type': 'application/json'
    },
    body: JSON.stringify(req)
  })
  if (!res.ok) {
    throw new Error(
      `Cannot fetch event log. JSON RPC error ${res.status}\n${await res.text()}`
    )
  }
  const body = await res.json()
  if (body.error) {
    throw new Error(`Cannot fetch event log. JSON RPC error: ${body.error}`)
  }

  const events = body.result.map(log => contract.interface.parseLog(log))
  let lastBlock
  if (body.result.length > 0) {
    lastBlock = body.result
      .map(r => ethers.getBigInt(r.blockNumber))
      .sort()
      .pop()
  }
  return { events, lastBlock }
}

export async function * onContractEvent ({
  contract,
  provider,
  rpcUrl,
  rpcHeaders
}) {
  const address = await contract.getAddress()
  let lastBlock = await provider.getBlockNumber()

  while (true) {
    const iterationStart = new Date()
    let events, _lastBlock

    try {
      ({
        events,
        lastBlock: _lastBlock
      } = await makeRequest({
        contract,
        address,
        rpcUrl,
        rpcHeaders,
        fromBlock: lastBlock
      }))
    } catch (err) {
      console.error(err)
    }

    if (events) {
      for (const event of events) {
        if (event !== null) yield event
      }
    }

    if (_lastBlock) {
      lastBlock = _lastBlock
    }

    const iterationEnd = new Date()
    const iterationDuration = iterationEnd - iterationStart
    if (iterationDuration < MIN_POLL_INTERVAL) {
      await timers.setTimeout(MIN_POLL_INTERVAL - iterationDuration)
    }
  }
}
