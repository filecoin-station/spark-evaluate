// dotenv must be imported before importing anything else
import 'dotenv/config'

import { IE_CONTRACT_ADDRESS, RPC_URL, rpcHeaders } from '../lib/config.js'
import { preprocess, fetchMeasurements } from '../lib/preprocess.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
// import { Point } from '../lib/telemetry.js'
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { ethers } from 'ethers'
import { RoundData } from '../lib/round.js'

import crypto from 'node:crypto'

const cacheDir = fileURLToPath(new URL('../.cache', import.meta.url))
await mkdir(cacheDir, { recursive: true })

const [nodePath, selfPath, ...args] = process.argv
if (args.length === 0 || !args[0].startsWith('0x')) {
  args.unshift(IE_CONTRACT_ADDRESS)
}
const [contractAddress, roundIndexStr, ...measurementCids] = args

const USAGE = `
Usage:
  ${nodePath} ${selfPath} [contract-address] <round-index> [measurements-cid-1 [cid2 [cid3...]]]
`

if (!contractAddress) {
  console.error('Missing required argument: contractAddress')
  console.log(USAGE)
  process.exit(1)
}

/** @type {bigint} */
let roundIndex
if (roundIndexStr) {
  roundIndex = BigInt(roundIndexStr)
} else {
  console.log('Round index not specified, fetching the last round index from the smart contract')
  const currentRoundIndex = await fetchLastRoundIndex()
  roundIndex = BigInt(currentRoundIndex - 2n)
}

if (!measurementCids.length) {
  measurementCids.push(...(await fetchMeasurementsAddedEvents(roundIndex)))
}

if (!measurementCids.length) {
  console.error(
    "No measurements for round %s were found in smart-contract's event log.",
    roundIndex
  )
  process.exit(1)
}

// const recordTelemetry = (measurementName, fn) => {
//   const point = new Point(measurementName)
//   fn(point)
//   console.log('TELEMETRY %s %o', measurementName, point.fields)
// }

const fetchMeasurementsWithCache = async (cid) => {
  const pathOfCachedResponse = path.join(cacheDir, cid + '.json')
  try {
    return JSON.parse(await readFile(pathOfCachedResponse, 'utf-8'))
  } catch (err) {
    if (err.code !== 'ENOENT') console.warn('Cannot read cached measurements:', err)
  }

  const measurements = await fetchMeasurements(cid)
  await writeFile(pathOfCachedResponse, JSON.stringify(measurements))
  return measurements
}

console.log('Evaluating round %s of contract %s', roundIndex, contractAddress)

const { retrievalTasks } = await fetchRoundDetails(contractAddress, roundIndex, () => {})
console.log('Retrieval tasks: %s', retrievalTasks.length)

measurementCids.splice(3)
console.log('==PREPROCESS==')
const round = new RoundData(roundIndex)
for (const cid of measurementCids) {
  try {
    await preprocess({
      roundIndex,
      round,
      cid,
      fetchMeasurements: fetchMeasurementsWithCache,
      recordTelemetry: () => {},
      logger: console,
      fetchRetries: 0
    })
  } catch (err) {
    console.error(err)
  }
}

console.log('Fetched %s measurements', round.measurements.length)

console.log('==EXPERIMENT==')
const started = Date.now()

/** @type {Map<string, BigInt>} */
const nodes = new Map()
printDuration('build station node keys', async () => {
  for (const { stationId } of round.measurements) {
    if (nodes.has(stationId)) continue
    const key = BigInt(`0x${stationId.slice(-64)}`)
    // console.log("mapped station %s to %s", stationId, key);
    nodes.set(stationId, key)
  }
  console.log('Station count: %s', nodes.size)
})

const drand = '3439d92d58e47d342131d446a3abe264396dd264717897af30525c98408c834f'
const keyedTasks = await printDuration('map tasks to keys', async () => {
  const keyed = await Promise.all(
    retrievalTasks.map(async (task) => {
      const hash = await crypto.subtle.digest(
        'sha-256',
        Buffer.from([task.cid, task.minerId, drand].join('\n'))
      )
      const hashStr = Buffer.from(hash).toString('hex')
      const key = BigInt(`0x${hashStr}`)
      // console.log("mapped task %o to hash %s key %s", task, hashStr, key);
      return { ...task, key }
    })
  )
  console.log('Task count: %s', keyed.length)
  return keyed
})

// TODO

console.log('Duration: %sms', Date.now() - started)
console.log(process.memoryUsage())

/**
 * @param {bigint} roundIndex
 * @returns {Promise<string[]>}
 */
async function fetchMeasurementsAddedEvents (roundIndex) {
  const pathOfCachedResponse = path.join(cacheDir, 'round-' + roundIndex + '.json')
  try {
    return JSON.parse(await readFile(pathOfCachedResponse, 'utf-8'))
  } catch (err) {
    if (err.code !== 'ENOENT') console.warn('Cannot read cached list of measurement CIDs:', err)
  }

  const list = await fetchMeasurementsAddedFromChain(roundIndex)
  await writeFile(pathOfCachedResponse, JSON.stringify(list))
  return list
}

async function createIeContract () {
  if (RPC_URL.includes('glif') && !process.env.GLIF_TOKEN) {
    throw new Error('Missing required env var GLIF_TOKEN. See https://api.node.glif.io/')
  }

  const fetchRequest = new ethers.FetchRequest(RPC_URL)
  fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
  const provider = new ethers.JsonRpcProvider(fetchRequest, null, { polling: true })
  // provider.on('debug', console.log)
  const ieContract = new ethers.Contract(
    contractAddress,
    JSON.parse(await readFile(fileURLToPath(new URL('../lib/abi.json', import.meta.url)), 'utf8')),
    provider
  )

  return { provider, ieContract }
}

async function fetchMeasurementsAddedFromChain (roundIndex) {
  const { provider, ieContract } = await createIeContract()

  console.log('Fetching MeasurementsAdded events for round %s from the ledger', roundIndex)

  const blockNumber = await provider.getBlockNumber()
  // console.log('Current block number', blockNumber)

  // TODO: filter only measurements for the given `roundIndex`
  // See https://github.com/Meridian-IE/impact-evaluator/issues/57

  // max look-back period allowed by Glif.io is 2000 blocks (approx 16h40m)
  // SPARK round is ~20 minutes, i.e. ~40 blocks
  const rawEvents = await ieContract.queryFilter(
    'MeasurementsAdded',
    blockNumber - 200,
    blockNumber - 1
  )

  /** @type {Array<{ cid: string, roundIndex: bigint, sender: string }>} */
  const events = rawEvents
    .filter(isEventLog)
    .map(({ args: [cid, roundIndex, sender] }) => ({ cid, roundIndex, sender }))
  // console.log('events', events)

  const prev = roundIndex - 1n
  const prevFound = events.some((e) => e.roundIndex === prev)
  if (!prevFound) {
    console.error(
      'Incomplete round data. No measurements from the previous round %s were found.',
      prev.toString()
    )
    process.exit(1)
  }

  const next = roundIndex + 1n
  const nextFound = events.some((e) => e.roundIndex === next)
  if (!nextFound) {
    console.error(
      'Incomplete round data. No measurements from the next round %s were found.',
      next.toString()
    )
    process.exit(1)
  }

  return events.filter((e) => e.roundIndex === roundIndex).map((e) => e.cid)
}

/**
 * @param {ethers.Log | ethers.EventLog} logOrEventLog
 * @returns {logOrEventLog is ethers.EventLog}
 */
function isEventLog (logOrEventLog) {
  return 'args' in logOrEventLog
}

async function fetchLastRoundIndex () {
  const { ieContract } = await createIeContract()
  return await ieContract.currentRoundIndex()
}

async function printDuration (label, fn) {
  const started = Date.now()
  const result = await fn()
  console.log('%s: finished in %sms', label, Date.now() - started)
  return result
}
