import { IE_CONTRACT_ADDRESS, RPC_URL, rpcHeaders } from '../lib/config.js'
import { evaluate } from '../lib/evaluate.js'
import { preprocess, fetchMeasurements } from '../lib/preprocess.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
import { Point } from '../lib/telemetry.js'
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { ethers } from 'ethers'
import { RoundData } from '../lib/round.js'

const cacheDir = fileURLToPath(new URL('../.cache', import.meta.url))
await mkdir(cacheDir, { recursive: true })

const [nodePath, selfPath, ...args] = process.argv
if (!args[0].startsWith('0x')) {
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

if (!roundIndexStr) {
  console.error('Missing required argument: roundIndex')
  console.log(USAGE)
  process.exit(1)
}
const roundIndex = Number(roundIndexStr)

if (!measurementCids.length) {
  measurementCids.push(...(await fetchMeasurementsAddedEvents(BigInt(roundIndex))))
}

if (!measurementCids.length) {
  console.error('No measurements for round %s were found in smart-contract\'s event log.', roundIndex)
  process.exit(1)
}

const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  console.log('TELEMETRY %s %o', measurementName, point.fields)
}

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

console.log('==PREPROCESS==')
const round = new RoundData(roundIndex)
for (const cid of measurementCids) {
  await preprocess({
    roundIndex,
    round,
    cid,
    fetchMeasurements: fetchMeasurementsWithCache,
    recordTelemetry,
    logger: console
  })
}

console.log('Fetched %s measurements', round.measurements.length)

console.log('==EVALUATE==')
const ieContractWithSigner = {
  async getAddress () {
    return contractAddress
  },
  async setScores (_roundIndex, participantAddresses, scores) {
    console.log('==EVALUATION RESULTS==')
    console.log('participants:', participantAddresses)
    console.log('scores:', scores)
    console.log('==END OF RESULTS==')
    return { hash: '0x234' }
  }
}

await evaluate({
  roundIndex,
  round,
  fetchRoundDetails,
  ieContractWithSigner,
  logger: console,
  recordTelemetry,

  // We don't want dry runs to update data in `sparks_stats`, therefore we are passing a stub
  // connection factory that creates no-op clients. This also keeps the setup simpler. The person
  // executing a dry run does not need access to any Postgres instance.
  // Evaluate uses the PG client only for updating the statistics, it's not reading any data.
  // Thus it's safe to inject a no-op client.
  createPgClient: createNoopPgClient
})

console.log(process.memoryUsage())

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

async function fetchMeasurementsAddedFromChain (roundIndex) {
  const fetchRequest = new ethers.FetchRequest(RPC_URL)
  fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
  const provider = new ethers.JsonRpcProvider(
    fetchRequest,
    null,
    { batchMaxCount: 1 }
  )
  // provider.on('debug', console.log)
  const ieContract = new ethers.Contract(
    contractAddress,
    JSON.parse(
      await readFile(
        fileURLToPath(new URL('../lib/abi.json', import.meta.url)),
        'utf8'
      )
    ),
    provider
  )

  console.log('Fetching MeasurementsAdded events from the ledger')

  const blockNumber = await provider.getBlockNumber()
  // console.log('Current block number', blockNumber)

  // TODO: filter only measurements for the given `roundIndex`
  // See https://github.com/Meridian-IE/impact-evaluator/issues/57

  // max look-back period allowed by Glif.io is 2000 blocks (approx 16h40m)
  // SPARK round is ~60 minutes, i.e. ~120 blocks
  const rawEvents = await ieContract.queryFilter('MeasurementsAdded', blockNumber - 1800, 'latest')

  /** @type {Array<{ cid: string, roundIndex: bigint, sender: string }>} */
  const events = rawEvents.map(({ args: [cid, roundIndex, sender] }) => ({ cid, roundIndex, sender }))
  console.log('events', events)

  const prev = roundIndex - 1n
  const prevFound = events.some(e => e.roundIndex === prev)
  if (!prevFound) {
    console.error(
      'Incomplete round data. No measurements from the previous round %s were found.',
      prev.toString()
    )
    process.exit(1)
  }

  const next = roundIndex + 1n
  const nextFound = events.some(e => e.roundIndex === next)
  if (!nextFound) {
    console.error(
      'Incomplete round data. No measurements from the next round %s were found.',
      next.toString()
    )
    process.exit(1)
  }

  return events.filter(e => e.roundIndex === roundIndex).map(e => e.cid)
}

function createNoopPgClient () {
  return {
    async query () {
      return { rows: [] }
    },
    async end () {
      // no-op
    }
  }
}
