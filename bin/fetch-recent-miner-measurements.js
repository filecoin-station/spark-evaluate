// dotenv must be imported before importing anything else
import 'dotenv/config'

import { Point } from '@influxdata/influxdb-client'
import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import fs from 'node:fs'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import pMap from 'p-map'
import { createMeridianContract } from '../lib/ie-contract.js'
import { fetchMeasurements, preprocess } from '../lib/preprocess.js'
import { RoundData } from '../lib/round.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'

const {
  STORE_ALL
} = process.env

Sentry.init({
  dsn: 'https://d0651617f9690c7e9421ab9c949d67a4@o1408530.ingest.sentry.io/4505906069766144',
  environment: process.env.SENTRY_ENVIRONMENT || 'dry-run',
  // Performance Monitoring
  tracesSampleRate: 0.1 // Capture 10% of the transactions
})

const debug = createDebug('spark:bin')

const cacheDir = path.resolve('.cache')
await mkdir(cacheDir, { recursive: true })

const [nodePath, selfPath, ...args] = process.argv
if (args.length === 0 || !args[0].startsWith('0x')) {
  args.unshift(SparkImpactEvaluator.ADDRESS)
}
const [contractAddress, minerId, blocksToQuery] = args

const USAGE = `
Usage:
  ${nodePath} ${selfPath} [contract-address] minerId [blocksToQuery]
`

if (!contractAddress) {
  console.error('Missing required argument: contractAddress')
  console.error(USAGE)
  process.exit(1)
}

if (!minerId) {
  console.error('Missing required argument: minerId')
  console.error(USAGE)
  process.exit(1)
}

if (!minerId.startsWith('f0')) {
  console.warn('Warning: miner id %s does not start with "f0", is it a valid miner address?', minerId)
}

console.error('Querying the chain for recent MeasurementsAdded events')
const measurementCids = await getRecentMeasurementsAddedEvents(contractAddress, blocksToQuery)
console.error(' → found %s events', measurementCids.length)

const ALL_MEASUREMENTS_FILE = 'measurements-all.ndjson'
const MINER_DATA_FILE = `measurements-${minerId}.ndjson`
const MINER_SUMMARY_FILE = `measurements-${minerId}.txt`

const allMeasurementsWriter = !!STORE_ALL && STORE_ALL.toLowerCase() !== 'false' && STORE_ALL !== '0'
  ? fs.createWriteStream(ALL_MEASUREMENTS_FILE)
  : undefined

const minerDataWriter = fs.createWriteStream(MINER_DATA_FILE)
const minerSummaryWriter = fs.createWriteStream(MINER_SUMMARY_FILE)
minerSummaryWriter.write(formatHeader() + '\n')

const abortController = new AbortController()
const signal = abortController.signal
process.on('SIGINT', () => abortController.abort(new Error('interrupted')))

try {
  await pMap(measurementCids, fetchAndProcess, { concurrency: os.availableParallelism() })
} catch (err) {
  if (signal.aborted) {
    console.error('Interrupted, exiting. Output files contain partial data.')
  } else {
    throw err
  }
}

if (allMeasurementsWriter) {
  console.error('Wrote (ALL) raw measurements to %s', ALL_MEASUREMENTS_FILE)
}
console.error('Wrote (minerId=%s) raw measurements to %s', minerId, MINER_DATA_FILE)
console.error('Wrote human-readable summary for %s to %s', minerId, MINER_SUMMARY_FILE)

/**
 * @param {string} contractAddress
 * @param {number | string} blocksToQuery
 * @returns
 */
async function getRecentMeasurementsAddedEvents (contractAddress, blocksToQuery = Number.POSITIVE_INFINITY) {
  const { ieContract } = await createMeridianContract(contractAddress)

  // max look-back period allowed by Glif.io is 2000 blocks (approx 16h40m)
  // in practice, requests for the last 2000 blocks are usually rejected,
  // so it's safer to use a slightly smaller number
  const fromBlock = Math.max(-blocksToQuery, -1990)
  debug('queryFilter(MeasurementsAdded, %s)', fromBlock)
  const rawEvents = await ieContract.queryFilter('MeasurementsAdded', fromBlock)

  /** @type {Array<{ cid: string, roundIndex: bigint, sender: string }>} */
  const events = rawEvents
    .filter(isEventLog)
    .map(({ args: [cid, roundIndex, sender] }) => ({ cid, roundIndex, sender }))
  // console.log('events', events)

  return events.map(e => e.cid)
}

/**
 * @param {string} cid
 * @param {object} options
 * @param {AbortSignal} [options.signal]
 */
async function fetchMeasurementsWithCache (cid, { signal }) {
  const pathOfCachedResponse = path.join(cacheDir, cid + '.json')
  try {
    const measurements = JSON.parse(
      await readFile(pathOfCachedResponse, { encoding: 'utf-8', signal })
    )
    debug('Loaded %s from cache', cid)
    return measurements
  } catch (err) {
    if (signal.aborted) return
    if (err.code !== 'ENOENT') console.warn('Cannot read cached measurements:', err)
  }

  debug('Fetching %s from web3.storage', cid)
  const measurements = await fetchMeasurements(cid, { signal })
  await writeFile(pathOfCachedResponse, JSON.stringify(measurements))
  return measurements
}

/**
 * @param {string} cid
 */
async function fetchAndProcess (cid) {
  const roundIndex = 0n
  const round = new RoundData(roundIndex)
  try {
    await preprocess({
      roundIndex: round.index,
      round,
      cid,
      fetchMeasurements: cid => fetchMeasurementsWithCache(cid, { signal }),
      recordTelemetry,
      logger: { log: debug, error: debug },
      fetchRetries: 0
    })

    if (allMeasurementsWriter) {
      allMeasurementsWriter.write(round.measurements.map(m => JSON.stringify(m) + '\n').join(''))
    }

    const minerMeasurements = round.measurements.filter(m => m.minerId === minerId)
    minerDataWriter.write(minerMeasurements.map(m => JSON.stringify(m) + '\n').join(''))
    minerSummaryWriter.write(minerMeasurements.map(m => formatMeasurement(m) + '\n').join(''))

    console.error(' ✓ %s', cid)
  } catch (err) {
    if (signal.aborted) return
    console.error(' × Skipping %s:', cid, err.message)
    debug(err)
  }
}

/**
 * @param {import('ethers').Log | import('ethers').EventLog} logOrEventLog
 * @returns {logOrEventLog is import('ethers').EventLog}
 */
function isEventLog (logOrEventLog) {
  return 'args' in logOrEventLog
}

/**
 * @param {string} measurementName
 * @param {(point: Point) => void} fn
 */
function recordTelemetry (measurementName, fn) {
  const point = new Point(measurementName)
  fn(point)
  debug('TELEMETRY %s %o', measurementName, point.fields)
}

/**
 * @param {import('../lib/preprocess.js').Measurement} m
 */
function formatMeasurement (m) {
  return [
    new Date(m.finished_at).toISOString(),
    (m.cid ?? '').padEnd(70),
    (m.protocol ?? '').padEnd(10),
    (m.retrievalResult ?? '')
  ].join(' ')
}

function formatHeader () {
  return [
    'Timestamp'.padEnd(new Date().toISOString().length),
    'CID'.padEnd(70),
    'Protocol'.padEnd(10),
    'RetrievalResult'
  ].join(' ')
}
