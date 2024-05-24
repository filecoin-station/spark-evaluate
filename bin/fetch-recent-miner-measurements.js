// dotenv must be imported before importing anything else
import 'dotenv/config'

import { Point } from '@influxdata/influxdb-client'
import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import fs, { mkdir, readFile, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import pMap from 'p-map'
import { IE_CONTRACT_ADDRESS } from '../lib/config.js'
import { createMeridianContract } from '../lib/ie-contract.js'
import { fetchMeasurements, preprocess } from '../lib/preprocess.js'
import { RoundData } from '../lib/round.js'

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
  args.unshift(IE_CONTRACT_ADDRESS)
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

await run(contractAddress, minerId, blocksToQuery)

async function run (contractAddress, minerId, blocksToQuery) {
  console.error('Querying the chain for recent MeasurementsAdded events')
  const measurementCids = await getRecentMeasurementsAddedEvents(contractAddress, blocksToQuery)
  console.error(' → found %s events', measurementCids.length)

  console.error('Fetching measurements from IPFS')
  const roundIndex = 0n
  const round = new RoundData(roundIndex)
  await pMap(
    measurementCids,
    cid => fetchAndLoadMeasurements(round, cid),
    { concurrency: os.cpus().length })
  for (const cid of measurementCids) {
    await fetchAndLoadMeasurements(round, cid)
  }
  console.error(' → fetched %s measurements', round.measurements.length)

  const measurements = round.measurements.filter(m => m.minerId === minerId)
  console.error('Found %s measurements for miner %s', measurements.length, minerId)
  if (!measurements.length) return

  console.error('Printing first 10 measurements')
  /**
   * @param {import('../lib/preprocess.js').Measurement} m
   * @returns {string}
   */
  const formatMeasurement = (m) => [
    new Date(m.finished_at).toISOString(),
    m.cid.padEnd(70),
    m.retrievalResult
  ].join(' ')
  const header = [
    'Timestamp'.padEnd(new Date().toISOString().length),
    'CID'.padEnd(70),
    'RetrievalResult'
  ].join(' ')

  console.log(header)
  for (const m of measurements.slice(0, 10)) {
    console.log(formatMeasurement(m))
  }

  let outfile = 'measurements-all.ndjson'
  await writeFile(outfile, round.measurements.map(m => JSON.stringify(m) + '\n').join(''))
  console.error('Wrote all raw measurements to %s', outfile)

  outfile = `measurements-${minerId}.ndjson`
  await writeFile(outfile, measurements.map(m => JSON.stringify(m) + '\n').join(''))
  console.error('Wrote %s raw measurements to %s', minerId, outfile)

  outfile = `measurements-${minerId}.txt`
  const text = header + '\n' + measurements.map(m => formatMeasurement(m) + '\n').join('')
  await writeFile(outfile, text)
  console.error('Wrote human-readable summary for %s to %s', minerId, outfile)
}

/**
 * @param {string} contractAddress
 * @param {number} blocksToQuery
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
 */
async function fetchMeasurementsWithCache (cid) {
  const pathOfCachedResponse = path.join(cacheDir, cid + '.json')
  try {
    const measurements = JSON.parse(await readFile(pathOfCachedResponse, 'utf-8'))
    debug('Loaded %s from cache', cid)
    return measurements
  } catch (err) {
    if (err.code !== 'ENOENT') console.warn('Cannot read cached measurements:', err)
  }

  debug('Fetching %s from web3.storage', cid)
  const measurements = await fetchMeasurements(cid)
  await writeFile(pathOfCachedResponse, JSON.stringify(measurements))
  return measurements
}

/**
 * @param {RoundData} round
 * @param {string} cid
 */
async function fetchAndLoadMeasurements (round, cid) {
  try {
    await preprocess({
      roundIndex: round.index,
      round,
      cid,
      fetchMeasurements: fetchMeasurementsWithCache,
      recordTelemetry,
      logger: { log: debug, error: debug },
      fetchRetries: 0
    })
    console.error(' ✓ %s', cid)
  } catch (err) {
    console.error(' × Skipping %s:', cid, err.message)
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
