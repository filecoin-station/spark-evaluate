/*
Usage:

1. Setup port forwarding between your local computer and Postgres instance hosted by Fly.io
  ([docs](https://fly.io/docs/postgres/connecting/connecting-with-flyctl/)). Remember to use a
  different port if you have a local Postgres server for development!
   ```sh
   fly proxy 5454:5432 -a spark-db
   ```

2. Find spark-db entry in 1Password and get the user and password from the connection string.

3. Run the following command to fetch all measurements, remember to replace "user" and "password"
   with the real credentials:

   ```sh
   DATABASE_URL="postgres://user:password@localhost:5454/spark" node bin/fetch-historic-measurements.js [contract-address] <range-start> <range-end>
   ```

   This will fetch all measurements committed between range-start (inclusive) and range-end (exclusive)
   and write them to file.
*/

// dotenv must be imported before importing anything else
import 'dotenv/config'

import { Point } from '@influxdata/influxdb-client'
import createDebug from 'debug'
import fs from 'node:fs'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import pMap from 'p-map'
import { fetchMeasurements, preprocess } from '../lib/preprocess.js'
import { RoundData } from '../lib/round.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import pg from 'pg'

const { DATABASE_URL } = process.env

const debug = createDebug('spark:bin')

const cacheDir = path.resolve('.cache')
await mkdir(cacheDir, { recursive: true })

const [nodePath, selfPath, ...args] = process.argv
if (args.length === 0 || !args[0].startsWith('0x')) {
  args.unshift(SparkImpactEvaluator.ADDRESS)
}
const [contractAddress, startStr, endStr, minerId] = args

const USAGE = `
Usage:
  ${nodePath} ${selfPath} [contract-address] range-start range-end [minerId]

Example:
  ${nodePath} ${selfPath} 2024-10-01T00:00:00Z 2024-10-02T00:00:00Z
`

if (!startStr) {
  console.error('Missing required argument: range-start')
  console.error(USAGE)
  process.exit(1)
}

const start = new Date(startStr)
if (Number.isNaN(start.getTime())) {
  console.error('Invalid range-start: not a valid date-time string')
  console.error(USAGE)
  process.exit(1)
}

if (!endStr) {
  console.error('Missing required argument: range-start')
  console.error(USAGE)
  process.exit(1)
}

const end = new Date(endStr)
if (Number.isNaN(end.getTime())) {
  console.error('Invalid range-end: not a valid date-time string')
  console.error(USAGE)
  process.exit(1)
}

if (minerId && !minerId.startsWith('f0')) {
  console.warn('Warning: miner id %s does not start with "f0", is it a valid miner address?', minerId)
}

console.error('Fetching measurements committed between %j and %j', start, end)
const client = new pg.Client({ connectionString: DATABASE_URL })
await client.connect()

const { rows } = await client.query(`
  SELECT cid, meridian_round FROM commitments
  WHERE published_at >= $1 AND published_at < $2
  `, [
  start,
  end
])

console.error('Found %s commitments', rows.length)

// Group events by rounds

/** @type {{roundIndex: bigint, measurementCids: string[]}[]} */
const rounds = []
for (const { meridian_round: roundIndex, cid } of rows) {
  if (!rounds.length || rounds[rounds.length - 1].roundIndex !== roundIndex) {
    rounds.push({ roundIndex, measurementCids: [] })
  }
  rounds[rounds.length - 1].measurementCids.push(cid)
}

// Discard the first and the last round, because most likely we don't have all events for them
rounds.shift()
rounds.pop()

console.error(' → found %s complete rounds', rounds.length)

const ALL_MEASUREMENTS_FILE = 'measurements-all.ndjson'
const MINER_DATA_FILE = `measurements-${minerId}.ndjson`

const allMeasurementsWriter = fs.createWriteStream(ALL_MEASUREMENTS_FILE)
const minerDataWriter = minerId
  ? fs.createWriteStream(MINER_DATA_FILE)
  : null

const abortController = new AbortController()
const signal = abortController.signal
process.on('SIGINT', () => abortController.abort(new Error('interrupted')))

const resultCounts = {
  total: 0
}

try {
  for (const { roundIndex, measurementCids } of rounds) {
    signal.throwIfAborted()
    await processRound(roundIndex, measurementCids, resultCounts)
  }
} catch (err) {
  if (!signal.aborted) {
    throw err
  }
}

if (signal.aborted) {
  console.error('Interrupted, exiting. Output files contain partial data.')
}

console.log('Found %s valid measurements.', resultCounts.total)
for (const [r, c] of Object.entries(resultCounts)) {
  if (r === 'total') continue
  console.log('  %s %s (%s%)',
    r.padEnd(40),
    String(c).padEnd(10),
    Math.floor(c / resultCounts.total * 10000) / 100
  )
}

if (allMeasurementsWriter) {
  console.error('Wrote (ALL) raw measurements to %s', ALL_MEASUREMENTS_FILE)
}
if (minerDataWriter) {
  console.error('Wrote (minerId=%s) raw measurements to %s', minerId, MINER_DATA_FILE)
}
await client.end()

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
 * @param {bigint} roundIndex
 * @param {string[]} measurementCids
 * @param {Record<string, number>} resultCounts
 */
async function processRound (roundIndex, measurementCids, resultCounts) {
  console.error('Processing round %s', roundIndex)
  const round = new RoundData(roundIndex)

  await pMap(
    measurementCids,
    cid => fetchAndPreprocess(round, cid),
    { concurrency: os.availableParallelism() }
  )
  signal.throwIfAborted()

  for (const m of round.measurements) {
    if (m.minerId !== minerId) continue
    resultCounts.total++
    resultCounts[m.retrievalResult] = (resultCounts[m.retrievalResult] ?? 0) + 1
  }

  if (allMeasurementsWriter && round.measurements.length > 0) {
    allMeasurementsWriter.write(
      round.measurements
        .map(measurement => ndJsonLine({ roundIndex: round.index.toString(), measurement }))
        .join('')
    )
  }

  const minerMeasurements = round.measurements.filter(m => m.minerId === minerId)
  if (minerMeasurements.length > 0) {
    minerDataWriter.write(
      minerMeasurements
        .map(measurement => ndJsonLine({ roundIndex: round.index.toString(), measurement }))
        .join('')
    )
  }
  console.error(' → added %s new measurements from this round', minerMeasurements.length)
}

/**
 * @param {*} obj
 * @returns string
 */
function ndJsonLine (obj) {
  return JSON.stringify(obj) + '\n'
}

/**
 * @param {RoundData} round
 * @param {string} cid
 */
async function fetchAndPreprocess (round, cid) {
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

    console.error(' ✓ %s', cid)
  } catch (err) {
    if (signal.aborted) return
    console.error(' × Skipping %s:', cid, err.message)
    debug(err)
  }
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
