import assert from 'node:assert'
import crypto from 'node:crypto'
import closest from 'k-closest'

//
// IMPORTANT: This algorithm must be in sync with the tasker in Spark checker nodes
// https://github.com/filecoin-station/spark/blob/main/lib/tasker.js
//

/** @import { RetrievalTask, RoundDetails } from './typings.js' */
/** @typedef {RetrievalTask & { key: bigint}} KeyedRetrievalTask */

const textEncoder = new TextEncoder()

/**
 * @param {Pick<RoundDetails, 'retrievalTasks' | 'maxTasksPerNode'>} roundDetails
 * @param {string} randomness
 * @param {import('./preprocess.js').Measurement[]} measurements
 */
export async function getTasksAllowedForStations (roundDetails, randomness, measurements) {
  const keyedTasks = await Promise.all(roundDetails.retrievalTasks.map(
    async (t) => ({ ...t, key: await getTaskKey(t, randomness) })
  ))

  // k-closest does not handle the edge case where K is larger than the input array length
  const numberOfTasksToPick = Math.min(keyedTasks.length, roundDetails.maxTasksPerNode)

  const seeker = new closest.Seeker([...keyedTasks], getStationTaskDistance)

  /** @type {Map<string, RetrievalTask[]>} */
  const tasksAllowedForStations = new Map()
  for (const m of measurements) {
    if (tasksAllowedForStations.has(m.stationId)) continue

    const stationKey = await getStationKey(m.stationId)
    const allowedTasks = seeker
      .wHeap(stationKey, numberOfTasksToPick)
      .map(({ key, ...t }) => (t))
    tasksAllowedForStations.set(m.stationId, allowedTasks)
  }

  return tasksAllowedForStations
}

/**
 * @param {bigint} stationKey
 * @param {KeyedRetrievalTask} task
 */
function getStationTaskDistance (stationKey, task) {
  return stationKey ^ task.key
}

/**
 * The function `encodeHex` is a platform API provided by Deno stdlib. We re-implement it here so
 * that the business logic part can stay as similar to what we have in Spark module as possible.
 * @param {ArrayBuffer} arrayBuffer
 */
function encodeHex (arrayBuffer) {
  return Buffer.from(arrayBuffer).toString('hex')
}

/**
 * @param {RetrievalTask} task
 * @param {string} randomness
 * @returns
 */
export async function getTaskKey (task, randomness) {
  assert.strictEqual(typeof task, 'object', 'task must be an object')
  assert.strictEqual(typeof task.cid, 'string', 'task.cid must be a string')
  assert.strictEqual(typeof task.minerId, 'string', 'task.minerId must be a string')
  assert.strictEqual(typeof randomness, 'string', 'randomness must be a string')

  const data = [task.cid, task.minerId, randomness].join('\n')
  const hash = await crypto.subtle.digest('sha-256', textEncoder.encode(data))
  return BigInt('0x' + encodeHex(hash))
}

/**
 * @param {string} stationId
 */
export async function getStationKey (stationId) {
  assert.strictEqual(typeof stationId, 'string', 'stationId must be a string')

  const hash = await crypto.subtle.digest('sha-256', textEncoder.encode(stationId))
  return BigInt('0x' + encodeHex(hash))
}
