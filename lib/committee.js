import assert from 'node:assert'
import createDebug from 'debug'

/** @import {Measurement} from './preprocess.js' */
/** @import {RetrievalResult, CommitteeCheckError} from './typings.js' */

const debug = createDebug('spark:committee')

const REQUIRED_COMMITTEE_SIZE = 30

export class Committee {
  /** @type {Measurement[]} */
  #measurements

  /**
   * @param {Pick<Measurement, 'cid' | 'minerId'>} retrievalTask
   */
  constructor ({ cid, minerId }) {
    this.retrievalTask = { minerId, cid }

    this.#measurements = []

    /** @type {string  | CommitteeCheckError} */
    this.indexerResult = 'MAJORITY_NOT_FOUND'

    /** @type {RetrievalResult} */
    this.retrievalResult = 'MAJORITY_NOT_FOUND'
  }

  get size () {
    return this.#measurements.length
  }

  get measurements () {
    const ret = [...this.#measurements]
    Object.freeze(ret)
    return ret
  }

  /**
   * @param {Measurement} m
   */
  addMeasurement (m) {
    assert.strictEqual(m.cid, this.retrievalTask.cid, 'cid must match')
    assert.strictEqual(m.minerId, this.retrievalTask.minerId, 'minerId must match')
    assert.strictEqual(m.fraudAssessment, 'OK', 'only accepted measurements can be added')
    this.#measurements.push(m)
  }

  /**
   * @param {number} requiredCommitteeSize
   * @returns
   */
  evaluate (requiredCommitteeSize = REQUIRED_COMMITTEE_SIZE) {
    debug(
      'Evaluating task %o with a committee of %s measurements',
      this.retrievalTask,
      this.#measurements.length
    )

    if (this.#measurements.length < requiredCommitteeSize) {
      debug('→ committee is too small (size=%s required=%s); retrievalResult=COMMITTEE_TOO_SMALL',
        this.#measurements.length,
        requiredCommitteeSize
      )
      this.indexerResult = 'COMMITTEE_TOO_SMALL'
      this.retrievalResult = 'COMMITTEE_TOO_SMALL'
      this.#measurements.forEach(m => { m.fraudAssessment = 'COMMITTEE_TOO_SMALL' })
      return
    }

    debug('- searching for majority in indexer results')
    /** @type {(keyof Measurement)[]} */
    const indexerResultProps = [
      'providerId',
      'indexerResult',
      'provider_address',
      'protocol'
    ]
    const indexerResultMajority = this.#findMajority(indexerResultProps)
    if (indexerResultMajority) {
      this.indexerResult = indexerResultMajority.majorityValue.indexerResult
    }

    debug('- searching for majority in retrieval results')
    /** @type {(keyof Measurement)[]} */
    const retrievalResultProps = [
      ...indexerResultProps,
      'retrievalResult'
      // TODO
      // - status_code,
      // - timeout,
      // - car size
      // - car checksum
      // - car too large
    ]

    const retrievalResultMajority = this.#findMajority(retrievalResultProps)
    if (retrievalResultMajority) {
      this.retrievalResult = retrievalResultMajority.majorityValue.retrievalResult
      retrievalResultMajority.minorityMeasurements.forEach(m => {
        m.fraudAssessment = 'MINORITY_RESULT'
      })
    } else {
      this.retrievalResult = 'MAJORITY_NOT_FOUND'
      this.#measurements.forEach(m => { m.fraudAssessment = 'MAJORITY_NOT_FOUND' })
    }
  }

  /**
   * @param {(keyof Measurement)[]} measurementFields
   */
  #findMajority (measurementFields) {
    /** @param {Measurement} m */
    const getResult = m => pick(m, ...measurementFields)

    /** @type {Map<string, Measurement[]>} */
    const resultGroups = new Map()

    // 1. Group measurements using the result as the grouping key
    for (const m of this.#measurements) {
      const key = JSON.stringify(getResult(m))
      let list = resultGroups.get(key)
      if (!list) {
        list = []
        resultGroups.set(key, list)
      }
      list.push(m)
    }

    if (debug.enabled) {
      debug('- results found:')
      for (const k of resultGroups.keys()) {
        debug('  %o', JSON.parse(k))
      }
    }

    // 2. Sort the measurement groups by their size
    const keys = Array.from(resultGroups.keys())
    keys.sort(
      (a, b) => (resultGroups.get(a)?.length ?? 0) - (resultGroups.get(b)?.length ?? 0)
    )
    const measurementGroups = keys.map(k => resultGroups.get(k))

    // 3. Find the majority
    const majorityMeasurements = measurementGroups.pop()
    const majoritySize = majorityMeasurements.length
    const majorityValue = getResult(majorityMeasurements[0])

    debug('- majority=%s committee-size=%s value=%o', majoritySize, this.size, majorityValue)
    if (majoritySize <= this.size / 2) {
      debug('→ majority is not absolute; result=MAJORITY_NOT_FOUND')
      return undefined
    } else {
      debug('→ majority agrees on result=%o', majorityValue)
      return {
        majorityValue,
        majorityMeasurements,
        minorityMeasurements: measurementGroups.flat()
      }
    }
  }
}

/**
 * @template T
 * @template {keyof T} K
 * @param {T} obj
 * @param {K[]} keys
 * @returns {Pick<T, K>}
 */
function pick (obj, ...keys) {
  /** @type {any} */
  const ret = {}
  keys.forEach(key => {
    ret[key] = obj[key]
  })
  return ret
}
