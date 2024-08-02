import createDebug from 'debug'

/** @import {Measurement} from './preprocess.js' */
/** @import {RetrievalResult, CommitteeCheckError} from './typings.js' */

const debug = createDebug('spark:committee')

const REQUIRED_COMMITTEE_SIZE = 30

export class Committee {
  /**
   * @param {Pick<Measurement, 'cid' | 'minerId'>} retrievalTask
   */
  constructor ({ cid, minerId }) {
    this.retrievalTask = { minerId, cid }

    /** @type {Measurement[]} */
    this.measurements = []

    /** @type {string  | CommitteeCheckError} */
    this.indexerResult = 'MAJORITY_NOT_FOUND'

    /** @type {RetrievalResult} */
    this.retrievalResult = 'MAJORITY_NOT_FOUND'
  }

  /**
   * @param {number} requiredCommitteeSize
   * @returns
   */
  evaluate (requiredCommitteeSize = REQUIRED_COMMITTEE_SIZE) {
    debug(
      'Evaluating task %o with a committee of %s measurements',
      this.retrievalTask,
      this.measurements.length
    )

    if (!this.#checkCommitteeSize(requiredCommitteeSize)) return
    if (!this.#checkMeasuredField('providerId')) return
    if (!this.#checkMeasuredField('indexerResult')) return

    // TODO
    // - provider_address,
    // - protocol,

    if (!this.#checkMeasuredField('retrievalResult')) return

    // TODO
    // - status_code,
    // - timeout,
    // - car size
    // - car checksum
    // - car too large

    debug('→ majority agrees on indexerResult=%s retrievalResult=%s', this.indexerResult, this.retrievalResult)
  }

  /**
   * @param {number} requiredCommitteeSize
   * @returns
   */
  #checkCommitteeSize (requiredCommitteeSize) {
    if (this.measurements.length < requiredCommitteeSize) {
      this.#checkFailed('COMMITTEE_TOO_SMALL')
      debug('→ committee is too small (size=%s required=%s); retrievalResult=%s',
        this.measurements.length,
        requiredCommitteeSize,
        this.retrievalResult
      )
      return false
    }

    return true
  }

  /**
   * @template {keyof Measurement} FIELD_NAME
   * @param {FIELD_NAME} fieldName
   */
  #checkMeasuredField (fieldName) {
    /** @type {Map<Measurement[FIELD_NAME], number>} */
    const countsOfValues = new Map()
    let committeeSize = 0
    for (const m of this.measurements) {
      if (m.fraudAssessment !== 'OK') continue
      committeeSize++
      const fieldValue = m[fieldName]
      countsOfValues.set(fieldValue, (countsOfValues.get(fieldValue) ?? 0) + 1)
    }
    const uniqueValues = Array.from(countsOfValues.keys())
    uniqueValues.sort((a, b) => (countsOfValues.get(a) ?? 0) - (countsOfValues.get(b) ?? 0))

    if (debug.enabled) {
      debug('- %s values found: %o', fieldName, Object.fromEntries(countsOfValues.entries()))
    }

    const majorityValue = uniqueValues.pop()
    const majoritySize = countsOfValues.get(majorityValue)
    debug('- %s majority=%s committee-size=%s value=%o', fieldName, majoritySize, committeeSize, majorityValue)
    if (majoritySize <= committeeSize / 2) {
      this.#checkFailed('MAJORITY_NOT_FOUND')
      debug(
        '→ %s majority is not absolute; retrievalResult=%s',
        fieldName,
        this.retrievalResult
      )
      return false
    }

    this.#updateAssessmentOfMeasurements(
      'MINORITY_RESULT',
      m => m[fieldName] !== majorityValue
    )

    if (fieldName in this) {
      // update indexerResult, retrievalResult, etc.
      debug('- updating committee.%s=%s', fieldName, majorityValue)
      Object.assign(this, { [fieldName]: majorityValue })
    }

    return true
  }

  /**
   * @param {CommitteeCheckError} code
   */
  #checkFailed (code) {
    this.retrievalResult = code
    this.#updateAssessmentOfMeasurements(code, _m => true)
  }

  /**
   * @param {CommitteeCheckError} newAssessment
   * @param {(m: Measurement) => boolean} filter
   */
  #updateAssessmentOfMeasurements (newAssessment, filter) {
    for (const m of this.measurements) {
      if (m.fraudAssessment !== 'OK') continue
      if (!filter(m)) continue
      m.fraudAssessment = newAssessment
    }
  }
}
