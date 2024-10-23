import assert from 'node:assert'
import { VALID_TASK, VALID_MEASUREMENT as VALID_MEASUREMENT_BEFORE_ASSESSMENT } from './helpers/test-data.js'
import { Committee } from '../lib/committee.js'

/** @import {Measurement} from '../lib/preprocess.js' */

/** @type {Measurement} */
const VALID_MEASUREMENT = {
  ...VALID_MEASUREMENT_BEFORE_ASSESSMENT,
  fraudAssessment: 'OK'
}
Object.freeze(VALID_MEASUREMENT)

describe('Committee', () => {
  describe('evaluate', () => {
    it('produces OK result when the absolute majority agrees', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })
      // minority result
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'CONTENT_VERIFICATION_FAILED' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: true,
        indexerResult: 'OK',
        hasRetrievalMajority: true,
        retrievalResult: 'OK'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })

    it('rejects committees that are too small', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT })
      c.evaluate({ requiredCommitteeSize: 10 })
      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: false,
        indexerResult: 'COMMITTEE_TOO_SMALL',
        hasRetrievalMajority: false,
        retrievalResult: 'COMMITTEE_TOO_SMALL'
      })
      assert.strictEqual(c.measurements[0].fraudAssessment, 'COMMITTEE_TOO_SMALL')
    })

    it('rejects committees without absolute majority for providerId', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, providerId: 'pubkey1' })
      c.addMeasurement({ ...VALID_MEASUREMENT, providerId: 'pubkey2' })
      c.addMeasurement({ ...VALID_MEASUREMENT, providerId: 'pubkey3' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: false,
        indexerResult: 'MAJORITY_NOT_FOUND',
        hasRetrievalMajority: false,
        retrievalResult: 'MAJORITY_NOT_FOUND'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND'
      ])
    })

    it('finds majority for providerId', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, providerId: 'pubkey1' })
      c.addMeasurement({ ...VALID_MEASUREMENT, providerId: 'pubkey1' })
      // minority result
      c.addMeasurement({ ...VALID_MEASUREMENT, providerId: 'pubkey3' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: true,
        indexerResult: 'OK',
        hasRetrievalMajority: true,
        retrievalResult: 'OK'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })

    it('rejects committees without absolute majority for retrievalResult', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'IPNI_ERROR_404' })
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'HTTP_502' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: true,
        indexerResult: 'OK',
        hasRetrievalMajority: false,
        retrievalResult: 'MAJORITY_NOT_FOUND'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND'
      ])
    })

    it('finds majority for retrievalResult', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'CONTENT_VERIFICATION_FAILED' })
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'CONTENT_VERIFICATION_FAILED' })
      // minority result
      c.addMeasurement({ ...VALID_MEASUREMENT, retrievalResult: 'OK' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: true,
        indexerResult: 'OK',
        hasRetrievalMajority: true,
        retrievalResult: 'CONTENT_VERIFICATION_FAILED'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })

    it('rejects committees without absolute majority for indexerResult', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, indexerResult: 'OK' })
      c.addMeasurement({ ...VALID_MEASUREMENT, indexerResult: 'ERROR_404' })
      c.addMeasurement({ ...VALID_MEASUREMENT, indexerResult: 'HTTP_NOT_ADVERTISED' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: false,
        indexerResult: 'MAJORITY_NOT_FOUND',
        hasRetrievalMajority: false,
        retrievalResult: 'MAJORITY_NOT_FOUND'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND',
        'MAJORITY_NOT_FOUND'
      ])
    })

    it('finds majority for indexerResult', () => {
      const c = new Committee(VALID_TASK)
      c.addMeasurement({ ...VALID_MEASUREMENT, indexerResult: 'HTTP_NOT_ADVERTISED' })
      c.addMeasurement({ ...VALID_MEASUREMENT, indexerResult: 'HTTP_NOT_ADVERTISED' })
      // minority result
      c.addMeasurement({ ...VALID_MEASUREMENT, indexerResult: 'OK' })

      c.evaluate({ requiredCommitteeSize: 2 })

      assert.deepStrictEqual(c.evaluation, {
        hasIndexMajority: true,
        indexerResult: 'HTTP_NOT_ADVERTISED',
        hasRetrievalMajority: true,
        retrievalResult: 'OK'
      })
      assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
        'OK',
        'OK',
        'MINORITY_RESULT'
      ])
    })
  })

  it('rejects committees without absolute majority for byte_length', () => {
    const c = new Committee(VALID_TASK)
    c.addMeasurement({ ...VALID_MEASUREMENT, byte_length: 0 })
    c.addMeasurement({ ...VALID_MEASUREMENT, byte_length: 256 })
    c.addMeasurement({ ...VALID_MEASUREMENT, byte_length: 1024 })

    c.evaluate({ requiredCommitteeSize: 2 })

    assert.deepStrictEqual(c.evaluation, {
      hasIndexMajority: true,
      indexerResult: 'OK',
      hasRetrievalMajority: false,
      retrievalResult: 'MAJORITY_NOT_FOUND'
    })
    assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
      'MAJORITY_NOT_FOUND',
      'MAJORITY_NOT_FOUND',
      'MAJORITY_NOT_FOUND'
    ])
  })

  it('finds majority for byte_length', () => {
    const c = new Committee(VALID_TASK)
    c.addMeasurement({ ...VALID_MEASUREMENT, byte_length: 1024 })
    c.addMeasurement({ ...VALID_MEASUREMENT, byte_length: 1024 })
    // minority result
    c.addMeasurement({ ...VALID_MEASUREMENT, byte_length: 256 })

    c.evaluate({ requiredCommitteeSize: 2 })

    assert.deepStrictEqual(c.evaluation, {
      hasIndexMajority: true,
      indexerResult: 'OK',
      hasRetrievalMajority: true,
      retrievalResult: 'OK'
    })
    assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
      'OK',
      'OK',
      'MINORITY_RESULT'
    ])
  })

  it('rejects committees without absolute majority for carChecksum', () => {
    const c = new Committee(VALID_TASK)
    c.addMeasurement({ ...VALID_MEASUREMENT, carChecksum: 'hashone' })
    c.addMeasurement({ ...VALID_MEASUREMENT, carChecksum: 'hash2' })
    c.addMeasurement({ ...VALID_MEASUREMENT, carChecksum: 'hash3' })

    c.evaluate({ requiredCommitteeSize: 2 })

    assert.deepStrictEqual(c.evaluation, {
      hasIndexMajority: true,
      indexerResult: 'OK',
      hasRetrievalMajority: false,
      retrievalResult: 'MAJORITY_NOT_FOUND'
    })
    assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
      'MAJORITY_NOT_FOUND',
      'MAJORITY_NOT_FOUND',
      'MAJORITY_NOT_FOUND'
    ])
  })

  it('finds majority for carChecksum', () => {
    const c = new Committee(VALID_TASK)
    c.addMeasurement({ ...VALID_MEASUREMENT, carChecksum: 'hashone' })
    c.addMeasurement({ ...VALID_MEASUREMENT, carChecksum: 'hashone' })
    // minority result
    c.addMeasurement({ ...VALID_MEASUREMENT, carChecksum: 'hash2' })

    c.evaluate({ requiredCommitteeSize: 2 })

    assert.deepStrictEqual(c.evaluation, {
      hasIndexMajority: true,
      indexerResult: 'OK',
      hasRetrievalMajority: true,
      retrievalResult: 'OK'
    })
    assert.deepStrictEqual(c.measurements.map(m => m.fraudAssessment), [
      'OK',
      'OK',
      'MINORITY_RESULT'
    ])
  })
})
