import { getRetrievalResult, parseParticipantAddress, preprocess, Measurement } from '../lib/preprocess.js'
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import createDebug from 'debug'
import { assertPointFieldValue, assertRecordedTelemetryPoint } from './helpers/assertions.js'

const debug = createDebug('test')

const telemetry = []
const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  debug('recordTelemetry(%s): %o', measurementName, point.fields)
  telemetry.push(point)
}
beforeEach(() => telemetry.splice(0))

describe('preprocess', () => {
  it('fetches measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 'f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i',
      spark_version: '1.2.3',
      inet_group: 'ig1',
      finished_at: '2023-11-01T09:00:00.000Z',
      first_byte_at: '2023-11-01T09:00:01.000Z',
      start_at: '2023-11-01T09:00:02.000Z',
      end_at: '2023-11-01T09:00:03.000Z'
    }]
    const getCalls = []
    const fetchMeasurements = async (cid) => {
      getCalls.push(cid)
      return measurements
    }
    const logger = { log: debug, error: console.error }
    await preprocess({ rounds, cid, roundIndex, fetchMeasurements, recordTelemetry, logger })

    assert.deepStrictEqual(Object.keys(rounds), ['0'])
    assert.deepStrictEqual(rounds[0].measurements, [
      new Measurement({
        participant_address: '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E',
        spark_version: '1.2.3',
        inet_group: 'ig1',
        finished_at: '2023-11-01T09:00:00.000Z',
        first_byte_at: '2023-11-01T09:00:01.000Z',
        start_at: '2023-11-01T09:00:02.000Z',
        end_at: '2023-11-01T09:00:03.000Z',
        retrievalResult: 'UNKNOWN_ERROR'
      })
    ])
    assert.deepStrictEqual(getCalls, [cid])

    const point = assertRecordedTelemetryPoint(telemetry, 'spark_versions')
    assertPointFieldValue(point, 'round_index', '0i')
    assertPointFieldValue(point, 'v1.2.3', '1i')
    assertPointFieldValue(point, 'total', '1i')
  })
  it('validates measurements', async () => {
    const rounds = {}
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 't1foobar',
      inet_group: 'ig1',
      finished_at: '2023-11-01T09:00:00.000Z',
      first_byte_at: '2023-11-01T09:00:01.000Z',
      start_at: '2023-11-01T09:00:02.000Z',
      end_at: '2023-11-01T09:00:03.000Z'
    }]
    const fetchMeasurements = async (_cid) => measurements
    const logger = { log: debug, error: debug }
    await preprocess({ rounds, cid, roundIndex, fetchMeasurements, recordTelemetry, logger })

    assert.deepStrictEqual(Object.keys(rounds), ['0'])
    // We allow invalid participant address for now.
    // We should update this test when we remove this temporary workaround.
    assert.deepStrictEqual(rounds[0].measurements, [
      new Measurement({
        participant_address: '0x000000000000000000000000000000000000dEaD',
        inet_group: 'ig1',
        finished_at: '2023-11-01T09:00:00.000Z',
        first_byte_at: '2023-11-01T09:00:01.000Z',
        start_at: '2023-11-01T09:00:02.000Z',
        end_at: '2023-11-01T09:00:03.000Z',
        retrievalResult: 'UNKNOWN_ERROR'
      })
    ])
  })

  it('converts mainnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts testnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('t410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts mainnet f1 wallet address to hard-coded participant ETH adddress', () => {
    const converted = parseParticipantAddress('f17uoq6tp427uzv7fztkbsnn64iwotfrristwpryy')
    assert.strictEqual(converted, '0x000000000000000000000000000000000000dEaD')
  })

  it('converts testnet f1 wallet address to hard-coded participant ETH adddress', () => {
    const converted = parseParticipantAddress('t17uoq6tp427uzv7fztkbsnn64iwotfrristwpryy')
    assert.strictEqual(converted, '0x000000000000000000000000000000000000dEaD')
  })

  it('accepts ETH 0x address', () => {
    const converted = parseParticipantAddress('0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
    assert.strictEqual(converted, '0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
  })
})

describe('getRetrievalResult', () => {
  /** @type {import('../lib/typings').Measurement} */
  const SUCCESSFUL_RETRIEVAL = {
    id: 11009569,
    spark_version: '1.5.2',
    zinnia_version: '0.14.0',
    participant_address: 'f410fgkhpcrbmdvic52o3nivftrjxr7nzw47updmuzra',
    finished_at: '2023-11-01T09:42:03.246Z',
    timeout: false,
    start_at: '2023-11-01T09:40:03.393Z',
    status_code: 200,
    first_byte_at: '1970-01-01T00:00:00.000Z',
    end_at: '1970-01-01T00:00:00.000Z',
    byte_length: 1234,
    attestation: null,
    inet_group: 'ue49TX_JdYjI',
    cid: 'bafkreihstuf2qcu3hs64ersidh46cjtilxcoipmzgu3pifwzmkqdjpraqq',
    provider_address: '/ip4/108.89.91.150/tcp/46717/p2p/12D3KooWSsaFCtzDJUEhLQYDdwoFtdCMqqfk562UMvccFz12kYxU',
    protocol: 'graphsync'
  }

  it('successful retrieval', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL
    })
    assert.strictEqual(result, 'OK')
  })

  it('TIMEOUT', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      timeout: true
    })
    assert.strictEqual(result, 'TIMEOUT')
  })

  it('CAR_TOO_LARGE', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      car_too_large: true
    })
    assert.strictEqual(result, 'CAR_TOO_LARGE')
  })

  it('BAD_GATEWAY', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 502
    })
    assert.strictEqual(result, 'BAD_GATEWAY')
  })

  it('GATEWAY_TIMEOUT', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 504
    })
    assert.strictEqual(result, 'GATEWAY_TIMEOUT')
  })

  it('SERVER_ERROR - 500', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 500
    })
    assert.strictEqual(result, 'ERROR_500')
  })

  it('SERVER_ERROR - 503', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      status_code: 503
    })
    assert.strictEqual(result, 'ERROR_503')
  })

  it('UNKNOWN_ERROR - missing end_at', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      end_at: undefined
    })
    assert.strictEqual(result, 'UNKNOWN_ERROR')
  })

  it('UNKNOWN_ERROR - status_code is null', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      timeout: false,
      status_code: null
    })
    assert.strictEqual(result, 'UNKNOWN_ERROR')
  })
})
