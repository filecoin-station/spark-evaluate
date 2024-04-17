import { getRetrievalResult, parseParticipantAddress, preprocess, Measurement, parseMeasurements } from '../lib/preprocess.js'
import { Point } from '../lib/telemetry.js'
import assert from 'node:assert'
import createDebug from 'debug'
import { assertPointFieldValue, assertRecordedTelemetryPoint } from './helpers/assertions.js'
import { VALID_STATION_ID } from './helpers/test-data.js'
import { RoundData } from '../lib/round.js'

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
    const round = new RoundData(0)
    const cid = 'bafybeif2'
    const roundIndex = 0
    const measurements = [{
      participant_address: 'f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i',
      station_id: VALID_STATION_ID,
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
    await preprocess({ round, cid, roundIndex, fetchMeasurements, recordTelemetry, logger })

    assert.deepStrictEqual(round.measurements, [
      new Measurement({
        participant_address: '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E',
        station_id: VALID_STATION_ID,
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

  it('converts mainnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('f410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('converts testnet wallet address to participant ETH address', () => {
    const converted = parseParticipantAddress('t410ftgmzttyqi3ti4nxbvixa4byql3o5d4eo3jtc43i')
    assert.strictEqual(converted, '0x999999cf1046e68e36E1aA2E0E07105eDDD1f08E')
  })

  it('accepts ETH 0x address', () => {
    const converted = parseParticipantAddress('0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
    assert.strictEqual(converted, '0x3356fd7D01F001f5FdA3dc032e8bA14E54C2a1a1')
  })
})

describe('parseMeasurements', () => {
  const measurements = [{ foo: 'bar' }, { beep: 'boop' }]
  it('parses a JSON array', () => {
    assert.deepStrictEqual(
      parseMeasurements(JSON.stringify(measurements)),
      measurements
    )
  })
  it('parses NDJSON', () => {
    assert.deepStrictEqual(
      parseMeasurements(measurements.map(m => JSON.stringify(m)).join('\n')),
      measurements
    )
  })
})

describe('getRetrievalResult', () => {
  /** @type {import('../lib/typings').Measurement} */
  const SUCCESSFUL_RETRIEVAL = {
    id: 11009569,
    spark_version: '1.5.2',
    zinnia_version: '0.14.0',
    participant_address: 'f410fgkhpcrbmdvic52o3nivftrjxr7nzw47updmuzra',
    station_id: VALID_STATION_ID,
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
    protocol: 'graphsync',
    indexer_result: 'OK'
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

  it('missing indexer result -> IPNI_NOT_QUERIED', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      indexer_result: undefined
    })
    assert.strictEqual(result, 'IPNI_NOT_QUERIED')
  })

  it('indexer result is null -> IPNI_NOT_QUERIED', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      indexer_result: null
    })
    assert.strictEqual(result, 'IPNI_NOT_QUERIED')
  })

  it('IPNI HTTP_NOT_ADVERTISED -> OK', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      indexer_result: 'HTTP_NOT_ADVERTISED'
    })
    assert.strictEqual(result, 'OK')
  })

  it('IPNI errors', () => {
    const result = getRetrievalResult({
      ...SUCCESSFUL_RETRIEVAL,
      indexer_result: 'ERROR_FETCH'
    })
    assert.strictEqual(result, 'IPNI_ERROR_FETCH')
  })
})
