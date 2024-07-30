import createDebug from 'debug'
import { beforeEach } from 'mocha'
import assert from 'node:assert'
import pg from 'pg'
import { DATABASE_URL } from '../lib/config.js'
import { evaluate } from '../lib/evaluate.js'
import { migrateWithPgClient } from '../lib/migrate.js'
import { fetchMeasurements, preprocess } from '../lib/preprocess.js'
import { RoundData } from '../lib/round.js'
import { fetchRoundDetails } from '../lib/spark-api.js'
import { Point } from '../lib/telemetry.js'
import { assertRecordedTelemetryPoint } from './helpers/assertions.js'
import { fileURLToPath } from 'node:url'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'

const debug = createDebug('test')

const logger = { log: debug, error: console.error }

const telemetry = []
const recordTelemetry = (measurementName, fn) => {
  const point = new Point(measurementName)
  fn(point)
  debug('recordTelemetry(%s): %o', measurementName, point.fields)
  telemetry.push(point)
}
beforeEach(() => telemetry.splice(0))

const createIeContractWithSigner = (contractAddress) => ({
  participantAddresses: null,
  scores: null,

  async getAddress () {
    return contractAddress
  },
  async setScores (_roundIndex, participantAddresses, scores) {
    this.participantAddresses = participantAddresses
    this.scores = scores
    return { hash: '0x234' }
  }
})

const createPgClient = async () => {
  const pgClient = new pg.Client({ connectionString: DATABASE_URL })
  await pgClient.connect()
  return pgClient
}

describe('preprocess-evaluate integration', () => {
  let pgClient
  before(async () => {
    pgClient = await createPgClient()
    await migrateWithPgClient(pgClient)
  })

  beforeEach(async () => {
    await pgClient.query('DELETE FROM retrieval_stats')
  })

  after(async () => {
    await pgClient.end()
  })

  it('produces expected results', async function () {
    this.timeout(60_000)

    // These three constants must correspond to a real round
    const MERIDIAN_VERSION = '0x8460766edc62b525fc1fa4d628fc79229dc73031'
    const MERIDIAN_ROUND = 12012n
    // You can find measurement CIDs committed for the given round by inspecting the logs printed
    // by the following command: node bin/dry-run.js ${MERIDIAN_ROUND}
    const MEASUREMENTS_CID = 'bafybeifjhg4z34ytwl4kkotxzoflbzk3ggz6k7laefl2rjur3vymlcumzm'

    const round = new RoundData(MERIDIAN_ROUND)
    await preprocess({
      round,
      roundIndex: round.index,
      cid: MEASUREMENTS_CID,
      fetchMeasurements: fetchMeasurementsWithCache,
      logger,
      recordTelemetry
    })

    const ieContractWithSigner = createIeContractWithSigner(MERIDIAN_VERSION)
    await evaluate({
      createPgClient,
      fetchRoundDetails,
      ieContractWithSigner,
      logger,
      recordTelemetry,
      round,
      roundIndex: round.index
    })

    // We are asserting on all outputs. This is verbose, but it should capture most regressions.
    // In the future, it would be great to use snapshot-based testing instead.

    assert.deepStrictEqual(telemetry.map(p => p.name), [
      'preprocess',
      'spark_versions',
      'fetch_tasks_for_round',
      'evaluate',
      'retrieval_stats_honest',
      'retrieval_stats_all',
      'committees'
    ])

    const {
      fields: {
      // eslint-disable-next-line camelcase
        fetch_duration_ms: fetchMeasurementsDuration,
        ...preprocessingStats
      }
    } = assertRecordedTelemetryPoint(telemetry, 'preprocess')
    assert.match(fetchMeasurementsDuration, /^\d+i/)
    assert.deepStrictEqual(preprocessingStats, {
      round_index: `${MERIDIAN_ROUND}i`,
      total_measurements: '44677i',
      valid_measurements: '44677i'
    })

    const {
      fields: {
        fetch_duration_ms: fetchTasksDuration,
        ...fetchTasksStats
      }
    } = assertRecordedTelemetryPoint(telemetry, 'fetch_tasks_for_round')
    assert.match(fetchTasksDuration, /^\d+i/)
    assert.deepStrictEqual(fetchTasksStats, {
      contract_address: `"${MERIDIAN_VERSION}"`,
      round_index: `${MERIDIAN_ROUND}i`,
      status: '200i',
      task_count: '1000i'
    })

    const {
      fields: {
        fraud_detection_duration_ms: fraudDetectionDuration,
        set_scores_duration_ms: setScoresDuration,
        ...evaluateStats
      }
    } = assertRecordedTelemetryPoint(telemetry, 'evaluate')
    assert.match(fraudDetectionDuration, /^\d+i/)
    assert.match(setScoresDuration, /^\d+i/)
    debugDumpData('evaluateStats', evaluateStats)
    assert.deepStrictEqual(evaluateStats, {
      honest_measurements: '14512i',
      measurements_DUP_INET_GROUP: '21380i',
      measurements_OK: '14512i',
      measurements_TASK_NOT_IN_ROUND: '0i',
      measurements_TASK_WRONG_NODE: '7185i',
      measurements_TOO_MANY_TASKS: '1600i',
      round_index: '12012i',
      total_measurements: '44677i',
      total_nodes: '14074i',
      total_participants: '7618i'
    })

    const { fields: retrievalStatsHonest } = assertRecordedTelemetryPoint(telemetry, 'retrieval_stats_honest')
    debugDumpData('retrievalStatsHonest', retrievalStatsHonest)
    assert.deepStrictEqual(retrievalStatsHonest, {
      car_size_max: '1048674i',
      car_size_mean: '111432i',
      car_size_min: '151i',
      car_size_p1: '151i',
      car_size_p10: '163i',
      car_size_p5: '151i',
      car_size_p50: '1063i',
      car_size_p90: '429014i',
      car_size_p95: '1048674i',
      car_size_p99: '1048674i',
      download_bandwidth: '79897050i',
      duration_max: '65926i',
      duration_mean: '8009i',
      duration_min: '85i',
      duration_p1: '246i',
      duration_p10: '749i',
      duration_p5: '540i',
      duration_p50: '1945i',
      duration_p90: '19379i',
      duration_p95: '60638i',
      duration_p99: '61350i',
      indexer_rate_ERROR_404: '0.6473952590959207',
      indexer_rate_ERROR_FETCH: '0.00020672546857772878',
      indexer_rate_HTTP_NOT_ADVERTISED: '0.1272050716648291',
      indexer_rate_NO_VALID_ADVERTISEMENT: '0.1983186328555678',
      indexer_rate_OK: '0.02687431091510474',
      inet_groups: '7573i',
      measurements: '14512i',
      nano_score_per_inet_group_max: '1033627i',
      nano_score_per_inet_group_mean: '132047i',
      nano_score_per_inet_group_min: '68908i',
      nano_score_per_inet_group_p1: '68908i',
      nano_score_per_inet_group_p10: '68908i',
      nano_score_per_inet_group_p5: '68908i',
      nano_score_per_inet_group_p50: '68908i',
      nano_score_per_inet_group_p90: '275634i',
      nano_score_per_inet_group_p95: '344542i',
      nano_score_per_inet_group_p99: '689085i',
      participants: '7618i',
      rate_of_deals_advertising_http: '0.02465166130760986',
      result_rate_BAD_GATEWAY: '0.07786659316427784',
      result_rate_CAR_TOO_LARGE: '0',
      result_rate_CONNECTION_REFUSED: '0.0024117971334068357',
      result_rate_ERROR_500: '0.009578280044101434',
      result_rate_ERROR_503: '0.0000689084895259096',
      result_rate_GATEWAY_TIMEOUT: '0',
      result_rate_IPNI_ERROR_404: '0.6473952590959207',
      result_rate_IPNI_ERROR_FETCH: '0.00020672546857772878',
      result_rate_IPNI_NO_VALID_ADVERTISEMENT: '0.1983186328555678',
      result_rate_OK: '0.04940738699007718',
      result_rate_TIMEOUT: '0.014539691289966924',
      result_rate_UNKNOWN_FETCH_ERROR: '0.00020672546857772878',
      round_index: '12012i',
      success_rate: '0.04940738699007718',
      tasks_per_node_max: '15i',
      tasks_per_node_mean: '1i',
      tasks_per_node_min: '1i',
      tasks_per_node_p1: '1i',
      tasks_per_node_p10: '1i',
      tasks_per_node_p5: '1i',
      tasks_per_node_p50: '1i',
      tasks_per_node_p90: '2i',
      tasks_per_node_p95: '2i',
      tasks_per_node_p99: '3i',
      ttfb_max: '28995i',
      ttfb_mean: '2391i',
      ttfb_min: '92i',
      ttfb_p1: '300i',
      ttfb_p10: '710i',
      ttfb_p5: '515i',
      ttfb_p50: '1776i',
      ttfb_p90: '4228i',
      ttfb_p95: '5831i',
      ttfb_p99: '12460i',
      unique_tasks: '933i'
    })

    const { fields: retrievalStatsAll } = assertRecordedTelemetryPoint(telemetry, 'retrieval_stats_all')
    debugDumpData('retrievalStatsAll', retrievalStatsAll)
    assert.deepStrictEqual(retrievalStatsAll, {
      car_size_max: '1048674i',
      car_size_mean: '153342i',
      car_size_min: '151i',
      car_size_p1: '151i',
      car_size_p10: '163i',
      car_size_p5: '155i',
      car_size_p50: '1063i',
      car_size_p90: '1048674i',
      car_size_p95: '1048674i',
      car_size_p99: '1048674i',
      download_bandwidth: '143068345i',
      duration_max: '65926i',
      duration_mean: '4654i',
      duration_min: '85i',
      duration_p1: '104i',
      duration_p10: '468i',
      duration_p5: '221i',
      duration_p50: '1495i',
      duration_p90: '4771i',
      duration_p95: '23194i',
      duration_p99: '61158i',
      indexer_rate_ERROR_404: '0.7199230028873917',
      indexer_rate_ERROR_FETCH: '0.0000671486447165208',
      indexer_rate_HTTP_NOT_ADVERTISED: '0.08841238221008572',
      indexer_rate_NO_VALID_ADVERTISEMENT: '0.16988607113279763',
      indexer_rate_OK: '0.021711395125008394',
      inet_groups: '7872i',
      measurements: '44677i',
      nano_score_per_inet_group_max: '1033627i',
      nano_score_per_inet_group_mean: '132047i',
      nano_score_per_inet_group_min: '68908i',
      nano_score_per_inet_group_p1: '68908i',
      nano_score_per_inet_group_p10: '68908i',
      nano_score_per_inet_group_p5: '68908i',
      nano_score_per_inet_group_p50: '68908i',
      nano_score_per_inet_group_p90: '275634i',
      nano_score_per_inet_group_p95: '344542i',
      nano_score_per_inet_group_p99: '689085i',
      participants: '8163i',
      rate_of_deals_advertising_http: '0.025',
      result_rate_BAD_GATEWAY: '0.07068514000492424',
      result_rate_CAR_TOO_LARGE: '0',
      result_rate_CONNECTION_REFUSED: '0.0025068827360834435',
      result_rate_ERROR_500: '0.010766166036215502',
      result_rate_ERROR_503: '0.000022382881572173602',
      result_rate_GATEWAY_TIMEOUT: '0',
      result_rate_IPNI_ERROR_404: '0.7199230028873917',
      result_rate_IPNI_ERROR_FETCH: '0.0000671486447165208',
      result_rate_IPNI_NO_VALID_ADVERTISEMENT: '0.16988607113279763',
      result_rate_OK: '0.02088322850683797',
      result_rate_TIMEOUT: '0.005192828524744276',
      result_rate_UNKNOWN_FETCH_ERROR: '0.0000671486447165208',
      round_index: '12012i',
      success_rate: '0.02088322850683797',
      tasks_per_node_max: '452i',
      tasks_per_node_mean: '3i',
      tasks_per_node_min: '1i',
      tasks_per_node_p1: '1i',
      tasks_per_node_p10: '1i',
      tasks_per_node_p5: '1i',
      tasks_per_node_p50: '1i',
      tasks_per_node_p90: '3i',
      tasks_per_node_p95: '8i',
      tasks_per_node_p99: '58i',
      ttfb_max: '28995i',
      ttfb_mean: '2132i',
      ttfb_min: '92i',
      ttfb_p1: '101i',
      ttfb_p10: '498i',
      ttfb_p5: '112i',
      ttfb_p50: '1613i',
      ttfb_p90: '3835i',
      ttfb_p95: '5412i',
      ttfb_p99: '12690i',
      unique_tasks: '1000i'
    })

    const { fields: committeesStats } = assertRecordedTelemetryPoint(telemetry, 'committees')
    debugDumpData('committeesStats', committeesStats)
    assert.deepStrictEqual(committeesStats, {
      measurements_max: '263i',
      measurements_mean: '44i',
      measurements_min: '2i',
      measurements_p1: '4i',
      measurements_p10: '10i',
      measurements_p5: '7i',
      measurements_p50: '31i',
      measurements_p90: '94i',
      measurements_p95: '130i',
      measurements_p99: '185i',
      nodes_max: '65i',
      nodes_mean: '22i',
      nodes_min: '2i',
      nodes_p1: '4i',
      nodes_p10: '9i',
      nodes_p5: '7i',
      nodes_p50: '22i',
      nodes_p90: '37i',
      nodes_p95: '43i',
      nodes_p99: '52i',
      participants_max: '56i',
      participants_mean: '18i',
      participants_min: '1i',
      participants_p1: '2i',
      participants_p10: '4i',
      participants_p5: '3i',
      participants_p50: '18i',
      participants_p90: '31i',
      participants_p95: '35i',
      participants_p99: '45i',
      round_index: '12012i',
      subnets_max: '65i',
      subnets_mean: '22i',
      subnets_min: '2i',
      subnets_p1: '4i',
      subnets_p10: '9i',
      subnets_p5: '7i',
      subnets_p50: '22i',
      subnets_p90: '36i',
      subnets_p95: '42i',
      subnets_p99: '51i'
    })

    // TODO: query `public_stats` table

    // Asserting all 8k participants & their scores would be too much code to have here.
    // Let's check a smaller number of participants & their scores as a smoke test.

    debugDumpData('participantAddresses.slice(0, 20)', Array.from(ieContractWithSigner.participantAddresses.slice(0, 20)))
    assert.deepStrictEqual(ieContractWithSigner.participantAddresses.slice(0, 20), [
      '0x15C95b5866bC31DCde07EF66d3ebd669f9052816',
      '0xec8DF4D0706BA44513674d40a2dffE75b12E5B9B',
      '0x05bfd3BcF79cdaF52628A4414e25538BEf2e4974',
      '0x4AAA137490aAF87343202DC2369227D8055cC334',
      '0xb7b0f04bbc010e752bdceeebeda123836130ff22',
      '0x0Fb3E8a725B370f04D4079A2Aa59C554740AD0E7',
      '0x6c7B358EB98025bA0f52bE60087F36a875b6D493',
      '0xd9D5A26Ddf984a23466af89d99296a5d1D820187',
      '0x5Be8B5Bf572A2d5cE0B8B11c8937C1681C5c319A',
      '0xdc46d07Ad25c6AcA735eE878b9edd9203Bf266cF',
      '0x2bdEA379A6C05d1cdbb5ce09504C225CEA207B5A',
      '0x274519DEDe793277eb7653e13117F26EF06F4B1A',
      '0x627456505e58f31AbCC3ce409199F43f162C45E0',
      '0xb1a685996715B0e5435f025f868d1674137e45B4',
      '0xf6DA175619B1Bc4a689F316082C6c696f6dFA1CE',
      '0x9A248d5886b152c13C77eAC8A86be55a86983cA4',
      '0x5fb3700e5933417c70Dd90252820fA00Ca986aA6',
      '0x04D2F181fbD6Ce2f1E4D4f0856d687478b2fAc71',
      '0x70d61045aB5D0402ab7742773419E391d0a2ed1E',
      '0x524eaCdB132d6d8777Dac3f04B9aff5D8098Dc66'
    ])

    debug('scores.slice(0, 20) [%s]', ieContractWithSigner.scores.slice(0, 20).map(it => `\n${it}n,`).join('') + '\n')
    assert.deepStrictEqual(ieContractWithSigner.scores.slice(0, 20), [
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n,
      68908489525n
    ])
  })
})

const debugDumpData = (name, data) => {
  if (Array.isArray(data)) {
    debug(name, JSON.stringify(data, null, 2))
    return
  }

  const keys = Object.keys(data)
  keys.sort()
  const sorted = {}
  for (const k of keys) sorted[k] = data[k]
  debug(name, JSON.stringify(sorted, null, 2))
}

const fetchMeasurementsWithCache = async (cid) => {
  const cacheDir = fileURLToPath(new URL('../.cache', import.meta.url))
  await mkdir(cacheDir, { recursive: true })

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
