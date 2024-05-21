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
    this.timeout(10000)

    // These three constants must correspond to a real round
    const MERIDIAN_VERSION = '0x8460766Edc62B525fc1FA4D628FC79229dC73031'
    const MERIDIAN_ROUND = 3602
    const MEASUREMENTS_CID = 'bafybeichkpwietn7w2ehwdkedc4wbpypodcz7rau5r2u772qbstjsblxtq'

    const round = new RoundData(MERIDIAN_ROUND)
    await preprocess({
      round,
      roundIndex: round.index,
      cid: MEASUREMENTS_CID,
      fetchMeasurements,
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
      total_measurements: '15889i',
      valid_measurements: '15889i'
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
    assert.deepStrictEqual(evaluateStats, {
      honest_measurements: '7706i',
      measurements_DUP_INET_GROUP: '160i',
      measurements_INVALID_TASK: '294i',
      measurements_OK: '7706i',
      measurements_TOO_MANY_TASKS: '7729i',
      round_index: `${MERIDIAN_ROUND}i`,
      total_measurements: '15889i',
      total_nodes: '11671i',
      total_participants: '2356i'
    })

    const { fields: retrievalStatsHonest } = assertRecordedTelemetryPoint(telemetry, 'retrieval_stats_honest')
    debugDumpData('retrievalStatsHonest', retrievalStatsHonest)
    assert.deepStrictEqual(retrievalStatsHonest, {
      car_size_max: '1674i',
      car_size_mean: '245i',
      car_size_min: '151i',
      car_size_p1: '151i',
      car_size_p10: '152i',
      car_size_p5: '151i',
      car_size_p50: '163i',
      car_size_p90: '164i',
      car_size_p95: '871i',
      car_size_p99: '1674i',
      download_bandwidth: '33630i',
      duration_max: '43692i',
      duration_mean: '5481i',
      duration_min: '391i',
      duration_p1: '726i',
      duration_p10: '945i',
      duration_p5: '880i',
      duration_p50: '2743i',
      duration_p90: '13950i',
      duration_p95: '23007i',
      duration_p99: '33120i',
      indexer_rate_ERROR_404: '0.8162470801972489',
      indexer_rate_ERROR_FETCH: '0.00012976901116013495',
      indexer_rate_HTTP_NOT_ADVERTISED: '0.07409810537243706',
      indexer_rate_NO_VALID_ADVERTISEMENT: '0.10005190760446406',
      indexer_rate_OK: '0.009473137814689852',
      inet_groups: '1459i',
      measurements: '7706i',
      nano_score_per_inet_group_max: '1946535i',
      nano_score_per_inet_group_mean: '685400i',
      nano_score_per_inet_group_min: '129769i',
      nano_score_per_inet_group_p1: '129769i',
      nano_score_per_inet_group_p10: '129769i',
      nano_score_per_inet_group_p5: '129769i',
      nano_score_per_inet_group_p50: '259538i',
      nano_score_per_inet_group_p90: '1946535i',
      nano_score_per_inet_group_p95: '1946535i',
      nano_score_per_inet_group_p99: '1946535i',
      participants: '2355i',
      rate_of_deals_advertising_http: '0.018',
      result_rate_BAD_GATEWAY: '0.055281598754217495',
      result_rate_CAR_TOO_LARGE: '0',
      result_rate_GATEWAY_TIMEOUT: '0',
      result_rate_IPNI_ERROR_404: '0.8162470801972489',
      result_rate_IPNI_ERROR_FETCH: '0.00012976901116013495',
      result_rate_IPNI_NO_VALID_ADVERTISEMENT: '0.10005190760446406',
      result_rate_OK: '0.01777835452893849',
      result_rate_TIMEOUT: '0.009862444848170258',
      result_rate_UNKNOWN_ERROR: '0.0006488450558006748',
      round_index: `${MERIDIAN_ROUND}i`,
      success_rate: '0.01777835452893849',
      tasks_per_node_max: '15i',
      tasks_per_node_mean: '1i',
      tasks_per_node_min: '1i',
      tasks_per_node_p1: '1i',
      tasks_per_node_p10: '1i',
      tasks_per_node_p5: '1i',
      tasks_per_node_p50: '1i',
      tasks_per_node_p90: '2i',
      tasks_per_node_p95: '3i',
      tasks_per_node_p99: '6i',
      ttfb_max: '43169i',
      ttfb_mean: '4050i',
      ttfb_min: '391i',
      ttfb_p1: '604i',
      ttfb_p10: '1131i',
      ttfb_p5: '843i',
      ttfb_p50: '2707i',
      ttfb_p90: '6763i',
      ttfb_p95: '11607i',
      ttfb_p99: '23513i',
      unique_tasks: '1000i'
    })

    const { fields: retrievalStatsAll } = assertRecordedTelemetryPoint(telemetry, 'retrieval_stats_all')
    debugDumpData('retrievalStatsAll', retrievalStatsAll)
    assert.deepStrictEqual(retrievalStatsAll, {
      car_size_max: '1674i',
      car_size_mean: '240i',
      car_size_min: '151i',
      car_size_p1: '151i',
      car_size_p10: '152i',
      car_size_p5: '151i',
      car_size_p50: '163i',
      car_size_p90: '164i',
      car_size_p95: '871i',
      car_size_p99: '1674i',
      download_bandwidth: '87487i',
      duration_max: '53508i',
      duration_mean: '6174i',
      duration_min: '391i',
      duration_p1: '676i',
      duration_p10: '946i',
      duration_p5: '854i',
      duration_p50: '2863i',
      duration_p90: '17690i',
      duration_p95: '30564i',
      duration_p99: '34097i',
      indexer_rate_ERROR_404: '0.780414122978161',
      indexer_rate_ERROR_FETCH: '0.00031468311410409717',
      indexer_rate_HTTP_NOT_ADVERTISED: '0.08515325067656869',
      indexer_rate_NO_VALID_ADVERTISEMENT: '0.12071244257033167',
      indexer_rate_OK: '0.013405500660834539',
      inet_groups: '1459i',
      measurements: '15889i',
      nano_score_per_inet_group_max: '1946535i',
      nano_score_per_inet_group_mean: '685400i',
      nano_score_per_inet_group_min: '129769i',
      nano_score_per_inet_group_p1: '129769i',
      nano_score_per_inet_group_p10: '129769i',
      nano_score_per_inet_group_p5: '129769i',
      nano_score_per_inet_group_p50: '259538i',
      nano_score_per_inet_group_p90: '1946535i',
      nano_score_per_inet_group_p95: '1946535i',
      nano_score_per_inet_group_p99: '1946535i',
      participants: '3091i',
      rate_of_deals_advertising_http: '0.015910898965791568',
      result_rate_BAD_GATEWAY: '0.0644471017685191',
      result_rate_CAR_TOO_LARGE: '0',
      result_rate_GATEWAY_TIMEOUT: '0',
      result_rate_IPNI_ERROR_404: '0.780414122978161',
      result_rate_IPNI_ERROR_FETCH: '0.00031468311410409717',
      result_rate_IPNI_NO_VALID_ADVERTISEMENT: '0.12071244257033167',
      result_rate_OK: '0.022908930706778276',
      result_rate_TIMEOUT: '0.010510416011076846',
      result_rate_UNKNOWN_ERROR: '0.0006923028510290138',
      round_index: `${MERIDIAN_ROUND}i`,
      success_rate: '0.022908930706778276',
      tasks_per_node_max: '81i',
      tasks_per_node_mean: '1i',
      tasks_per_node_min: '1i',
      tasks_per_node_p1: '1i',
      tasks_per_node_p10: '1i',
      tasks_per_node_p5: '1i',
      tasks_per_node_p50: '1i',
      tasks_per_node_p90: '2i',
      tasks_per_node_p95: '3i',
      tasks_per_node_p99: '5i',
      ttfb_max: '43169i',
      ttfb_mean: '4747i',
      ttfb_min: '391i',
      ttfb_p1: '672i',
      ttfb_p10: '1154i',
      ttfb_p5: '850i',
      ttfb_p50: '2876i',
      ttfb_p90: '10384i',
      ttfb_p95: '15839i',
      ttfb_p99: '25973i',
      unique_tasks: '1257i'
    })

    const { fields: committeesStats } = assertRecordedTelemetryPoint(telemetry, 'committees')
    debugDumpData('committeesStats', committeesStats)
    assert.deepStrictEqual(committeesStats, {
      measurements_max: '31i',
      measurements_mean: '12i',
      measurements_min: '1i',
      measurements_p1: '1i',
      measurements_p10: '1i',
      measurements_p5: '1i',
      measurements_p50: '14i',
      measurements_p90: '20i',
      measurements_p95: '22i',
      measurements_p99: '25i',
      nodes_max: '31i',
      nodes_mean: '12i',
      nodes_min: '1i',
      nodes_p1: '1i',
      nodes_p10: '1i',
      nodes_p5: '1i',
      nodes_p50: '14i',
      nodes_p90: '20i',
      nodes_p95: '22i',
      nodes_p99: '25i',
      participants_max: '28i',
      participants_mean: '11i',
      participants_min: '1i',
      participants_p1: '1i',
      participants_p10: '1i',
      participants_p5: '1i',
      participants_p50: '13i',
      participants_p90: '18i',
      participants_p95: '20i',
      participants_p99: '23i',
      round_index: `${MERIDIAN_ROUND}i`,
      subnets_max: '29i',
      subnets_mean: '12i',
      subnets_min: '1i',
      subnets_p1: '1i',
      subnets_p10: '1i',
      subnets_p5: '1i',
      subnets_p50: '14i',
      subnets_p90: '19i',
      subnets_p95: '21i',
      subnets_p99: '25i'
    })

    // TODO: query `public_stats` table

    // Asserting all 8k participants & their scores would be too much code to have here.
    // Let's check a smaller number of participants & their scores as a smoke test.

    debugDumpData('participantAddresses.slice(0, 20)', Array.from(ieContractWithSigner.participantAddresses.slice(0, 20)))
    assert.deepStrictEqual(ieContractWithSigner.participantAddresses.slice(0, 20), [
      '0xc79e3159817B9A3D875C1d27561500337c6888D3',
      '0x83A2140703df40EAafb89F5c7751BF28e36b6320',
      '0x5AbB29D74fBA3770De4f94EE916B705C09A61d1E',
      '0xF1332F0C302Cb1BB8Eeac99b6f6dcbDe3AcD8F2f',
      '0xAAE01930Ebb5691fcbCb2ff55d757A392a732822',
      '0x1231CC8E61dEfD317fB522D111cD1b32a559ec13',
      '0x1305cd955586Cc40ae67Cd3eF1da6a97788bFfEa',
      '0x5414435240788bEa919aB210AF07F563eb40B2dd',
      '0x2c7AC351c658bAdCE61359392e9566A7569fd23D',
      '0x5d317728639fdD3083b962Cc579dC4D2E3230Bf1',
      '0x4CCEA97c1AE6eDd77d954003db07f1c6DfF67902',
      '0xC4C961D3Ffd9192780907aEE33603dB9E2F975B3',
      '0x8Fe8FAe6FfBB6870eB8E208b5bd2c0B0d09e17F3',
      '0x5827c0e02E0FD1191FABb5b1A8Aa5FFB747c6eF9',
      '0x19ea9e9f4C2AE3B90501D6ED6B90Cd4f1a203b1C',
      '0x488BBeb02dBcBa7667Ae003Ac30795a65d015827',
      '0xd88691B0d63c3d4Ce11098BD38d59BD3B27567ec',
      '0x2cE5644f2E470b664e0B87dEa744e9EC5e448180',
      '0x0f553C9DDe2ba1d1d18a0669653EAB980A3928aA',
      '0x0CDB197fb9eA0b7927c6f43Aa0bCE60F60814Eb4'
    ])

    debug('scores.slice(0, 20) [%s]', ieContractWithSigner.scores.slice(0, 20).map(it => `\n${it}n,`).join('') + '\n')
    assert.deepStrictEqual(ieContractWithSigner.scores.slice(0, 20), [
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      259538022320n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n,
      129769011160n
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
