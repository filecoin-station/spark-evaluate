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
    const MERIDIAN_ROUND = 1255
    const MEASUREMENTS_CID = 'bafybeicnmc4yz43q2pq7kmrwbgtqtx3kubdxymdkd57q7wrv5dcuaw7c4a'

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
      total_measurements: '15480i',
      valid_measurements: '15480i'
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
      group_winning_max: '1',
      group_winning_mean: '0.9765015570893654',
      group_winning_min: '0.3333333333333333',
      honest_measurements: '7956i',
      measurements_DUP_INET_GROUP: '351i',
      measurements_INVALID_TASK: '190i',
      measurements_IPNI_NOT_QUERIED: '6879i',
      measurements_OK: '7956i',
      measurements_TOO_MANY_TASKS: '104i',
      round_index: '1255i',
      total_measurements: '15480i',
      total_nodes: '8426i',
      total_participants: '2067i'
    })

    const { fields: retrievalStatsHonest } = assertRecordedTelemetryPoint(telemetry, 'retrieval_stats_honest')
    assert.deepStrictEqual(retrievalStatsHonest, {
      car_size_max: '1452i',
      car_size_mean: '237i',
      car_size_min: '151i',
      car_size_p1: '151i',
      car_size_p10: '151i',
      car_size_p5: '151i',
      car_size_p50: '163i',
      car_size_p90: '275i',
      car_size_p95: '389i',
      car_size_p99: '1452i',
      download_bandwidth: '62891i',
      duration_max: '30992i',
      duration_mean: '3498i',
      duration_min: '107i',
      duration_p1: '173i',
      duration_p10: '357i',
      duration_p5: '287i',
      duration_p50: '808i',
      duration_p90: '5135i',
      duration_p95: '30222i',
      duration_p99: '30424i',
      indexer_rate_ERROR_404: '0.8329562594268477',
      indexer_rate_ERROR_FETCH: '0.0007541478129713424',
      indexer_rate_HTTP_NOT_ADVERTISED: '0.1151332327802916',
      indexer_rate_OK: '0.05115635997988939',
      inet_groups: '967i',
      measurements: '7956i',
      participants: '2067i',
      rate_of_deals_advertising_http: '0.054',
      result_rate_BAD_GATEWAY: '0.017722473604826545',
      result_rate_CAR_TOO_LARGE: '0',
      result_rate_GATEWAY_TIMEOUT: '0',
      result_rate_IPNI_ERROR_404: '0.8329562594268477',
      result_rate_IPNI_ERROR_FETCH: '0.0007541478129713424',
      result_rate_OK: '0.033308195072900955',
      result_rate_TIMEOUT: '0',
      result_rate_UNKNOWN_ERROR: '0.1152589240824535',
      round_index: `${MERIDIAN_ROUND}i`,
      success_rate: '0.033308195072900955',
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
      ttfb_max: '16029i',
      ttfb_mean: '1286i',
      ttfb_min: '107i',
      ttfb_p1: '171i',
      ttfb_p10: '427i',
      ttfb_p5: '285i',
      ttfb_p50: '781i',
      ttfb_p90: '2257i',
      ttfb_p95: '2915i',
      ttfb_p99: '7537i',
      unique_tasks: '1000i'
    })

    const { fields: retrievalStatsAll } = assertRecordedTelemetryPoint(telemetry, 'retrieval_stats_all')
    assert.deepStrictEqual(retrievalStatsAll, {
      car_size_max: '1452i',
      car_size_mean: '233i',
      car_size_min: '151i',
      car_size_p1: '151i',
      car_size_p10: '151i',
      car_size_p5: '151i',
      car_size_p50: '163i',
      car_size_p90: '275i',
      car_size_p95: '389i',
      car_size_p99: '1452i',
      download_bandwidth: '64975i',
      duration_max: '32372i',
      duration_mean: '11620i',
      duration_min: '107i',
      duration_p1: '185i',
      duration_p10: '395i',
      duration_p5: '320i',
      duration_p50: '1711i',
      duration_p90: '30246i',
      duration_p95: '30370i',
      duration_p99: '30858i',
      indexer_rate_ERROR_404: '0.45251937984496127',
      indexer_rate_ERROR_FETCH: '0.00045219638242894054',
      indexer_rate_HTTP_NOT_ADVERTISED: '0.06291989664082687',
      indexer_rate_OK: '0.03972868217054264',
      indexer_rate_UNDEFINED: '0.44437984496124033',
      inet_groups: '1386i',
      measurements: '15480i',
      participants: '2076i',
      rate_of_deals_advertising_http: '0.09714285714285714',
      result_rate_BAD_GATEWAY: '0.021640826873385012',
      result_rate_CAR_TOO_LARGE: '0',
      result_rate_GATEWAY_TIMEOUT: '0',
      result_rate_IPNI_ERROR_404: '0.45251937984496127',
      result_rate_IPNI_ERROR_FETCH: '0.00045219638242894054',
      result_rate_IPNI_NOT_QUERIED: '0.44437984496124033',
      result_rate_OK: '0.017958656330749355',
      result_rate_TIMEOUT: '0.00006459948320413436',
      result_rate_UNKNOWN_ERROR: '0.06298449612403101',
      round_index: `${MERIDIAN_ROUND}i`,
      success_rate: '0.017958656330749355',
      tasks_per_node_max: '158i',
      tasks_per_node_mean: '1i',
      tasks_per_node_min: '1i',
      tasks_per_node_p1: '1i',
      tasks_per_node_p10: '1i',
      tasks_per_node_p5: '1i',
      tasks_per_node_p50: '1i',
      tasks_per_node_p90: '3i',
      tasks_per_node_p95: '4i',
      tasks_per_node_p99: '11i',
      ttfb_max: '16029i',
      ttfb_mean: '1295i',
      ttfb_min: '107i',
      ttfb_p1: '171i',
      ttfb_p10: '469i',
      ttfb_p5: '285i',
      ttfb_p50: '779i',
      ttfb_p90: '2314i',
      ttfb_p95: '3011i',
      ttfb_p99: '7487i',
      unique_tasks: '1997i'

    })

    const { fields: committeesStats } = assertRecordedTelemetryPoint(telemetry, 'committees')
    assert.deepStrictEqual(committeesStats, {
      measurements_max: '24i',
      measurements_mean: '7i',
      measurements_min: '1i',
      measurements_p1: '2i',
      measurements_p10: '4i',
      measurements_p5: '3i',
      measurements_p50: '7i',
      measurements_p90: '12i',
      measurements_p95: '14i',
      measurements_p99: '17i',
      nodes_max: '24i',
      nodes_mean: '7i',
      nodes_min: '1i',
      nodes_p1: '2i',
      nodes_p10: '4i',
      nodes_p5: '3i',
      nodes_p50: '7i',
      nodes_p90: '12i',
      nodes_p95: '14i',
      nodes_p99: '17i',
      participants_max: '19i',
      participants_mean: '5i',
      participants_min: '1i',
      participants_p1: '1i',
      participants_p10: '2i',
      participants_p5: '2i',
      participants_p50: '4i',
      participants_p90: '12i',
      participants_p95: '13i',
      participants_p99: '16i',
      round_index: `${MERIDIAN_ROUND}i`,
      subnets_max: '23i',
      subnets_mean: '7i',
      subnets_min: '1i',
      subnets_p1: '2i',
      subnets_p10: '4i',
      subnets_p5: '3i',
      subnets_p50: '7i',
      subnets_p90: '12i',
      subnets_p95: '13i',
      subnets_p99: '16i'
    })

    // TODO: query `public_stats` table

    // Asserting all 8k participants & their scores would be too much code to have here.
    // Let's check a smaller number of participants & their scores as a smoke test.

    assert.deepStrictEqual(ieContractWithSigner.participantAddresses.slice(0, 20), [
      '0xBE36aeB2d73F5f5E4eec5580E3135ce4D61d1597',
      '0xbCbcc7e404424E3E80be10BdBf80E71b3dB78F64',
      '0x4EE4671E8eDaA5aD749F7bce21dCf6E970d193FB',
      '0xA035eF87dA35ee9c0674659301C71712D645cb91',
      '0x5A45f28AEef32Da96Db5b6C435081A677BDfe938',
      '0x5Bd2a35b960B67295F12a850A9324Fea50E6B411',
      '0x809ECAAE36dd5AFEc4f3fE0370d8D834209A7BcD',
      '0xcc313F29D7B55Da624cF86728fC1558f35358250',
      '0xA860eefe8A03662C7348bB4529781dd214062441',
      '0x488BBeb02dBcBa7667Ae003Ac30795a65d015827',
      '0xb2400f2ff17d24F68Dc8Fc86138D673573c43000',
      '0x04e50401Bd39740286c8D1A92cF39B4D4D071963',
      '0x132ac8d0BE41874079463C26496C88b7E1536349',
      '0x4bF2e19Ab601144a0465bA16cBB78007e9032f7b',
      '0x6D7ACB2C38d412E4655A9cB227a72B3d16C91015',
      '0x2C288857FC07741cdC34AB36811f392c905cAA83',
      '0x07221f9fab99C7298e20f182771222dfE1181091',
      '0x146ffDa604B3a8eFd6c2F828Ade9C8a59C0BAdE9',
      '0x1dfA207dd8d6d1bA0836000b77dDdC02a3dA32C0',
      '0xc9549D267D39e494770297a589a5A0BF3171D169'
    ])

    assert.deepStrictEqual(ieContractWithSigner.scores.slice(0, 20), [
      125691302161n,
      125691302161n,
      377073906485n,
      125691302161n,
      377073906485n,
      125691302161n,
      125691302161n,
      125691302161n,
      879839115133n,
      125691302161n,
      125691302161n,
      377073906485n,
      251382604323n,
      125691302161n,
      125691302161n,
      1131221719457n,
      125691302161n,
      628456510809n,
      125691302161n,
      125691302161n
    ])
  })
})
