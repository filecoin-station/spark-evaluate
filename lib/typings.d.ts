import { Point } from '@influxdata/influxdb-client'

export {
  Point
}


/**
 * Details of a retrieval task as returned by SPARK HTTP API.
 */
export interface RetrievalTask {
  cid: String;
  minerId: String;
}

/**
 * Details of a SPARK round as returned by SPARK HTTP API.
 */
export interface RoundDetails {
  roundId: string; // BigInt serialized as String (JSON does not support BigInt)
  retrievalTasks: RetrievalTask[];
  maxTasksPerNode: number;
}

export type RecordTelemetryFn = (
  name: string,
  fn: (point: Point) => void
) => void

// When adding a new enum value, remember to update the summary initializer inside `evaluate()`
export type FraudAssesment =
  | 'OK'
  | 'INVALID_TASK'
  | 'DUP_INET_GROUP'
  | 'TOO_MANY_TASKS'
  | 'IPNI_NOT_QUERIED'


// When adding a new enum value, remember to update the summary initializer inside `reportRetrievalStats()`
export type RetrievalResult =
  | 'OK'
  | 'TIMEOUT'
  | 'CAR_TOO_LARGE'
  | 'BAD_GATEWAY'
  | 'GATEWAY_TIMEOUT'
  | 'ERROR_500'
  | 'UNKNOWN_ERROR'

// Data coming from spark-api and spark-publish
export interface RawMeasurement {
  participant_address: string;

  cid: string;
  provider_address: string;
  protocol: string;
  inet_group: string;

  start_at: string;
  first_byte_at: string;
  end_at: string;
  finished_at: string;

  status_code: number | undefined | null;
  timeout: boolean;
  byte_length: number;
  car_too_large: boolean;
  indexer_result: string | undefined | null;
}

export interface GroupWinningStats {
  min: number;
  max: number;
  mean: number;
}

export interface FraudDetectionStats {
  groupWinning: GroupWinningStats
}

export type CreatePgClient = () => Promise<import('pg').Client>;
