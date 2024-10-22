import { Point } from '@influxdata/influxdb-client'

export {
  Point
}


/**
 * Details of a retrieval task as returned by SPARK HTTP API.
 */
export interface RetrievalTask {
  cid: string;
  minerId: string;
  clients?: string[];
}

/**
 * Details of a SPARK round as returned by SPARK HTTP API.
 */
export interface RoundDetails {
  roundId: string; // BigInt serialized as String (JSON does not support BigInt)
  retrievalTasks: RetrievalTask[];
  maxTasksPerNode: number;
  startEpoch: string; // BigInt serialized as String (JSON does not support BigInt)
}

export type RecordTelemetryFn = (
  name: string,
  fn: (point: Point) => void
) => void

export type CommitteeCheckError =
| 'COMMITTEE_TOO_SMALL'
| 'MAJORITY_NOT_FOUND'
| 'MINORITY_RESULT'

// When adding a new enum value, remember to update the summary initializer inside `evaluate()`
export type FraudAssesment =
  | 'OK'
  | 'TASK_NOT_IN_ROUND'
  | 'TASK_WRONG_NODE'
  | 'DUP_INET_GROUP'
  | 'TOO_MANY_TASKS'
  | 'IPNI_NOT_QUERIED'
  | CommitteeCheckError


// When adding a new enum value, remember to update the summary initializer inside `reportRetrievalStats()`
export type RetrievalResult =
  | 'OK'
  | 'TIMEOUT'
  | 'CAR_TOO_LARGE'
  | 'UNKNOWN_FETCH_ERROR'
  | 'UNSUPPORTED_MULTIADDR_FORMAT'
  | 'HOSTNAME_DNS_ERROR'
  | 'CONNECTION_REFUSED'
  | 'UNSUPPORTED_CID_HASH_ALGO'
  | 'CONTENT_VERIFICATION_FAILED'
  | 'UNEXPECTED_CAR_BLOCK'
  | 'CANNOT_PARSE_CAR_FILE'
  | 'IPNI_NOT_QUERIED'
  | `IPNI_${string}`
  | `HTTP_${number}`
  | `LASSIE_${number}`
  | 'UNKNOWN_ERROR'
  | CommitteeCheckError


// Data coming from spark-api and spark-publish
export interface RawMeasurement {
  participant_address: string;
  spark_version: string;

  miner_id: string;
  cid: string;
  provider_id: string;
  provider_address: string;
  protocol: string;
  inet_group: string;
  station_id: string;

  start_at: string;
  first_byte_at: string;
  end_at: string;
  finished_at: string;

  status_code: number | undefined | null;
  timeout: boolean;
  byte_length: number;
  car_too_large: boolean;
  car_checksum: string;
  indexer_result: string | undefined | null;
}

export type CreatePgClient = () => Promise<import('pg').Client>;
