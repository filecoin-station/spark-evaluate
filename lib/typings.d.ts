import { Point } from '@influxdata/influxdb-client'

/**
 * Details of a retrieval task as returned by SPARK HTTP API.
 */
export interface RetrievalTask {
  cid: String;
  providerAddress: String;
  protocol: String;
}

/**
 * Details of a SPARK round as returned by SPARK HTTP API.
 */
export interface RoundDetails {
  roundId: string; // BigInt serialized as String (JSON does not support BigInt)
  retrievalTasks: RetrievalTask[];
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

export interface Measurement {
  participantAddress: string;
  fraudAssessment?: FraudAssesment;

  cid: string;
  provider_address: string;
  protocol: string;
  inet_group: string;

  start_at: string;
  first_byte_at: string;
  end_at: string;
  finished_at: string;
}

export interface GroupWinningStats {
  min: number;
  max: number;
  mean: number;
}

export interface FraudDetectionStats {
  groupWinning: GroupWinningStats
}
