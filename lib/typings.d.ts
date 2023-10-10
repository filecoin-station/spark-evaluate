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
