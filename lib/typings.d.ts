export interface RetrievalTask {
  cid: String;
  providerAddress: String;
  protocol: String;
}

export interface RoundDetails {
  roundId: string; // BigInt
  retrievalTasks: RetrievalTask[];
}
