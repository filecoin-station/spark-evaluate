CREATE TABLE unpublished_rsr_rounds (
  round_index NUMERIC PRIMARY KEY,
  evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  spark_evaluate_version TEXT NOT NULL,
  measurement_commitments TEXT[] NOT NULL,
  round_details JSONB NOT NULL,
  provider_retrieval_result_stats JSONB NOT NULL
);
