CREATE TABLE retrieval_timings (
  day DATE NOT NULL,
  miner_id TEXT NOT NULL,
  ttfb_p50 INT[] NOT NULL,
  PRIMARY KEY (day, miner_id)
);
