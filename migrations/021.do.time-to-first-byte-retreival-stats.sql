CREATE TABLE ttfb_retreival_stats (
  day DATE NOT NULL,
  miner_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  ttfb_median INT NOT NULL,
  PRIMARY KEY (day, miner_id, task_id)
);
