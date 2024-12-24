CREATE TABLE retreival_times (
  day DATE NOT NULL,
  miner_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  time_to_first_byte_p50 INT NOT NULL,
  PRIMARY KEY (day, miner_id, task_id)
);
