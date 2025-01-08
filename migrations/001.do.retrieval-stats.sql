CREATE TABLE retrieval_stats (
  day DATE NOT NULL PRIMARY KEY,
  total INT NOT NULL,
  successful INT NOT NULL,
  successful_http INT NOT NULL
);
