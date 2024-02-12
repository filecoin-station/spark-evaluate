CREATE TABLE indexer_query_stats (
  day DATE NOT NULL PRIMARY KEY,
  total INT NOT NULL,
  advertising_http INT NOT NULL
);
