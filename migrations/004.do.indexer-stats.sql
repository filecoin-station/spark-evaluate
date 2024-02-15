CREATE TABLE indexer_query_stats (
  day DATE NOT NULL PRIMARY KEY,
  deals_tested INT NOT NULL,
  deals_advertising_http INT NOT NULL
);
