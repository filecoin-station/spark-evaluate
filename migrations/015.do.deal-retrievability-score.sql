-- For each (day, miner_id, client_id), we want to know the following numbers (counts):
--  * `tested`: (NEW) total deals tested
--  * `indexed`: deals announcing retrievals to IPNI (HTTP or Graphsync retrievals)
--  * `indexed_http`: (NEW) deals announcing HTTP retrievals to IPNI
--  * `majority_found`: (NEW) deals where we found a majority agreeing on the same result
--  * `retrievable`: deals where the majority agrees the content can be retrieved

ALTER TABLE daily_deals ADD COLUMN miner_id TEXT;
UPDATE daily_deals SET miner_id = 'all-combined';
ALTER TABLE daily_deals ALTER COLUMN miner_id SET NOT NULL;

ALTER TABLE daily_deals ADD COLUMN client_id TEXT;
UPDATE daily_deals SET client_id = 'all-combined';
ALTER TABLE daily_deals ALTER COLUMN client_id SET NOT NULL;

-- Change the primary key to a composite pair (day, miner_id, client_id)
ALTER TABLE daily_deals DROP CONSTRAINT daily_deals_pkey;
ALTER TABLE daily_deals ADD PRIMARY KEY (day, miner_id, client_id);

CREATE INDEX daily_deals_day ON daily_deals (day);

ALTER TABLE daily_deals ADD COLUMN majority_found INT;
UPDATE daily_deals SET majority_found = total;
ALTER TABLE daily_deals ALTER COLUMN majority_found SET NOT NULL;

-- Note: backfilling `tested = total` is not entirely accurate:
-- * Before we introduced committees & majorities, tested = total
-- * After that change we started to calculate total = majority_found
ALTER TABLE daily_deals RENAME COLUMN total to tested;

ALTER TABLE daily_deals ADD COLUMN indexed_http INT;
-- We don't how many of the deals tested in the past offered HTTP retrievals.
-- Historically, this value was between 1/7 to 1/3 of indexed deals.
-- I am using 1/5 as an approximation to give us more meaningful data than 0.
UPDATE daily_deals SET indexed_http = indexed/5;
ALTER TABLE daily_deals ALTER COLUMN indexed_http SET NOT NULL;

