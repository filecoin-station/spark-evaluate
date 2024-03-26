ALTER TABLE retrieval_stats
ADD COLUMN miner_id TEXT;

-- We don't have miner_id for historical measurements.
-- Let's set the id to a special constant that's not a valid actor address.
UPDATE retrieval_stats SET miner_id = 'LEGACY';

-- Going forwards, all new stats must be linked to a particular miner
ALTER TABLE retrieval_stats
ALTER COLUMN miner_id SET NOT NULL;

-- Change the primary key to a composite pair (day, miner_id)
ALTER TABLE retrieval_stats
DROP CONSTRAINT retrieval_stats_pkey;

ALTER TABLE retrieval_stats
ADD PRIMARY KEY (day, miner_id);

-- Add back index on retrieval_stats
CREATE INDEX retrieval_stats_day ON retrieval_stats (day);
