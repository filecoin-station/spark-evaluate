-- We had a bug in the code updating retrieval stats.
-- As a result, existing data are meaningless and misleading.
-- Let's delete it.
DELETE FROM retrieval_stats;
