export const migrate = async db => {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS retrieval_tasks (
      round_index INTEGER NOT NULL,
      cid TEXT NOT NULL,
      provider_address TEXT NOT NULL,
      protocol TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS measurements (
      round_index INTEGER NOT NULL,
      inet_group TEXT NOT NULL,
      finished_at TIMESTAMPZ NOT NULL UNIQUE,
      hash TEXT NOT NULL,
      participant_address TEXT NOT NULL,
      cid TEXT NOT NULL,
      provider_address TEXT NOT NULL,
      protocol TEXT NOT NULL,
      task_group TEXT AS (CONCAT(inet_group, cid, provider_address)) STORED
    );
  `)
}
