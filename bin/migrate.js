import { DATABASE_URL } from '../lib/config.js'
import { migrate } from '../lib/migrate.js'
import pg from 'pg'

const client = new pg.Client({ connectionString: DATABASE_URL })
await client.connect()
try {
  await migrate(client)
} finally {
  await client.end()
}
