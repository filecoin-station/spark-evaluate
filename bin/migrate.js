import { DATABASE_URL } from '../lib/config.js'
import { migrateWithPgConfig } from '../lib/migrate.js'

await migrateWithPgConfig({ connectionString: DATABASE_URL })
