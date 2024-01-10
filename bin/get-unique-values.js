/*
Usage:

1. Setup port forwarding between your local computer and Postgres instance hosted by Fly.io
  ([docs](https://fly.io/docs/postgres/connecting/connecting-with-flyctl/)). Remember to use a
  different port if you have a local Postgres server for development!
   ```sh
   fly proxy 5454:5432 -a spark-db
   ```

2. Run the following command to fetch all unique participant addresses.

   Replace "auth" with the auth from the `spark-db` connection string stored in 1Password

   ```sh
   DATABASE_URL="postgres://auth@localhost:5454/spark" node bin/get-unique-values.js participant_address <range-start> <range-end>
   ```

   This will get all unique participant addresses committed between range-start (inclusive) and range-end (exclusive).

The script prints prints raw values to stdout and progress information to stderr.
*/

import pg from 'pg'
import { fetchMeasurements } from '../lib/preprocess.js'
import assert from 'node:assert'

const { DATABASE_URL, CONCURRENCY = 4 } = process.env

const [,, key, startStr, endStr] = process.argv

const printUsage = () => {
  console.error(`
Usage:
  ${process.argv[0]} ${process.argv[1]} <key> <range-start> <range-end>

Example
  ${process.argv[0]} ${process.argv[1]} participant_address 2024-01-01T00:00:00Z 2024-01-02T00:00:00Z
  `)
}

if (!key) {
  console.error('Missing argument: key')
  printUsage()
  process.exit(1)
}

if (!startStr) {
  console.error('Missing argument: range-start')
  printUsage()
  process.exit(1)
}

const start = new Date(startStr)
if (Number.isNaN(start.getTime())) {
  console.error('Invalid range-start: not a valid date-time string')
  printUsage()
  process.exit(1)
}

if (!endStr) {
  console.error('Missing argument: range-end')
  printUsage()
  process.exit(1)
}

const end = new Date(endStr)
if (Number.isNaN(end.getTime())) {
  console.error('Invalid range-end: not a valid date-time string')
  printUsage()
  process.exit(1)
}

console.error('Fetching measurements committed between %j and %j', start, end)
const client = new pg.Client({ connectionString: DATABASE_URL })
await client.connect()

const seen = new Set()
let i = 0

const { rows } = await client.query(`
  SELECT cid FROM commitments
  WHERE published_at >= $1 AND published_at < $2
  ORDER BY published_at DESC
  `, [
  start,
  end
])
await client.end()

const cids = rows.map(r => r.cid)
const totalCids = cids.length
console.error('Found %s commitments', totalCids)

await Promise.all(new Array(CONCURRENCY).fill().map(async () => {
  while (true) {
    const cid = cids.shift()
    if (!cid) return
    try {
      const measurements = await fetchMeasurements(cid)
      for (const m of measurements) {
        const value = m[key]
        assert(value)
        if (seen.has(value)) continue
        seen.add(value)
        console.log(value)
      }
    } catch (err) {
      console.error('Skipping %s', cid)
    }
    console.error(
      `${String(++i).padStart(String(totalCids).length)}/${totalCids} ${cid}`
    )
  }
}))

console.error('Done')
