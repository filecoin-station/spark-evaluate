/*
Usage:

1. Setup port forwarding between your local computer and Postgres instance hosted by Fly.io
  ([docs](https://fly.io/docs/postgres/connecting/connecting-with-flyctl/)). Remember to use a
  different port if you have a local Postgres server for development!
   ```sh
   fly proxy 5454:5432 -a spark-db
   ```

2. Find spark-db entry in 1Password and get the user and password from the connection string.

3. Run the following command to fetch all measurements, remember to replace "user" and "password"
   with the real credentials:

   ```sh
   DATABASE_URL="postgres://user:password@localhost:5454/spark" node bin/fetch-measurements.js <range-start> <range-end> > measurements.ndjson
   ```

   This will fetch all measurements committed between range-start (inclusive) and range-end (exclusive)
   and write them to `measurements.ndjson` in the current directory.

The script prints prints measurement in NDJSON format to stdout and progress information to stderr.
*/

import pg from 'pg'
import { fetchMeasurements } from '../lib/preprocess.js'

const { DATABASE_URL } = process.env

const [,, startStr, endStr] = process.argv

const printUsage = () => {
  console.error(`
Usage:
  ${process.argv[0]} ${process.argv[1]} <range-start> <range-end>

Example
  ${process.argv[0]} ${process.argv[1]} 2024-01-01T00:00:00Z 2024-01-02T00:00:00Z
  `)
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

const { rows } = await client.query(`
  SELECT cid FROM commitments
  WHERE published_at >= $1 AND published_at < $2
  `, [
  start,
  end
])
const cids = rows.map(r => r.cid)

console.error('Found %s commitments', cids.length)

for await (const c of cids) {
  console.error('  fetching %s', c)
  try {
    const measurements = await fetchMeasurements(c)
    for (const m of measurements) {
      console.log(JSON.stringify(m))
    }
  } catch (err) {
    console.error('**ALERT** skipping %s: %s', c, err)
  }
}

console.error('Done')
await client.end()
