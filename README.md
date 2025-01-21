# spark-evaluate
Evaluate service

- [Meridian spec](https://www.notion.so/pl-strflt/Meridian-Design-Doc-07-Flexible-preprocessing-1b8f2f19ca7d4fd4b74a1e57e7d7ef8a?pvs=4)
- [Meridian evaluate service](https://github.com/Meridian-IE/evaluate-service)

## Dry-run evaluation

You can evaluate a round locally by running the script `bin/dry-run.js`.

Remember to obtain a Glif API token first. You can store the token in the `GLIF_TOKEN` environment
variable or in the `.env` file in the root directory of your local clone of this repository.

```ini
GLIF_TOKEN="<value>"
```

**IMPORTANT**

The script needs to query the chain to list all historic `MeasurementsAdded` events. Glif, the RPC API provider we use, keeps only ~16 hours of event history. As a result, if you want to evaluate an older round, you must provide the list of all CIDs containing measurements submitted for that round.

**CACHING**

The dry-run script caches the list of MeasurementsAdded and the content of CIDs in the `.cache`
directory. This speeds up subsequent invocations of the script at the expense of increased disk
usage. Feel free to delete any files in the cache directory to reclaim disk space.

### Evaluate the round before the last one

```bash
$ node bin/dry-run.js
```

### Evaluate a round of the current smart contract version

To evaluate round index 123:

```bash
$ node bin/dry-run.js 123
```

### Evaluate a round of a given smart contract version

To evaluate round index 123 of the smart contract with address 0xabc:

```bash
$ node bin/dry-run.js 0xabc 123
```

### Specify CIDs of measurements

```bash
$ node bin/dry-run.js [contract] round [list of CIDs]
```

### Save evaluated measurements

You can also save the evaluated measurements for further processing by running the script with the
environment variable DUMP set to a non-empty value. The script will write the evaluated measurements
to a CSV file. This CSV file can be easily converted to a spreadsheet, which makes it easy to
perform further data analysis.

#### Save all measurements

```
$ DUMP=1 node bin/dry-run.js 7970
(...lots of logs...)
Evaluated measurements saved to measurements-7970-all.csv
```

#### Save measurements of one miner

Set `DUMP` to the miner ID you are interested in (`f0123` in the example below):

```
$ DUMP=f0123 node bin/dry-run.js 7970
(...lots of logs...)
Storing measurements for miner id f0123
Evaluated measurements saved to measurements-7970-f0123.csv
```

#### Save measurements from one participant

Set `DUMP` to the participant address you are interested in (`0xdead` in the example below):

```
$ DUMP=0xdead node bin/dry-run.js 7970
(...lots of logs...)
Storing measurements from participant address 0xdead
Evaluated measurements saved to measurements-7970-0xdead.csv
```

## Development

Set up [PostgreSQL](https://www.postgresql.org/) with default settings:
 - Port: 5432
 - User: _your system user name_
 - Password: _blank_
 - Database: spark_evaluate

Alternatively, set the environment variable `$DATABASE_URL` with
`postgres://${USER}:${PASS}@${HOST}:${PORT}/${DATABASE}`.

The Postgres user and database need to exist already, and the user
needs full management permissions for the database.

You can also run the following command to set up the PostgreSQL server via Docker:

```bash
docker run -d --name spark-db \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_USER=$USER \
  -e POSTGRES_DB=spark_evaluate \
  -p 5432:5432 \
  postgres
```

If you are sharing the same Postgres instance for multiple projects, run the following
command to create a new `spark_evaluate` database for this project:

```bash
psql postgres://localhost:5432/ -c "CREATE DATABASE spark_evaluate;"
```

## Run the tests

```bash
$ npm test
```

## Run the service

```bash
$ WALLET_SEED=$(cat secrets/mnemonic) npm start
```

## Troubleshooting

You can perform a dry-run evaluation of a given Meridan round using the script `bin/dry-run.js`.

1. Get your GLIF API access token at https://api.node.glif.io/

2. Save the token to the `.env` file in project's root directory:

   ```ini
   GLIF_TOKEN="...your-token..."
   ```

3. Run the dry-run script. By default, the script evaluates the last round of the current smart contract version.

   ```shell
   node bin/dry-run.js
   ```

You can optionally specify the smart contract address, round index and list of CIDs of measurements
to load.  For example, run the following command to evaluate round `273` of the Meridian version
`0x3113b83ccec38a18df936f31297de490485d7b2e` with measurements from CID
`bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4`:

```shell
node bin/dry-run.js \
  0x3113b83ccec38a18df936f31297de490485d7b2e \
  273 \
  bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4
```

## Deployment

```bash
$ git push
```

## Publish

Publish a new version of `@filecoin-station/spark-evaluate`:

```bash
$ npm run release
```
