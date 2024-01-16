# spark-evaluate
Evaluate service

- [Meridian spec](https://www.notion.so/pl-strflt/Meridian-Design-Doc-07-Flexible-preprocessing-1b8f2f19ca7d4fd4b74a1e57e7d7ef8a?pvs=4)
- [Meridian evaluate service](https://github.com/Meridian-IE/evaluate-service)

## Development

Set up [PostgreSQL](https://www.postgresql.org/) with default settings:
 - Port: 5432
 - User: _your system user name_
 - Password: _blank_
 - Database: spark_stats

Alternatively, set the environment variable `$DATABASE_URL` with
`postgres://${USER}:${PASS}@${HOST}:${POST}/${DATABASE}`.

The Postgres user and database need to exist already, and the user
needs full management permissions for the database.

You can also run the following command to set up the PostgreSQL server via Docker:

```bash
docker run -d --name spark-db \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -e POSTGRES_USER=$USER \
  -e POSTGRES_DB=spark_stats \
  -p 5432:5432 \
  postgres
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

At the moment, the script requires CID(s) of measurements to load. (In the future, we may discover
those CIDs from on-chain events.)

Example: evaluate round `273` of Meridian version `0x3113b83ccec38a18df936f31297de490485d7b2e` with measurements from CID `bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4z`.

```shell
❯ node bin/dry-run.js 0x3113b83ccec38a18df936f31297de490485d7b2e 273 bafybeie5rekb2jox77ow64wjjd2bjdsp6d3yeivhzzd234hnbpscfjarv4
```

## Deployment

```bash
$ git push
```
