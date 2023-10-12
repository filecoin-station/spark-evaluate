# spark-evaluate
Evaluate service

- [Meridian spec](https://www.notion.so/pl-strflt/Meridian-Design-Doc-07-Flexible-preprocessing-1b8f2f19ca7d4fd4b74a1e57e7d7ef8a?pvs=4)
- [Meridian evaluate service](https://github.com/Meridian-IE/evaluate-service)

## Development

```bash
$ WALLET_SEED=$(cat secrets/mnemonic) WEB3_STORAGE_API_TOKEN=$(cat secrets/web3storage) npm start
$ npm test
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
