# spark-evaluate
Evaluate service

- [Meridian spec](https://www.notion.so/pl-strflt/Meridian-Design-Doc-07-Flexible-preprocessing-1b8f2f19ca7d4fd4b74a1e57e7d7ef8a?pvs=4)
- [Meridian evaluate service](https://github.com/Meridian-IE/evaluate-service)

## Development

```bash
$ WALLET_SEED=$(cat secrets/mnemonic) WEB3_STORAGE_API_TOKEN=$(cat secrets/web3storage) npm start
$ npm test
```

## Deployment

```bash
$ git push
```