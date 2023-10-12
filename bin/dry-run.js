import { evaluate } from '../lib/evaluate.js'
import { preprocess, fetchMeasurementsViaGateway } from '../lib/preprocess.js'
import { fetchRoundDetails } from '../lib/spark-api.js'

const [nodePath, selfPath, contractAddress, roundIndex, ...measurementCids] = process.argv

const USAGE = `
Usage:
  ${nodePath} ${selfPath} <contract-address> <round-index> <measurements-cid-1> [cid2 [cid3...]]
`

if (!contractAddress) {
  console.error('Missing required argument: contractAddress')
  console.log(USAGE)
  process.exit(1)
}

if (!roundIndex) {
  console.error('Missing required argument: roundIndex')
  console.log(USAGE)
  process.exit(1)
}

// TODO: fetch measurement CIDs from on-chain events
if (!measurementCids.length) {
  console.error('Missing required argument: measurements CID (at least one is required)')
  console.log(USAGE)
  process.exit(1)
}

const recordTelemetry = (measurementName, fn) => { /* no-op */ }

console.log('Evaluating round %s of contract %s', roundIndex, contractAddress)

console.log('==PREPROCESS==')

const rounds = {}
for (const cid of measurementCids) {
  await preprocess({
    roundIndex,
    rounds,
    cid,
    fetchMeasurements: fetchMeasurementsViaGateway,
    recordTelemetry,
    logger: console
  })
}

console.log('Fetched %s measurements', rounds[roundIndex].length)

console.log('==EVALUATE==')
const ieContractWithSigner = {
  address: contractAddress,
  async setScores (_roundIndex, participantAddresses, scores) {
    console.log('==EVALUATION RESULTS==')
    console.log('participants:', participantAddresses)
    console.log('scores:', scores)
    console.log('==END OF RESULTS==')
    return { hash: '0x234' }
  }
}

await evaluate({
  roundIndex,
  rounds,
  fetchRoundDetails,
  ieContractWithSigner,
  logger: console,
  recordTelemetry
})
