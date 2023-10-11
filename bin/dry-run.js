import { evaluate } from '../lib/evaluate.js'
import { fetchRoundDetails } from '../lib/spark-api.js'

const [nodePath, selfPath, roundIndex, ...measurementCids] = process.argv

const USAGE = `
Usage:
  ${nodePath} ${selfPath} <round-index> <measurements-cid-1> [cid2 [cid3...]]
`

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

// TODO: fetch measurements via preprocess
const measurements = []
for (const cid of measurementCids) {
  const res = await fetch(`https://${cid}.ipfs.w3s.link/measurements.json`)
  if (!res.ok) {
    console.log('Cannot fetch %s: %s %s', cid, res.status, await res.text())
    process.exit(2)
  }
  const data = await res.json()
  measurements.push(...data)
}

console.log('Evaluating round %s', roundIndex)
console.log('Fetched %s measurements', measurements.length)

const ieContractWithSigner = {
  async setScores (_roundIndex, participantAddresses, scores) {
    console.log('==EVALUATION RESULTS==')
    console.log('participants:', participantAddresses)
    console.log('scores:', scores)
    console.log('==END OF RESULTS==')
    return { hash: '0x234' }
  }
}
const recordTelemetry = (measurementName, fn) => { /* no-op */ }

await evaluate({
  roundIndex,
  rounds: { [roundIndex]: measurements },
  fetchRoundDetails,
  ieContractWithSigner,
  logger: console,
  recordTelemetry
})
