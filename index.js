import { preprocess } from './lib/preprocess.js'
import { evaluate } from './lib/evaluate.js'

export const startEvaluate = ({
  ieContract,
  ieContractWithSigner,
  web3Storage
}) => {
  const rounds = {}
  const cidsSeen = []
  const roundsSeen = []

  // Listen for events
  ieContract.on('MeasurementsAdded', (cid, _roundIndex) => {
    const roundIndex = Number(_roundIndex)
    if (cidsSeen.includes(cid)) return
    cidsSeen.push(cid)

    console.log('Event: MeasurementsAdded', { roundIndex })
    // Preprocess
    preprocess({ rounds, cid, roundIndex, web3Storage }).catch(console.error)
  })

  ieContract.on('RoundStart', _roundIndex => {
    const roundIndex = Number(_roundIndex)
    if (roundsSeen.includes(roundIndex)) return
    roundsSeen.push(roundIndex)

    console.log('Event: RoundStart', { roundIndex })
    // Evaluate previous round
    evaluate({
      rounds,
      roundIndex: roundIndex - 1,
      ieContractWithSigner
    }).catch(console.error)
  })
}
