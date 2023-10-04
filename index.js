import * as Sentry from '@sentry/node'
import { preprocess } from './lib/preprocess.js'
import { evaluate } from './lib/evaluate.js'

export const startEvaluate = ({
  ieContract,
  ieContractWithSigner,
  web3Storage,
  logger
}) => {
  const rounds = {}
  const cidsSeen = []
  const roundsSeen = []

  // Listen for events
  ieContract.on('MeasurementsAdded', (cid, _roundIndex) => {
    const roundIndex = Number(_roundIndex)
    if (cidsSeen.includes(cid)) return
    cidsSeen.push(cid)
    if (cidsSeen.length > 1000) cidsSeen.shift()

    console.log('Event: MeasurementsAdded', { roundIndex })
    // Preprocess
    preprocess({
      rounds,
      cid,
      roundIndex,
      web3Storage,
      logger
    }).catch(err => {
      console.error(err)
      Sentry.captureException(err, {
        extras: {
          roundIndex,
          measurementsCid: cid,
        }
      })
    })
  })

  ieContract.on('RoundStart', _roundIndex => {
    const roundIndex = Number(_roundIndex)
    if (roundsSeen.includes(roundIndex)) return
    roundsSeen.push(roundIndex)
    if (roundsSeen.length > 1000) roundsSeen.shift()

    console.log('Event: RoundStart', { roundIndex })
    // Evaluate previous round
    evaluate({
      rounds,
      roundIndex: roundIndex - 1,
      ieContractWithSigner,
      logger
    }).catch(err => {
      console.error(err)
      Sentry.captureException(err, {
        extras: {
          roundIndex
        }
      })
    })
  })
}
