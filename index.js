import * as Sentry from '@sentry/node'
import { preprocess } from './lib/preprocess.js'
import { evaluate } from './lib/evaluate.js'

export const startEvaluate = ({
  ieContract,
  ieContractWithSigner,
  fetchMeasurements,
  fetchRoundDetails,
  recordTelemetry,
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
      fetchMeasurements,
      recordTelemetry,
      logger
    }).catch(err => {
      // See https://github.com/filecoin-station/spark-evaluate/issues/36
      // Because each error message contains unique CID, Sentry is not able to group these errors
      // Let's wrap the error message in a new Error object as a cause
      if (typeof err === 'string' && err.match(/ENOENT: no such file or directory, open.*\/bafy/)) {
        err = new Error('web3.storage cannot find block\'s temp file', { cause: err })
      }

      console.error(err)
      Sentry.captureException(err, {
        extras: {
          roundIndex,
          measurementsCid: cid
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
      fetchRoundDetails,
      recordTelemetry,
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
