import assert from 'node:assert'
import * as Sentry from '@sentry/node'
import { preprocess } from './lib/preprocess.js'
import { evaluate } from './lib/evaluate.js'
import { onContractEvent } from './lib/on-contract-event.js'
import { RoundData } from './lib/round.js'

export const startEvaluate = async ({
  ieContract,
  ieContractWithSigner,
  provider,
  rpcUrl,
  rpcHeaders,
  fetchMeasurements,
  fetchRoundDetails,
  recordTelemetry,
  createPgClient,
  logger
}) => {
  assert(typeof createPgClient === 'function', 'createPgClient must be a function')

  const rounds = {
    current: null,
    previous: null
  }
  const cidsSeen = []
  const roundsSeen = []

  const onMeasurementsAdded = (cid, _roundIndex) => {
    const roundIndex = Number(_roundIndex)
    if (cidsSeen.includes(cid)) return
    cidsSeen.push(cid)
    if (cidsSeen.length > 1000) cidsSeen.shift()

    if (!rounds.current) {
      rounds.current = new RoundData(roundIndex)
    } else if (rounds.current.index !== roundIndex) {
      // This should never happen
      throw new Error(
        `Round index mismatch: ${rounds.current.index} !== ${roundIndex}`
      )
    }

    console.log('Event: MeasurementsAdded', { roundIndex })
    // Preprocess
    preprocess({
      round: rounds.current,
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
  }

  const onRoundStart = (_roundIndex) => {
    const roundIndex = Number(_roundIndex)
    if (roundsSeen.includes(roundIndex)) return
    roundsSeen.push(roundIndex)
    if (roundsSeen.length > 1000) roundsSeen.shift()

    console.log('Event: RoundStart', { roundIndex })

    if (!rounds.current) {
      console.error('No current round data available, skipping evaluation')
      return
    }

    rounds.previous = rounds.current
    rounds.current = new RoundData(roundIndex)

    // Evaluate previous round
    evaluate({
      round: rounds.previous,
      roundIndex: roundIndex - 1,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger
    }).catch(err => {
      console.error(err)
      Sentry.captureException(err, {
        extras: {
          roundIndex
        }
      })
    })
  }

  // Listen for events
  const it = onContractEvent({
    contract: ieContract,
    provider,
    rpcUrl,
    rpcHeaders
  })
  for await (const event of it) {
    if (event.name === 'MeasurementsAdded') {
      onMeasurementsAdded(...event.args)
    } else if (event.name === 'RoundStart') {
      onRoundStart(...event.args)
    }
  }
}
