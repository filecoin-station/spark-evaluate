import assert from 'node:assert'
import * as Sentry from '@sentry/node'
import { preprocess } from './lib/preprocess.js'
import { evaluate } from './lib/evaluate.js'
// TODO: implement typings - see https://github.com/filecoin-station/on-contract-event/issues/2
import { onContractEvent } from 'on-contract-event'
import { RoundData } from './lib/round.js'
import timers from 'node:timers/promises'

// Tweak this value to improve the chances of the data being available
const PREPROCESS_DELAY = 60_000

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

  const onMeasurementsAdded = async (cid, _roundIndex) => {
    const roundIndex = BigInt(_roundIndex)
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
    console.log(`Sleeping for ${PREPROCESS_DELAY}ms before preprocessing to improve chances of the data being available`)
    await timers.setTimeout(PREPROCESS_DELAY)
    console.log(`Now preprocessing measurements for CID ${cid} in round ${roundIndex}`)

    // Preprocess
    try {
      await preprocess({
        round: rounds.current,
        cid,
        roundIndex,
        fetchMeasurements,
        recordTelemetry,
        logger
      })
    } catch (err) {
      console.error(err)
      Sentry.captureException(err, {
        extra: {
          roundIndex,
          measurementsCid: cid
        }
      })
    }
  }

  const onRoundStart = async (_roundIndex) => {
    const roundIndex = BigInt(_roundIndex)
    if (roundsSeen.includes(roundIndex)) return
    roundsSeen.push(roundIndex)
    if (roundsSeen.length > 1000) roundsSeen.shift()

    console.log('Event: RoundStart', { roundIndex })

    if (!rounds.current) {
      console.error('No current round data available, skipping evaluation')
      return
    }

    // TODO: Fix this properly and implement a signalling mechanism allowing the "preprocess" step
    // to notify the "evaluate" when the preprocessing is done, so that we don't have to use a timer
    // here. See also https://github.com/filecoin-station/spark-evaluate/issues/64
    const delay = PREPROCESS_DELAY + 60_000
    console.log(`Sleeping for ${delay}ms before evaluating the round to let the preprocess step finish for the last batch of measurements`)
    await timers.setTimeout(delay)
    console.log(`Now evaluating the round ${roundIndex}`)

    rounds.previous = rounds.current
    rounds.current = new RoundData(roundIndex)

    // Evaluate previous round
    evaluate({
      round: rounds.previous,
      roundIndex: roundIndex - 1n,
      ieContractWithSigner,
      fetchRoundDetails,
      recordTelemetry,
      createPgClient,
      logger
    }).catch(err => {
      console.error(err)
      Sentry.captureException(err, {
        extra: {
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
      onMeasurementsAdded(...event.args).catch(err => {
        console.error(err)
        Sentry.captureException(err)
      })
    } else if (event.name === 'RoundStart') {
      onRoundStart(...event.args).catch(err => {
        console.error(err)
        Sentry.captureException(err)
      })
    }
  }
}
