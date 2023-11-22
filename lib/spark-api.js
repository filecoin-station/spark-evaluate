import { SPARK_API } from './config.js'

/**
 * @param {string} contractAddress
 * @param {BigInt} roundIndex
 * @param {import('./typings').RecordTelemetryFn} recordTelemetry
 */
export const fetchRoundDetails = async (
  contractAddress,
  roundIndex,
  recordTelemetry
) => {
  const start = new Date()
  let status = 0
  let taskCount

  try {
    const res = await fetch(`${SPARK_API}/rounds/meridian/${contractAddress}/${roundIndex}`)
    status = res.status
    if (!res.ok) {
      const msg = `Cannot fetch tasks for round ${contractAddress}/${roundIndex}: ${status}\n${await res.text()}`
      throw new Error(msg)
    }
    /** @type {import('./typings.d.ts').RoundDetails} */
    const details = res.json()
    taskCount = details.retrievalTasks?.length
    return details
  } finally {
    recordTelemetry('fetch_tasks_for_round', point => {
      point.stringField('contract_address', contractAddress)
      point.intField('round_index', roundIndex)
      point.intField('fetch_duration_ms', new Date() - start)
      point.intField('status', status)
      point.intField('task_count', taskCount ?? -1)
    })
  }
}
