import { SPARK_API } from './config.js'

export const fetchRoundDetails = async (/** @type {BigInt} */ roundIndex, recordTelemetry) => {
  const start = new Date()
  let status = 0

  try {
    const res = await fetch(`${SPARK_API}/rounds/${roundIndex}`)
    status = res.status
    if (!res.ok) {
      const msg = `Cannot fetch tasks for round ${roundIndex}: ${status}\n${await res.text()}`
      throw new Error(msg)
    }
    /** @type {import('./typings.d.ts').RoundDetails} */
    const details = res.json()
    return details
  } finally {
    recordTelemetry('fetch_tasks_for_round', point => {
      point.intField('round_index', roundIndex)
      point.intField('fetch_duration_ms', new Date() - start)
      point.intField('status', status)
    })
  }
}
