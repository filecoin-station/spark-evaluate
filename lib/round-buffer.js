import fs from 'node:fs/promises'
import { RoundData } from './round.js'

const ROUND_BUFFER_PATH = '/var/lib/spark-evaluate/round-buffer.ndjson'

export const recoverRound = async () => {
  let roundBuffer
  try {
    roundBuffer = await fs.readFile(ROUND_BUFFER_PATH, 'utf8')
  } catch (err) {
    if (err.code !== 'ENOENT') {
      throw err
    }
  }
  if (roundBuffer) {
    const lines = roundBuffer.split('\n').filter(Boolean)
    if (lines.length > 1) {
      const round = new RoundData(JSON.parse(lines[0]))
      for (const line of lines.slice(1)) {
        round.measurements.push(JSON.parse(line))
      }
      return round
    }
  }
}

export const appendToRoundBuffer = async validMeasurements => {
  await fs.appendFile(
    ROUND_BUFFER_PATH,
    validMeasurements.map(m => JSON.stringify(m)).join('\n') + '\n'
  )
}

export const clearRoundBuffer = async () => {
  try {
    await fs.writeFile(ROUND_BUFFER_PATH, '')
  } catch (err) {
    if (err.code !== 'ENOENT') {
      throw err
    }
  }
}
