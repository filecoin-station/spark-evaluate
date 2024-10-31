import { InfluxDB, Point } from '@influxdata/influxdb-client'
import createDebug from 'debug'

const debug = createDebug('spark:evaluate:telemetry')

export const createInflux = token => {
  const influx = new InfluxDB({
    url: 'https://eu-central-1-1.aws.cloud2.influxdata.com',
    // spark-evaluate-write
    token
  })
  const writeClient = influx.getWriteApi(
    'Filecoin Station', // org
    'spark-evaluate', // bucket
    'ns' // precision
  )
  setInterval(() => {
    writeClient.flush().catch(console.error)
  }, 10_000).unref()

  return {
    recordTelemetry: (name, fn) => {
      const point = new Point(name)
      fn(point)
      writeClient.writePoint(point)
      debug('%s %o', name, point)
    }
  }
}

export { Point }
