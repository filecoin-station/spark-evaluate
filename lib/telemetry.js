import { InfluxDB, Point } from '@influxdata/influxdb-client'

const influx = new InfluxDB({
  url: 'https://eu-central-1-1.aws.cloud2.influxdata.com',
  // spark-evaluate-write
  token: 'n_V3rr3GQodR0mUNMEC5m13OnWs_vUEFud00BkCJoFJkOc52jCF-p4nbIQPrgyfuisAw8-p1q2Z2c3ZLVA8XYw=='
})
const writeClient = influx.getWriteApi(
  'Filecoin Station', // org
  'spark-evaluate', // bucket
  'ns' // precision
)

setInterval(() => {
  writeClient.flush().catch(console.error)
}, 10_000).unref()

export const record = (name, fn) => {
  const point = new Point(name)
  fn(point)
  writeClient.writePoint(point)
}

export const close = () => writeClient.close()
