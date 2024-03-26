export const VALID_PARTICIPANT_ADDRESS = '0x000000000000000000000000000000000000dEaD'

export const VALID_TASK = {
  cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
  minerId: 'f1test'
}
Object.freeze(VALID_TASK)

/** @type {import('../lib/typings').Measurement} */
export const VALID_MEASUREMENT = {
  cid: VALID_TASK.cid,
  minerId: VALID_TASK.minerId,
  provider_address: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
  protocol: 'bitswap',
  participantAddress: VALID_PARTICIPANT_ADDRESS,
  inet_group: 'some-group-id',
  status_code: 200,
  timeout: false,
  car_too_large: false,
  start_at: new Date('2023-11-01T09:00:00.000Z').getTime(),
  first_byte_at: new Date('2023-11-01T09:00:01.000Z').getTime(),
  end_at: new Date('2023-11-01T09:00:02.000Z').getTime(),
  finished_at: new Date('2023-11-01T09:00:10.000Z').getTime(),
  byte_length: 1024,
  retrievalResult: 'OK',
  indexerResult: 'OK'
}

// Fraud detection is mutating the measurements parsed from JSON
// To prevent tests from accidentally mutating data used by subsequent tests,
// we freeze this test data object. If we forget to clone this default measurement
// then such test will immediately fail.
Object.freeze(VALID_MEASUREMENT)

export const today = () => {
  const d = new Date()
  d.setHours(0)
  d.setMinutes(0)
  d.setSeconds(0)
  d.setMilliseconds(0)
  return d
}
