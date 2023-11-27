export const VALID_PARTICIPANT_ADDRESS = '0x000000000000000000000000000000000000dEaD'

export const VALID_TASK = {
  cid: 'QmUuEoBdjC8D1PfWZCc7JCSK8nj7TV6HbXWDHYHzZHCVGS',
  providerAddress: '/dns4/production-ipfs-peer.pinata.cloud/tcp/3000/ws/p2p/Qma8ddFEQWEU8ijWvdxXm3nxU7oHsRtCykAaVz8WUYhiKn',
  protocol: 'bitswap'
}
Object.freeze(VALID_TASK)

/** @type {import('../lib/typings').Measurement} */
export const VALID_MEASUREMENT = {
  cid: VALID_TASK.cid,
  provider_address: VALID_TASK.providerAddress,
  protocol: VALID_TASK.protocol,
  participantAddress: VALID_PARTICIPANT_ADDRESS,
  inet_group: 'some-group-id',
  status_code: 200,
  timeout: false,
  car_too_large: false,
  start_at: '2023-11-01T09:00:00.000Z',
  first_byte_at: '2023-11-01T09:00:01.000Z',
  end_at: '2023-11-01T09:00:02.000Z',
  finished_at: '2023-11-01T09:00:10.000Z',
  byte_length: 1024,
  retrievalResult: 'OK'
}

// Fraud detection is mutating the measurements parsed from JSON
// To prevent tests from accidentally mutating data used by subsequent tests,
// we freeze this test data object. If we forget to clone this default measurement
// then such test will immediately fail.
Object.freeze(VALID_MEASUREMENT)
