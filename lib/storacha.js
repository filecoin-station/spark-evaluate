import * as Client from '@web3-storage/w3up-client'
import { ed25519 } from '@ucanto/principal'
import { CarReader } from '@ipld/car'
import { importDAG } from '@ucanto/core/delegation'

async function parseProof (data) {
  const blocks = []
  const reader = await CarReader.fromBytes(Buffer.from(data, 'base64'))
  for await (const block of reader.blocks()) {
    blocks.push(block)
  }
  return importDAG(blocks)
}

export async function createStorachaClient ({ secretKey, proof }) {
  const principal = ed25519.Signer.parse(secretKey)
  const client = await Client.create({ principal })
  const space = await client.addSpace(await parseProof(proof))
  await client.setCurrentSpace(space.did())
  return client
}
