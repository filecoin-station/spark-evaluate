import { CarWriter } from '@ipld/car'
import * as dagJSON from '@ipld/dag-json'
import { sha256 } from 'multiformats/hashes/sha2'
import { CID } from 'multiformats'

export async function createDagJsonCar (json) {
  const bytes = dagJSON.encode(json)
  const hash = await sha256.digest(bytes)
  const cid = CID.create(1, dagJSON.code, hash)
  const car = await createCar({ cid, bytes }, cid)
  return { cid, car }
}

async function createCar (block, root) {
  const { writer, out } = CarWriter.create(root)
  const [chunks] = await Promise.all([
    (async () => {
      const chunks = []
      for await (const chunk of out) chunks.push(chunk)
      return chunks
    })(),
    (async () => {
      await writer.put(block)
      await writer.close()
    })()
  ])
  return Object.assign(new Blob(chunks), { version: 1, roots: [root] })
}
