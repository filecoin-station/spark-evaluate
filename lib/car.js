import { CarWriter } from '@ipld/car'

export async function createCAR (block, root) {
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
