import { createDagJsonCar, createCar } from '../lib/car.js'
import assert from 'node:assert'
import * as dagJSON from '@ipld/dag-json'
import { sha256 } from 'multiformats/hashes/sha2'
import { CID } from 'multiformats'
import { CarReader } from '@ipld/car'

describe('CAR', () => {
  describe('createDagJsonCar', () => {
    it('Creates a CAR file (with CID) from a JSON object', async () => {
      const object = { beep: 'boop' }
      const { cid, car } = await createDagJsonCar(object)
      assert.strictEqual(cid.toString(), 'baguqeerawg5jfpiy2g5xp5d422uwa3mpyzkmiguoeecesds7q65mn2hdoa4q')
      const reader = await CarReader.fromBytes(await car.bytes())
      const block = await reader.get(cid)
      assert.deepStrictEqual(block.cid, cid)
      assert.deepStrictEqual(dagJSON.decode(block.bytes), object)
    })
  })
  describe('createCar', () => {
    it('Creates a CAR blob from a block', async () => {
      const payload = 'hi'
      const bytes = dagJSON.encode(payload)
      const hash = await sha256.digest(bytes)
      const cid = CID.create(1, dagJSON.code, hash)
      const car = await createCar({ cid, bytes }, cid)
      assert.strictEqual(cid.toString(), 'baguqeerawsixpycync327ducuzcmdtra4uq26rsjplpk77ugduuu3g2lw5pa')
      const reader = await CarReader.fromBytes(await car.bytes())
      const block = await reader.get(cid)
      assert.deepStrictEqual(block.cid, cid)
      assert.strictEqual(dagJSON.decode(block.bytes), payload)
    })
  })
})
