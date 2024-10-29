import { createDagJsonCar, createCar } from '../lib/car.js'
import assert from 'node:assert'
import * as dagJSON from '@ipld/dag-json'
import { sha256 } from 'multiformats/hashes/sha2'
import { CID } from 'multiformats'

describe('CAR', () => {
  describe('createDagJsonCar', () => {
    it('Creates a CAR file (with CID) from a JSON object', async () => {
      const { cid, car } = await createDagJsonCar('hi')
      assert(cid)
      assert(car)
      // TODO: Test that CAR can be decoded
    })
  })
  describe('createCar', () => {
    it('Creates a CAR blob from a block', async () => {
      const bytes = dagJSON.encode('hi')
      const hash = await sha256.digest(bytes)
      const cid = CID.create(1, dagJSON.code, hash)
      const car = await createCar({ cid, bytes }, cid)
      assert(cid)
      assert(car)
      // TODO: Test that CAR can be decoded
    })
  })
})