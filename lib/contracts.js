import { ethers } from 'ethers'
import { rpcUrls, GLIF_TOKEN } from './config.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import fs from 'node:fs/promises'
import { fileURLToPath } from 'node:url'

const rsrContractAbi = JSON.parse(
  await fs.readFile(
    fileURLToPath(new URL('./rsrContract.json', import.meta.url)),
    'utf8'
  )
).abi

export const createContracts = (ieContractAddress = SparkImpactEvaluator.ADDRESS) => {
  const provider = new ethers.FallbackProvider(rpcUrls.map(url => {
    const fetchRequest = new ethers.FetchRequest(url)
    fetchRequest.setHeader('Authorization', `Bearer ${GLIF_TOKEN}`)
    return new ethers.JsonRpcProvider(fetchRequest, null, {
      polling: true,
      batchMaxCount: 10
    })
  }))

  // Uncomment for troubleshooting
  // provider.on('debug', d => console.log('[ethers:debug %s] %s %o', new Date().toISOString().split('T')[1], d.action, d.payload ?? d.result))

  const ieContract = new ethers.Contract(
    ieContractAddress,
    SparkImpactEvaluator.ABI,
    provider
  )

  const rsrContract = new ethers.Contract(
    '0x620bfc5AdE7eeEE90034B05DC9Bb5b540336ff90',
    rsrContractAbi,
    provider
  )

  return { ieContract, rsrContract, provider }
}
