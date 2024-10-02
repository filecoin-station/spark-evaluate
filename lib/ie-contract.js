import { ethers } from 'ethers'
import { rpcUrls, GLIF_TOKEN } from './config.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'

export const createMeridianContract = async (contractAddress = SparkImpactEvaluator.ADDRESS) => {
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
    contractAddress,
    SparkImpactEvaluator.ABI,
    provider
  )

  return { ieContract, provider }
}
