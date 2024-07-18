import { ethers } from 'ethers'
import { RPC_URL, rpcHeaders } from './config.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'

export const createMeridianContract = async (contractAddress = SparkImpactEvaluator.ADDRESS) => {
  const fetchRequest = new ethers.FetchRequest(RPC_URL)
  fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
  const provider = new ethers.JsonRpcProvider(
    fetchRequest,
    null,
    {
      polling: true,
      batchMaxCount: 10
    }
  )

  // Uncomment for troubleshooting
  // provider.on('debug', d => console.log('[ethers:debug %s] %s %o', new Date().toISOString().split('T')[1], d.action, d.payload ?? d.result))

  const ieContract = new ethers.Contract(
    contractAddress,
    SparkImpactEvaluator.ABI,
    provider
  )
  return { ieContract, provider }
}
