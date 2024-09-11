import { ethers } from 'ethers'
import { rpcUrls, GLIF_TOKEN } from './config.js'
import * as SparkImpactEvaluator from '@filecoin-station/spark-impact-evaluator'
import * as SparkEvaluationsRecentParticipants from '@filecoin-station/spark-evaluations-recent-participants'

export const provider = new ethers.FallbackProvider(rpcUrls.map(url => {
  const fetchRequest = new ethers.FetchRequest(url)
  fetchRequest.setHeader('Authorization', `Bearer ${GLIF_TOKEN}`)
  return new ethers.JsonRpcProvider(fetchRequest, null, {
    polling: true,
    batchMaxCount: 10
  })
}))

// Uncomment for troubleshooting
// provider.on('debug', d => console.log('[ethers:debug %s] %s %o', new Date().toISOString().split('T')[1], d.action, d.payload ?? d.result))

export const createMeridianContract = (contractAddress = SparkImpactEvaluator.ADDRESS) => {
  return new ethers.Contract(
    contractAddress,
    SparkImpactEvaluator.ABI,
    provider
  )
}

export const recentParticipantsContract = new ethers.Contract(
  SparkEvaluationsRecentParticipants.ADDRESS,
  SparkEvaluationsRecentParticipants.ABI,
  provider
)
