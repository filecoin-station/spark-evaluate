import fs from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { ethers } from 'ethers'
import { IE_CONTRACT_ADDRESS, RPC_URL, rpcHeaders } from './config.js'

export const createMeridianContract = async (contractAddress = IE_CONTRACT_ADDRESS) => {
  const fetchRequest = new ethers.FetchRequest(RPC_URL)
  fetchRequest.setHeader('Authorization', rpcHeaders.Authorization || '')
  const provider = new ethers.JsonRpcProvider(
    fetchRequest,
    null,
    { polling: true }
  )

  // Uncomment for troubleshooting
  // provider.on('debug', d => console.log('[ethers:debug %s] %s %o', new Date().toISOString().split('T')[1], d.action, d.payload ?? d.result))

  const ieContract = new ethers.Contract(
    contractAddress,
    await fs.readFile(
      fileURLToPath(new URL('abi.json', import.meta.url)),
      'utf8'
    ),
    provider
  )
  return { ieContract, provider }
}
