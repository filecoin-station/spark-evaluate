const {
  IE_CONTRACT_ADDRESS = '0x8460766Edc62B525fc1FA4D628FC79229dC73031',
  // FIXME Add back chain.love either when it's online or once onContractEvent
  // supports rpc failover
  // RPC_URLS = 'https://api.node.glif.io/rpc/v0,https://api.chain.love/rpc/v1',
  RPC_URLS = 'https://api.node.glif.io/rpc/v0',
  GLIF_TOKEN,
  DATABASE_URL = 'postgres://localhost:5432/spark_stats',
  SPARK_API = 'https://api.filspark.com'
} = process.env

const rpcUrls = RPC_URLS.split(',')
const RPC_URL = rpcUrls[Math.floor(Math.random() * rpcUrls.length)]
console.log(`Selected JSON-RPC endpoint ${RPC_URL}`)

const rpcHeaders = {}
if (RPC_URL.includes('glif') && GLIF_TOKEN) {
  rpcHeaders.Authorization = `Bearer ${GLIF_TOKEN}`
}

export {
  IE_CONTRACT_ADDRESS,
  RPC_URL,
  DATABASE_URL,
  SPARK_API,
  rpcHeaders
}
