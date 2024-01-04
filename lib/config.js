const {
  IE_CONTRACT_ADDRESS = '0xaaef78eaf86dcf34f275288752e892424dda9341',
  RPC_URLS = 'https://api.node.glif.io/rpc/v0,https://api.chain.love/rpc/v1',
  GLIF_TOKEN,
  SPARK_API = 'https://api.filspark.com'
} = process.env

const rpcUrls = RPC_URLS.split(',')
const RPC_URL = rpcUrls[Math.floor(Math.random() * rpcUrls.length)]
console.log(`Selected JSON-RPC endpoint ${RPC_URL}`)

const rpcHeaders = {}
if (RPC_URL.includes('glif')) {
  rpcHeaders.Authorization = `Bearer ${GLIF_TOKEN}`
}

export {
  IE_CONTRACT_ADDRESS,
  RPC_URL,
  SPARK_API,
  rpcHeaders
}
