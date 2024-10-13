import { ethers } from 'ethers'

// TODO: Add tests
export const setScores = async (signer, participants, scores) => {
  const digest = ethers.solidityPackedKeccak256(
    ['address[]', 'uint256[]'],
    [participants, scores]
  )
  const signed = await signer.signMessage(digest)
  const { v, r, s } = ethers.Signature.from(signed)
  const res = await fetch('https://spark-rewards.fly.dev/scores', {
    method: 'POST',
    body: JSON.stringify({
      participants,
      scores: scores.map(n => String(n)),
      signature: { v, r, s }
    })
  })
  if (res.status !== 200) {
    throw new Error(
      `spark-rewards responded with ${res.status}: ${await res.text().catch(err => err)}`
    )
  }
}
