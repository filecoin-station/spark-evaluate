import { ethers } from 'ethers'

// TODO: Add tests
export const submitScores = async (signer, participants, values) => {
  const digest = ethers.solidityPackedKeccak256(
    ['address[]', 'uint256[]'],
    [participants, values]
  )
  const signed = await signer.signMessage(digest)
  const { v, r, s } = ethers.Signature.from(signed)
  const res = await fetch('https://spark-rewards.fly.dev/scores', {
    method: 'POST',
    body: JSON.stringify({
      participants,
      values: values.map(n => String(n)),
      signature: { v, r, s }
    })
  })
  if (res.status !== 200) {
    throw new Error(
      `spark-rewards responded with ${res.status}: ${await res.text().catch(err => err)}`
    )
  }
}
