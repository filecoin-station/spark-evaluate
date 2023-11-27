import assert from 'node:assert'

export const assertPointFieldValue = (point, fieldName, expectedValue) => {
  const actualValue = point.fields[fieldName]
  assert.strictEqual(
    actualValue,
    expectedValue,
   `Expected ${point.name}.fields.${fieldName} to equal ${expectedValue} but found ${actualValue}`
  )
}
