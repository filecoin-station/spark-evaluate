import assert from 'node:assert'

/**
 * @param {import('../../lib/telemetry.js').Point[]} recordings
 * @param {string} name
 * @returns {import('../../lib/telemetry.js').Point}
 */
export const assertRecordedTelemetryPoint = (recordings, name) => {
  const point = recordings.find(p => getPointName(p) === name)
  assert(!!point,
    `No telemetry point "spark_version" was recorded. Actual points: ${JSON.stringify(recordings.map(getPointName))}`)
  return point
}

export const assertPointFieldValue = (point, fieldName, expectedValue) => {
  const actualValue = point.fields[fieldName]
  assert.strictEqual(
    actualValue,
    expectedValue,
   `Expected ${point.name}.fields.${fieldName} to equal ${expectedValue} but found ${actualValue}`
  )
}

/**
 * @param {import('../../lib/telemetry.js').Point} point
 */
export const getPointName = (point) => {
  // Point.name is marked as a private property at the TypeScript level
  return /** @type {any} */(point).name
}
