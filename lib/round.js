export class RoundData {
  /** @type {Map<string, string>} */
  #knownStrings

  /**
   * @param {bigint} index
   */
  constructor (index) {
    this.index = index
    /** @type {import('./preprocess.js').Measurement[]} */
    this.measurementBatches = []
    this.measurements = []
    this.details = null
    this.#knownStrings = new Map()

    // defined as a rocket function that can be detached
    // from this object and passed around as a lambda fn
    this.pointerize = (str) => {
      if (str === undefined || str === null) return str
      const found = this.#knownStrings.get(str)
      if (found !== undefined) return found
      this.#knownStrings.set(str, str)
      return str
    }
  }
}
