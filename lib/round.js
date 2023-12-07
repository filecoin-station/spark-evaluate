export class RoundData {
  /** @type {Map<string, string>} */
  #knownStrings

  constructor () {
    /** @type {import('./preprocess').Measurement[]} */
    this.measurements = []
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
