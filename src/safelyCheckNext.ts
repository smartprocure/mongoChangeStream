import _debug from 'debug'

import type { Cursor } from './types.js'

const debug = _debug('mongochangestream:safelyCheckNext')

/**
 * Get next record without throwing an exception.
 * Get the last error safely via `getLastError`.
 */
export const safelyCheckNext = (cursor: Cursor) => {
  debug('safelyCheckNext called')
  let lastError: unknown

  // Returns the next record, or null if there are no more records.
  //
  // NOTE: In the event that there are no more records, this will still block
  // for a minute or so, until there is a "no-op" event in the oplog. At that
  // point, it will return null and a fresh resume token can be retrieved from
  // the change stream object.
  //
  // Also returns null if there was an error, so it's important to check if
  // there was an error afterwards via `errorExists()` and/or `getLastError()`.
  const getNext = async () => {
    debug('getNext called')
    try {
      return await cursor.tryNext()
    } catch (e) {
      debug('getNext error: %o', e)
      lastError = e
      return null
    }
  }

  const errorExists = () => Boolean(lastError)
  const getLastError = () => lastError

  return { getNext, errorExists, getLastError }
}
