import _debug from 'debug'
import { EventEmitter } from 'eventemitter3'
import type { Redis } from 'ioredis'
import _ from 'lodash/fp.js'
import {
  type ChangeStream,
  type ChangeStreamDocument,
  type ChangeStreamInsertDocument,
  type Collection,
  type Db,
  type Document,
  ObjectId,
} from 'mongodb'
import * as mongodb from 'mongodb'
import ms from 'ms'
import retry, { type Options as RetryOptions } from 'p-retry'
import {
  batchQueue,
  defer,
  type Deferred,
  pausable,
  type QueueOptions,
} from 'prom-utils'
import { fsm, type StateTransitions } from 'simple-machines'

import { safelyCheckNext } from './safelyCheckNext.js'
import type {
  ChangeOptions,
  ChangeStreamOptions,
  CursorErrorEvent,
  Events,
  JSONSchema,
  ProcessChangeStreamRecords,
  ProcessInitialScanRecords,
  ScanOptions,
  SchemaChangeEvent,
  SimpleState,
  SortField,
  State,
  StatsEvent,
  SyncOptions,
} from './types.js'
import {
  docToChangeStreamInsert,
  generatePipelineFromOmit,
  getCollectionKey,
  omitFieldsForUpdate,
  removeUnusedFields,
  setDefaults,
  when,
} from './util.js'

const debug = _debug('mongochangestream')

const keyPrefix = 'mongoChangeStream'

/**
 * Get Redis keys used for the collection.
 */
export const getKeys = (collection: Collection, options: SyncOptions) => {
  const collectionKey = getCollectionKey(collection)
  const uniqueId = options.uniqueId ? `:${options.uniqueId}` : ''
  const collectionPrefix = `${keyPrefix}:${collectionKey}${uniqueId}`
  const scanCompletedKey = `${collectionPrefix}:initialScanCompletedOn`
  const lastScanIdKey = `${collectionPrefix}:lastScanId`
  const changeStreamTokenKey = `${collectionPrefix}:changeStreamToken`
  const schemaKey = `${collectionPrefix}:schema`
  const lastChangeProcessedAtKey = `${collectionPrefix}:lastChangeProcessedAt`
  const lastScanProcessedAtKey = `${collectionPrefix}:lastScanProcessedAt`
  const resyncKey = `${collectionPrefix}:resync`
  return {
    scanCompletedKey,
    lastScanIdKey,
    changeStreamTokenKey,
    schemaKey,
    lastChangeProcessedAtKey,
    lastScanProcessedAtKey,
    resyncKey,
  }
}

const stateTransitions: StateTransitions<State> = {
  stopped: ['starting'],
  starting: ['started', 'stopped'],
  started: ['stopping'],
  stopping: ['stopped'],
}

const simpleStateTransistions: StateTransitions<SimpleState> = {
  started: ['stopped'],
  stopped: ['started'],
}

const stateFields = ['get', 'is'] as const

const defaultRetryOptions: RetryOptions = {
  minTimeout: ms('30s'),
  maxTimeout: ms('1h'),
  // This translates to retrying for up to 24 hours. It takes 7 retries
  // to reach the maxTimeout of 1 hour.
  retries: 30,
}

export function initSync<ExtendedEvents extends EventEmitter.ValidEventTypes>(
  redis: Redis,
  collection: Collection,
  options: SyncOptions = {}
) {
  /**
   * Redis keys used for the collection.
   */
  const keys = getKeys(collection, options)
  const { omit } = options
  const retryOptions: RetryOptions = {
    ...defaultRetryOptions,
    ...options.retry,
  }
  /** Event emitter */
  const emitter = new EventEmitter<Events | ExtendedEvents>()
  const emit = (event: Events, data: object) => {
    emitter.emit(event, { type: event, ...data })
  }
  const emitStateChange = (change: object) => emit('stateChange', change)
  const pause = pausable(options.maxPauseTime)
  const toChangeStreamInsert = docToChangeStreamInsert(collection)
  /**
   * Determine if the collection should be resynced by checking for the existence
   * of the resync key in Redis.
   */
  const detectResync = (resyncCheckInterval = ms('1m')) => {
    let resyncTimer: NodeJS.Timeout
    const state = fsm(simpleStateTransistions, 'stopped', {
      name: 'detectResync',
      onStateChange: emitStateChange,
    })

    const shouldResync = () => redis.exists(keys.resyncKey)

    const start = () => {
      debug('Starting resync check')
      if (state.is('started')) {
        return
      }
      // Check periodically if the collection should be resynced
      resyncTimer = setInterval(async () => {
        if (pause.isPaused) {
          debug('Skipping re-sync check - paused')
          return
        }
        if (await shouldResync()) {
          debug('Resync triggered')
          emit('resync', {})
        }
      }, resyncCheckInterval)
      state.change('started')
    }

    const stop = () => {
      debug('Stopping resync check')
      if (state.is('stopped')) {
        return
      }
      clearInterval(resyncTimer)
      state.change('stopped')
    }

    return { start, stop }
  }

  /**
   * Run initial collection scan. `options.batchSize` defaults to 500.
   * Sorting defaults to `_id`.
   *
   * Call `start` to start processing documents and `stop` to close
   * the cursor.
   */
  async function runInitialScan<T = any>(
    processRecords: ProcessInitialScanRecords,
    options: QueueOptions & ScanOptions<T> = {}
  ) {
    let deferred: Deferred
    const retryController = new AbortController()
    let cursor: ReturnType<typeof collection.aggregate>
    const state = fsm(stateTransitions, 'stopped', {
      name: 'runInitialScan',
      onStateChange: emitStateChange,
    })
    const defaultSortField: SortField<ObjectId> = {
      field: '_id',
      serialize: _.toString,
      deserialize: (x: string) => new ObjectId(x),
      order: 'asc',
    }

    const sortField = options.sortField || defaultSortField

    const sortOrderToNum = { asc: 1, desc: -1 }
    const getCursor = async () => {
      // Lookup last id successfully processed
      const lastIdProcessed = await redis.get(keys.lastScanIdKey)
      debug('Last id processed %s', lastIdProcessed)
      // Query collection
      const omitPipeline = omit ? [{ $project: setDefaults(omit, 0) }] : []
      const extendedPipeline = options.pipeline ?? []
      const pipeline = [
        // Skip ids already processed
        ...(lastIdProcessed
          ? [
              {
                $match: {
                  [sortField.field]: {
                    $gt: sortField.deserialize(lastIdProcessed),
                  },
                },
              },
            ]
          : []),
        {
          $sort: {
            [sortField.field]: sortOrderToNum[sortField.order ?? 'asc'],
          },
        },
        ...omitPipeline,
        ...extendedPipeline,
      ]
      debug('Initial scan pipeline %O', pipeline)
      return collection.aggregate(pipeline)
    }

    /** Start the initial scan */
    const start = async () => {
      debug('Starting initial scan')
      // Nothing to do
      if (state.is('starting', 'started')) {
        debug('Initial scan already starting or started')
        return
      }
      if (state.is('stopping')) {
        // Wait until stopping -> stopped
        await state.waitForChange('stopped')
      }
      state.change('starting')
      deferred = defer()
      // Determine if initial scan has already completed
      const scanCompleted = await redis.get(keys.scanCompletedKey)
      // Scan already completed so return
      if (scanCompleted) {
        debug(`Initial scan previously completed on %s`, scanCompleted)
        // We're done
        state.change('stopped')
        return
      }

      const _processRecords = async (records: ChangeStreamInsertDocument[]) => {
        const numRecords = records.length
        const lastDocument = records[numRecords - 1].fullDocument
        // Record last id of the batch
        const lastId = _.get(sortField.field, lastDocument)
        // Process batch of records with retries.
        // NOTE: processRecords could mutate records.
        try {
          await retry(() => processRecords(records), {
            ...retryOptions,
            signal: retryController.signal,
          })
          debug('Processed %d records', numRecords)
          debug('Last id %s', lastId)
          if (lastId) {
            await redis.mset(
              keys.lastScanIdKey,
              sortField.serialize(lastId),
              keys.lastScanProcessedAtKey,
              new Date().getTime()
            )
          }
        } catch (error) {
          debug('Process error %o', error)
          if (!state.is('stopping')) {
            emit('processError', { error, name: 'runInitialScan' })
          }
        }
        // Emit stats
        emit('stats', {
          name: 'runInitialScan',
          stats: queue.getStats(),
          lastFlush: queue.lastFlush,
        } as StatsEvent)
      }
      // Create queue
      const queue = batchQueue(_processRecords, options)
      // Query collection
      cursor = await getCursor()
      // Change state
      state.change('started')

      const nextChecker = safelyCheckNext(cursor)
      let doc: Document | null
      // Process documents
      while ((doc = await nextChecker.getNext())) {
        debug('Initial scan doc %O', doc)
        await queue.enqueue(toChangeStreamInsert(doc))
        await pause.maybeBlock()
      }
      // Flush the queue
      await queue.flush()
      // We are not stopping
      if (!state.is('stopping')) {
        // An error occurred getting next
        if (nextChecker.errorExists()) {
          emit('cursorError', {
            name: 'runInitialScan',
            error: nextChecker.getLastError(),
          } as CursorErrorEvent)
        }
        // Exited cleanly from the loop so we're done
        else {
          debug('Completed initial scan')
          // Record scan complete
          await redis.set(keys.scanCompletedKey, new Date().toString())
          // Emit event
          emit('initialScanComplete', {})
        }
      }
      // Resolve deferred
      deferred.done()
      debug('Exit initial scan')
    }

    /** Stop the initial scan */
    const stop = async () => {
      debug('Stopping initial scan')
      // Nothing to do
      if (state.is('stopping', 'stopped')) {
        debug('Initial scan already stopping or stopped')
        return
      }
      if (state.is('starting')) {
        // Wait until starting -> started
        await state.waitForChange('started')
      }
      state.change('stopping')
      // Close the cursor
      await cursor?.close()
      debug('MongoDB cursor closed')
      // Abort retries
      retryController.abort('stopping')
      debug('Retry controller aborted')
      // Wait for start fn to finish
      await deferred?.promise
      state.change('stopped')
      debug('Stopped initial scan')
    }

    /** Restart the initial scan */
    const restart = async () => {
      debug('Restarting initial scan')
      await stop()
      start()
    }

    return { start, stop, restart, state: _.pick(stateFields, state) }
  }

  /**
   * Process MongoDB change stream for the collection.
   * If omit is passed to `initSync` a pipeline stage that removes
   * those fields will be prepended to the `pipeline` argument.
   *
   * Call `start` to start processing events and `stop` to close
   * the change stream.
   */
  const processChangeStream = async (
    processRecords: ProcessChangeStreamRecords,
    options: QueueOptions & ChangeStreamOptions = {}
  ) => {
    let deferred: Deferred
    const retryController = new AbortController()
    let changeStream: ChangeStream
    const state = fsm(stateTransitions, 'stopped', {
      name: 'processChangeStream',
      onStateChange: emitStateChange,
    })
    const defaultOptions = { fullDocument: 'updateLookup' }
    const operationTypes = options.operationTypes
    debug('Operation types %o', operationTypes)

    /**
     * Get the change stream, resuming from a previous token if exists.
     */
    const getChangeStream = async () => {
      const operationsPipeline = operationTypes
        ? [{ $match: { operationType: { $in: operationTypes } } }]
        : []
      const omitPipeline = omit ? generatePipelineFromOmit(omit) : []
      const extendedPipeline = options.pipeline ?? []
      const pipeline = [
        ...operationsPipeline,
        ...omitPipeline,
        ...extendedPipeline,
      ]
      debug('Change stream pipeline %O', pipeline)
      // Lookup change stream token
      const token = await redis.get(keys.changeStreamTokenKey)
      debug('Last recorded resume token: %O', token)
      const changeStreamOptions: mongodb.ChangeStreamOptions = token
        ? // Resume token found, so set change stream resume point
          { ...defaultOptions, resumeAfter: JSON.parse(token) }
        : defaultOptions
      return collection.watch(pipeline, changeStreamOptions)
    }

    /** Start processing change stream */
    const start = async () => {
      debug('Starting change stream')
      // Nothing to do
      if (state.is('starting', 'started')) {
        debug('Change stream already starting or started')
        return
      }
      if (state.is('stopping')) {
        // Wait until stopping -> stopped
        await state.waitForChange('stopped')
      }
      state.change('starting')

      changeStream = await getChangeStream()
      debug('Started change stream')

      const _processRecords = async (
        maybeRecords: (ChangeStreamDocument | null)[]
      ) => {
        // Each item in `maybeRecords` is either an actual record, or null. Null
        // is a signal that we have reached the end of the change stream and we
        // just need to update the resume token in Redis.
        const records = maybeRecords.filter((record) => record != null)

        // Process batch of records with retries.
        // NOTE: processRecords could mutate records.
        try {
          if (records.length > 0) {
            await retry(() => processRecords(records), {
              ...retryOptions,
              signal: retryController.signal,
            })
            debug('Processed %d records', records.length)
          }

          // Persist state
          const token = changeStream.resumeToken
          debug('Updating resume token to: %s', token)
          await redis.mset(
            keys.changeStreamTokenKey,
            JSON.stringify(token),
            keys.lastChangeProcessedAtKey,
            new Date().getTime()
          )
        } catch (error) {
          debug('Process error %o', error)
          if (!state.is('stopping')) {
            emit('processError', { error, name: 'processChangeStream' })
          }
        }
        // Emit stats
        emit('stats', {
          name: 'processChangeStream',
          stats: queue.getStats(),
          lastFlush: queue.lastFlush,
        } as StatsEvent)
      }
      // New deferred
      deferred = defer()
      // Create queue
      const queue = batchQueue(_processRecords, {
        timeout: ms('30s'),
        ...options,
      })

      const nextChecker = safelyCheckNext(changeStream)
      state.change('started')

      // Consume change stream until there is an error or `stop` is called.
      while (!state.is('stopping', 'stopped')) {
        // This always updates `changeStream.resumeToken`, even when we reach
        // the end of the change stream and null is returned.
        //
        // This is important because we always want to keep a recent resume
        // token. Attempting to resume from an older resume token might not
        // work, because only a certain (configurable) amount of oplog history
        // is kept. Even if the oplog entry still exists, it can take a very
        // long time to scan through all of the oplog entries since then, so
        // keeping a recent resume token is vital for performance.
        const event: ChangeStreamDocument | null = await nextChecker.getNext()

        if (event) {
          debug('Change stream event %O', event)
          // Omit nested fields that are not handled by $unset.
          // For example, if 'a' was omitted then 'a.b.c' should be omitted.
          if (
            omit &&
            event.operationType === 'update' &&
            // Downstream libraries might unset event.updateDescription
            // to optimize performance (e.g., mongo2elastic).
            event.updateDescription
          ) {
            omitFieldsForUpdate(omit, event)
          }
        }

        await queue.enqueue(event)
        await pause.maybeBlock()

        if (nextChecker.errorExists() && !state.is('stopping')) {
          emit('cursorError', {
            name: 'processChangeStream',
            error: nextChecker.getLastError(),
          } as CursorErrorEvent)
          break
        }
      }

      await queue.flush()

      // Signal stopping => stopped state change
      deferred.done()
      debug('Exit change stream')
    }

    /** Stop processing change stream */
    const stop = async () => {
      debug('Stopping change stream')
      // Nothing to do
      if (state.is('stopping', 'stopped')) {
        debug('Change stream already stopping or stopped')
        return
      }
      if (state.is('starting')) {
        // Wait until starting -> started
        await state.waitForChange('started')
      }
      state.change('stopping')
      // Close the change stream
      await changeStream?.close()
      debug('MongoDB change stream closed')
      // Abort retries
      retryController.abort('stopping')
      debug('Retry controller aborted')
      // Wait for start fn to finish
      await deferred?.promise
      state.change('stopped')
      debug('Stopped change stream')
    }

    /** Restart change stream */
    const restart = async () => {
      debug('Restarting change stream')
      await stop()
      start()
    }

    return { start, stop, restart, state: _.pick(stateFields, state) }
  }

  /**
   * Delete all Redis keys for the collection.
   */
  const reset = async () => {
    debug('Reset')
    await redis.del(...Object.values(keys))
  }

  /**
   * Get the existing JSON schema for the collection.
   */
  const getCollectionSchema = async (db: Db): Promise<JSONSchema> => {
    const colls = await db
      .listCollections({ name: collection.collectionName })
      .toArray()
    return _.get('0.options.validator.$jsonSchema', colls) || {}
  }

  /**
   * Get cached collection schema
   */
  const getCachedCollectionSchema = () =>
    redis.get(keys.schemaKey).then((val: any) => val && JSON.parse(val))

  /**
   * Check for schema changes every interval and emit 'change' event if found.
   * Optionally, set interval and strip unused fields (e.g., title and description)
   * from the JSON schema.
   *
   * Call `start` to start polling for schema changes and `stop` to clear
   * the timer.
   */
  const detectSchemaChange = async (db: Db, options: ChangeOptions = {}) => {
    const interval = options.interval || ms('1m')
    const shouldRemoveUnusedFields = options.shouldRemoveUnusedFields
    const maybeRemoveUnusedFields = when(
      shouldRemoveUnusedFields,
      removeUnusedFields
    )
    const state = fsm(simpleStateTransistions, 'stopped', {
      name: 'detectSchemaChange',
      onStateChange: emitStateChange,
    })

    let timer: NodeJS.Timeout
    // Check for a cached schema
    let previousSchema = await getCachedCollectionSchema().then((schema) => {
      if (schema) {
        return maybeRemoveUnusedFields(schema)
      }
    })
    if (!previousSchema) {
      const schema = await getCollectionSchema(db).then(maybeRemoveUnusedFields)
      // Persist schema
      await redis.setnx(keys.schemaKey, JSON.stringify(schema))
      previousSchema = schema
    }
    debug('Previous schema %O', previousSchema)
    // Check for a schema change
    const checkForSchemaChange = async () => {
      if (pause.isPaused) {
        debug('Skipping schema change check - paused')
        return
      }

      const currentSchema = await getCollectionSchema(db).then(
        maybeRemoveUnusedFields
      )
      // Schemas are no longer the same
      if (!_.isEqual(currentSchema, previousSchema)) {
        debug('Schema change detected %O', currentSchema)
        // Persist schema
        await redis.set(keys.schemaKey, JSON.stringify(currentSchema))
        // Emit change
        emit('schemaChange', {
          previousSchema,
          currentSchema,
        } as SchemaChangeEvent)
        // Previous schema is now the current schema
        previousSchema = currentSchema
      }
    }
    const start = () => {
      debug('Starting polling for schema changes')
      if (state.is('started')) {
        return
      }
      // Perform an inital check
      checkForSchemaChange()
      // Check for schema changes every interval
      timer = setInterval(checkForSchemaChange, interval)
      state.change('started')
    }
    const stop = () => {
      debug('Stopping polling for schema changes')
      if (state.is('stopped')) {
        return
      }
      clearInterval(timer)
      state.change('stopped')
    }
    return { start, stop }
  }

  return {
    runInitialScan,
    processChangeStream,
    reset,
    getCollectionSchema,
    detectSchemaChange,
    detectResync,
    keys,
    emitter,
    /** Pause and resume all syncing functions at once. */
    pausable: pause,
  }
}
