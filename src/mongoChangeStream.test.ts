import { Redis } from 'ioredis'
import _ from 'lodash/fp.js'
import { genUser, initState, numDocs, schema } from 'mongochangestream-testing'
import {
  type ChangeStreamDocument,
  type ChangeStreamInsertDocument,
  type Document,
  MongoClient,
  ObjectId,
} from 'mongodb'
import ms from 'ms'
import assert from 'node:assert'
import { setTimeout } from 'node:timers/promises'
import {
  type LastFlush,
  type QueueOptions,
  type QueueStats,
  TimeoutError,
  waitUntil,
} from 'prom-utils'
import { describe, test } from 'vitest'

import { getKeys, initSync } from './mongoChangeStream.js'
import type {
  CursorErrorEvent,
  ScanOptions,
  SchemaChangeEvent,
  SortField,
  StatsEvent,
  SyncOptions,
} from './types.js'
import { missingOplogEntry } from './util.js'

const getConns = _.memoize(async (x?: any) => {
  // Memoize hack
  console.log(x)
  const redis = new Redis({ keyPrefix: 'testing:' })
  const client = await MongoClient.connect(process.env.MONGO_CONN as string)
  const db = client.db()
  const coll = db.collection('testing')
  return { client, db, coll, redis }
})

const getSync = async (options?: SyncOptions) => {
  const { redis, coll } = await getConns()
  const sync = initSync(redis, coll, options)
  sync.emitter.on('stateChange', console.log)
  return sync
}

interface AssertEventuallyOptions {
  /** The maximum time to wait for the predicate to return true. */
  timeout?: number
  /** The time to wait between checks. */
  interval?: number
  /** The message to use if the assertion fails after the timeout. */
  message?: string
}

/**
 * Asserts that the provided predicate eventually returns true.
 *
 * @param pred - The predicate to check: an async function returning a boolean.
 * @param options - @see AssertEventuallyOptions
 *
 * @throws AssertionError if the predicate does not return true before the
 * timeout.
 */
const assertEventually = async (
  pred: () => Promise<boolean>,
  options: AssertEventuallyOptions = {}
) => {
  const { timeout, interval, message } = options

  try {
    await waitUntil(pred, {
      timeout: timeout ?? ms('20s'),
      checkFrequency: interval ?? ms('50ms'),
    })
  } catch (e) {
    if (e instanceof TimeoutError) {
      assert.fail(message)
    } else {
      throw e
    }
  }
}

describe.sequential('syncing', () => {
  // NOTE: This test is flaky. Having it run first seems to help :)
  test('stopping change stream is idempotent', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    // Change stream
    const processRecords = async () => {
      await setTimeout(5)
    }
    const changeStream = await sync.processChangeStream(processRecords)
    changeStream.start()
    // Change documents
    await coll.updateMany({}, { $set: { createdAt: new Date('2022-01-03') } })
    // Stop twice
    await changeStream.stop()
    await changeStream.stop()
  })

  test('starting change stream is idempotent', async () => {
    const sync = await getSync()
    // Change stream
    const processRecords = async () => {
      await setTimeout(5)
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start twice
    changeStream.start()
    changeStream.start()
    await setTimeout(500)
    await changeStream.stop()
  })

  test('starting initial scan is idempotent', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processRecords = async () => {
      await setTimeout(50)
    }
    const initialScan = await sync.runInitialScan(processRecords)
    // Start twice
    initialScan.start()
    initialScan.start()
    await setTimeout(500)
    await initialScan.stop()
  })

  test('stopping initial scan is idempotent', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processRecords = async () => {
      await setTimeout(50)
    }
    const initialScan = await sync.runInitialScan(processRecords)
    initialScan.start()
    await setTimeout(1000)
    // Stop twice
    await initialScan.stop()
    await initialScan.stop()
  })

  test('should complete initial scan', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      for (const doc of docs) {
        // `processRecords` can mutate the documents in arbitrary ways,
        // including deleting the `_id`, which would cause issues with
        // continuing from where we last left off.
        //
        // This simulates that scenario, in order to test that the library
        // handles it appropriately.
        delete doc._id
        processed.push(doc)
      }
    }
    const scanOptions = { batchSize: 100 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(processed.length, numDocs)
    // Check that Redis keys are set
    assert.ok(await redis.get(sync.keys.lastScanIdKey))
    assert.ok(await redis.get(sync.keys.lastScanProcessedAtKey))
    assert.ok(await redis.get(sync.keys.scanCompletedKey))
    // Stop
    await initialScan.stop()
  })

  test('should emit processError after exhausting retries - initial scan', async () => {
    const { coll, db } = await getConns()
    const options: SyncOptions = {
      retry: {
        retries: 1,
        minTimeout: 100,
      },
    }
    const sync = await getSync(options)
    await initState(sync, db, coll)

    let processErrorCount = 0
    sync.emitter.on('processError', () => {
      processErrorCount++
    })
    const processRecords = async () => {
      await setTimeout(50)
      // Simulate a failure
      throw new Error('Fail')
    }
    const initialScan = await sync.runInitialScan(processRecords)
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(processErrorCount, 1)
    // Stop
    await initialScan.stop()
  })

  test('should abort retries if stopping and not emit error - initial scan', async () => {
    const { coll, db } = await getConns()
    const options: SyncOptions = {
      retry: {
        retries: 2,
        minTimeout: ms('1s'),
      },
    }
    const sync = await getSync(options)
    await initState(sync, db, coll)

    let processError = false
    sync.emitter.on('processError', () => {
      processError = true
    })

    let processRecordsCount = 0
    const processRecords = async () => {
      processRecordsCount++
      await setTimeout(50)
      // Simulate a failure
      throw new Error('Fail')
    }
    const initialScan = await sync.runInitialScan(processRecords)
    // Start initial scan
    initialScan.start()
    await setTimeout(ms('2s'))
    // Stop
    await initialScan.stop()
    console.log('processRecordsCount %d', processRecordsCount)
    assert.ok(processRecordsCount < 3)
    assert.strictEqual(processError, false)
  })

  test('should retry initial scan', async () => {
    const { coll, db } = await getConns()
    const options: SyncOptions = {
      retry: {
        minTimeout: 100,
      },
    }
    const sync = await getSync(options)
    await initState(sync, db, coll)

    let counter = 0
    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      // Simulate a failure on the first try
      if (counter++ === 0) {
        throw new Error('Fail')
      }
      processed.push(...docs)
    }
    const batchSize = 100
    const initialScan = await sync.runInitialScan(processRecords, { batchSize })
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(processed.length, numDocs)
    // Expect one extra call to processRecords
    assert.strictEqual(counter, numDocs / batchSize + 1)
    // Stop
    await initialScan.stop()
  })

  test('should pause initial scan', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      processed.push(...docs)

      if (processed.length === 200) {
        sync.pausable.pause()
        // Call resume after 1s
        global.setTimeout(sync.pausable.resume, ms('1s'))
      }
    }
    const scanOptions = { batchSize: 100 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(processed.length, numDocs)
    // Stop
    await initialScan.stop()
  })

  test('initial scan should throttle', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    let stats: QueueStats = { itemsPerSec: 0, bytesPerSec: 0 }
    let lastFlush: LastFlush | undefined
    sync.emitter.on('stats', (event: StatsEvent) => {
      stats = event.stats
      lastFlush = event.lastFlush
    })

    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      processed.push(...docs)
    }
    const initialScan = await sync.runInitialScan(processRecords, {
      batchSize: 100,
      maxItemsPerSec: 200,
      maxBytesPerSec: 25000,
    })
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(processed.length, numDocs)
    assert.ok(stats.itemsPerSec > 0 && stats.itemsPerSec < 250)
    assert.ok(stats.bytesPerSec > 0 && stats.bytesPerSec < 40000)
    assert.deepEqual(lastFlush, { batchSize: 100 })
    // Stop
    await initialScan.stop()
  })

  test('should allow parallel syncing via uniqueId option', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    const sync2 = await getSync({ uniqueId: 'v2' })
    await initState(sync, db, coll)
    // Clear syncing state
    await sync2.reset()

    const processed: { v1: any[]; v2: any[] } = { v1: [], v2: [] }
    const processRecords =
      (version: 'v1' | 'v2') => async (docs: ChangeStreamInsertDocument[]) => {
        await setTimeout(50)
        processed[version].push(...docs)
      }
    const scanOptions = { batchSize: 100 }
    const initialScan = await sync.runInitialScan(
      processRecords('v1'),
      scanOptions
    )
    const initialScan2 = await sync2.runInitialScan(
      processRecords('v2'),
      scanOptions
    )
    // Wait for initial scan to complete
    await Promise.all([initialScan.start(), initialScan2.start()])
    // Assertions
    assert.strictEqual(processed.v1.length, numDocs)
    assert.strictEqual(processed.v2.length, numDocs)
    // Stop
    await Promise.all([initialScan.stop(), initialScan2.stop()])
  })

  test('should exit cleanly if initial scan is already complete', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)
    // Mark initial scan complete
    await redis.set(sync.keys.scanCompletedKey, new Date().toString())

    const processRecords = async () => {
      await setTimeout(50)
    }
    const scanOptions = { batchSize: 100 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    await initialScan.start()
    await initialScan.stop()
  })

  test('should run initial scan in reverse sort order', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      processed.push(...docs)
    }
    const sortField: SortField<ObjectId> = {
      field: '_id',
      serialize: _.toString,
      deserialize: (x: string) => new ObjectId(x),
      order: 'desc',
    }
    const scanOptions = { batchSize: 100, sortField }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(processed.length, numDocs)
    // Stop
    await initialScan.stop()
  })

  test('should omit fields from initial scan', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync({ omit: ['address.city', 'address.geo'] })
    await initState(sync, db, coll)

    const documents: Document[] = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      documents.push(docs[0].fullDocument)
    }
    const scanOptions = { batchSize: 100 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Wait for initial scan to complete
    await initialScan.start()
    assert.strictEqual(documents[0]?.address?.city, undefined)
    assert.strictEqual(documents[0]?.address?.geo, undefined)
    // Stop
    await initialScan.stop()
  })

  test('should complete initial scan if collection is empty', async () => {
    const { coll } = await getConns()
    const sync = await getSync()

    // Reset state
    await sync.reset()
    await coll.deleteMany({})

    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      processed.push(...docs)
    }
    let completed = false
    sync.emitter.on('initialScanComplete', () => {
      completed = true
    })
    const scanOptions = { batchSize: 100 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Wait for initial scan to complete
    await initialScan.start()
    assert.ok(completed)
    assert.strictEqual(processed.length, 0)
    // Stop
    await initialScan.stop()
  })

  test('initial scan should resume after stop', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processed: unknown[] = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      processed.push(...docs)
    }
    const scanOptions = { batchSize: 25 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    let completed = false
    sync.emitter.on('initialScanComplete', () => {
      completed = true
    })
    let cursorError = false
    sync.emitter.on('cursorError', () => {
      cursorError = true
    })
    // Start
    initialScan.start()
    // Allow for some records to be processed
    await setTimeout(200)
    // Stop the initial scan
    await initialScan.stop()
    // Only a subset of the documents were processed
    assert.ok(processed.length < numDocs)
    // Should not emit cursorError when stopping
    assert.strictEqual(cursorError, false)
    // Wait for the initial scan to complete
    await initialScan.start()
    assert.ok(completed)
    assert.strictEqual(processed.length, numDocs)
    // Stop
    await initialScan.stop()
  })

  test('initial scan should not be marked as completed if connection is closed', async () => {
    // Get a new connection since we're closing the connection in the test
    const { coll, redis, db, client } = await getConns({})
    const sync = initSync(redis, coll)
    let cursorErrorEmitted = false
    sync.emitter.on('stateChange', console.log)
    sync.emitter.on('cursorError', (e: unknown) => {
      cursorErrorEmitted = true
      console.log(e)
    })
    await initState(sync, db, coll)

    const processed = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(10)
      processed.push(...docs)
    }
    const scanOptions = { batchSize: 50 }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Start
    initialScan.start()
    // Allow for some records to be processed
    await setTimeout(100)
    // Close the connection.
    await client.close()
    // Allow for some time
    await setTimeout(100)
    // Check if completed
    const completedAt = await redis.get(sync.keys.scanCompletedKey)
    assert.strictEqual(completedAt, null)
    assert.ok(cursorErrorEmitted)
    // Stop
    await initialScan.stop()
  })

  test('initial scan should support custom pipeline', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    sync.emitter.on('stateChange', console.log)

    const documents: Document[] = []
    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(10)
      documents.push(docs[0].fullDocument)
    }
    const scanOptions: QueueOptions & ScanOptions = {
      batchSize: 50,
      pipeline: [
        {
          $addFields: {
            cityState: { $concat: ['$address.city', '-', '$address.state'] },
          },
        },
      ],
    }
    const initialScan = await sync.runInitialScan(processRecords, scanOptions)
    // Start
    initialScan.start()
    // Allow for some records to be processed
    await setTimeout(500)
    // Stop
    await initialScan.stop()
    assert.ok(documents[0].cityState)
  })

  test('should process records via change stream', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    let cursorError = false
    sync.emitter.on('cursorError', () => {
      cursorError = true
    })
    const processed: any[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        // Simulate downstream mutation
        delete doc._id
        processed.push(doc)
      }
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
    // Wait for the change stream events to be processed
    await setTimeout(ms('6s'))
    assert.strictEqual(processed.length, numDocs)
    // Check that Redis keys are set
    assert.ok(await redis.get(sync.keys.changeStreamTokenKey))
    assert.ok(await redis.get(sync.keys.lastChangeProcessedAtKey))
    // Stop
    await changeStream.stop()
    // Should not emit cursorError when stopping
    assert.strictEqual(cursorError, false)
  })

  test('should retry processing change stream records', async () => {
    const { coll, db } = await getConns()
    const options: SyncOptions = {
      retry: {
        minTimeout: 100,
      },
    }
    const sync = await getSync(options)
    await initState(sync, db, coll)

    let counter = 0
    const processed: any[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      // Simulate a failure on the first try
      if (counter++ === 0) {
        throw new Error('Fail')
      }
      for (const doc of docs) {
        await setTimeout(5)
        processed.push(doc)
      }
    }
    const batchSize = 100
    const changeStream = await sync.processChangeStream(processRecords, {
      batchSize,
    })
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
    // Wait for the change stream events to be processed
    await setTimeout(ms('6s'))
    assert.strictEqual(processed.length, numDocs)
    // Expect one extra call to processRecords
    assert.strictEqual(counter, numDocs / batchSize + 1)
    // Stop
    await changeStream.stop()
  })

  test('should emit error after exhausting retries - change stream', async () => {
    const { coll, db } = await getConns()
    const options: SyncOptions = {
      retry: {
        retries: 1,
        minTimeout: 100,
      },
    }
    const sync = await getSync(options)
    await initState(sync, db, coll)

    let processErrorCount = 0
    sync.emitter.on('processError', () => {
      processErrorCount++
    })
    const processRecords = async () => {
      // Simulate a failure
      throw new Error('Fail')
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
    // Wait for the change stream events to be processed
    await setTimeout(ms('6s'))
    assert.strictEqual(processErrorCount, 1)
    // Stop
    await changeStream.stop()
  })

  test('should abort retries if stopping and not emit error - change stream', async () => {
    const { coll, db } = await getConns()
    const options: SyncOptions = {
      retry: {
        retries: 2,
        minTimeout: ms('1s'),
      },
    }
    const sync = await getSync(options)
    await initState(sync, db, coll)

    let processError = false
    sync.emitter.on('processError', () => {
      processError = true
    })
    let processRecordsCount = 0
    const processRecords = async () => {
      processRecordsCount++
      await setTimeout(50)
      // Simulate a failure
      throw new Error('Fail')
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
    // Wait for some of the change stream events to be processed
    await setTimeout(ms('2s'))
    // Stop
    await changeStream.stop()
    console.log('processRecordsCount %d', processRecordsCount)
    assert.ok(processRecordsCount < 3)
    assert.strictEqual(processError, false)
  })

  test('change stream should resume after pause in events', async () => {
    const { coll, db } = await getConns()
    // Use maxPauseTime option to auto-resume
    const sync = await getSync({ maxPauseTime: ms('1s') })
    await initState(sync, db, coll)

    let cursorError = false
    sync.emitter.on('cursorError', () => {
      cursorError = true
    })
    const processed: any[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        processed.push(doc)
        if (processed.length === 200) {
          sync.pausable.pause()
        }
      }
    }
    const changeStream = await sync.processChangeStream(processRecords, {
      batchSize: 100,
    })
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
    // Wait for the change stream events to be processed
    await setTimeout(ms('6s'))
    assert.strictEqual(processed.length, numDocs)
    // Stop
    await changeStream.stop()
    // Should not emit cursorError when stopping
    assert.strictEqual(cursorError, false)
  })

  test('change stream should throttle', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    let stats: QueueStats = { itemsPerSec: 0, bytesPerSec: 0 }
    let lastFlush: LastFlush | undefined
    sync.emitter.on('stats', (event: StatsEvent) => {
      stats = event.stats
      lastFlush = event.lastFlush
    })

    const processed: any[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        processed.push(doc)
      }
    }
    const changeStream = await sync.processChangeStream(processRecords, {
      batchSize: 100,
      maxItemsPerSec: 100,
      maxBytesPerSec: 65000,
    })
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany(
      {},
      {
        $set: { createdAt: new Date('2022-01-01') },
        $unset: { 'address.city': '', 'address.geo.lat': '' },
        $pop: { likes: 1 },
      }
    )
    // Wait for the change stream events to be processed
    await setTimeout(ms('8s'))
    assert.strictEqual(processed.length, numDocs)
    assert.ok(stats.itemsPerSec > 0 && stats.itemsPerSec < 200)
    assert.ok(stats.bytesPerSec > 0 && stats.bytesPerSec < 80000)
    assert.deepEqual(lastFlush, { batchSize: 100 })
    // Stop
    await changeStream.stop()
  })

  test('should process records via change stream - updateDescription removed', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processed: any[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        processed.push(doc)
      }
    }
    const changeStream = await sync.processChangeStream(processRecords, {
      pipeline: [{ $unset: ['updateDescription'] }],
    })
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-01') } })
    // Wait for the change stream events to be processed
    await setTimeout(ms('6s'))
    assert.strictEqual(processed.length, numDocs)
    // Stop
    await changeStream.stop()
  })

  test('should omit fields from change stream - dotted paths', async () => {
    const { coll, db } = await getConns()
    // address.geo is a path prefix relative to the paths being updated below
    const sync = await getSync({ omit: ['address.city', 'address.geo'] })
    await initState(sync, db, coll)

    const documents: Document[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        if (doc.operationType === 'update' && doc.fullDocument) {
          documents.push(doc)
        }
      }
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany(
      {},
      {
        $set: {
          name: 'unknown',
          'address.city': 'San Diego',
          'address.geo.lat': 24,
        },
        $unset: {
          'address.geo.long': '',
        },
      }
    )
    // Wait for the change stream events to be processed
    await setTimeout(ms('2s'))
    // Assertions
    assert.strictEqual(documents[0].fullDocument.address.city, undefined)
    assert.strictEqual(documents[0].fullDocument.address.geo, undefined)
    const fields = ['address.city', 'address.geo.lat']
    for (const field of fields) {
      assert.strictEqual(
        documents[0].updateDescription.updatedFields[field],
        undefined
      )
    }
    assert.deepEqual(documents[0].updateDescription.removedFields, [])
    // Stop
    await changeStream.stop()
  })

  test('should omit fields from change stream - nested dotted path', async () => {
    const { coll, db } = await getConns()
    // address.geo is a path prefix relative to the paths being updated below
    const sync = await getSync({ omit: ['address.geo.lat'] })
    await initState(sync, db, coll)

    const documents: Document[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        if (doc.operationType === 'update' && doc.fullDocument) {
          documents.push(doc)
        }
      }
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update record
    coll.updateMany({}, [
      {
        $set: {
          'address.geo.lat': 24,
          'address.geo.long': 25,
        },
      },
    ])
    // Wait for the change stream events to be processed
    await setTimeout(ms('2s'))
    // Assertions
    assert.strictEqual(documents[0].fullDocument.address.geo.long, 25)
    assert.strictEqual(documents[0].fullDocument.address.geo.lat, undefined)
    assert.strictEqual(
      documents[0].updateDescription.updatedFields['address.geo'].long,
      25
    )
    assert.strictEqual(
      documents[0].updateDescription.updatedFields['address.geo'].lat,
      undefined
    )
    // Stop
    await changeStream.stop()
  })

  test('should omit fields from change stream - object', async () => {
    const { coll, db } = await getConns()
    // address.geo is a path prefix relative to the paths being updated below
    const sync = await getSync({ omit: ['address'] })
    await initState(sync, db, coll)

    const documents: Document[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        if (doc.operationType === 'update' && doc.fullDocument) {
          documents.push(doc)
        }
      }
    }
    const changeStream = await sync.processChangeStream(processRecords)
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    coll.updateMany(
      {},
      {
        $set: {
          address: { city: 'San Diego' },
        },
      }
    )
    // Wait for the change stream events to be processed
    await setTimeout(ms('2s'))
    // Assertions
    assert.strictEqual(documents[0].fullDocument.address, undefined)
    assert.strictEqual(
      documents[0].updateDescription.updatedFields.address,
      undefined
    )
    // Stop
    await changeStream.stop()
  })

  test('should omit operation types from change stream', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const operations: string[] = []
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(5)
        operations.push(doc.operationType)
      }
    }
    const changeStream = await sync.processChangeStream(processRecords, {
      operationTypes: ['insert'],
      // Short timeout since only event will be queued
      timeout: 500,
    })
    // Start
    changeStream.start()
    await setTimeout(ms('1s'))
    // Update records
    await coll.updateMany({}, { $set: { name: 'unknown' } })
    // Insert record
    await coll.insertOne(genUser())
    // Wait for the change stream events to be processed
    await setTimeout(ms('2s'))
    assert.deepEqual(_.uniq(operations), ['insert'])
    // Stop
    await changeStream.stop()
  })

  test('change stream should resume after being stopped', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    const processed = []
    // Change stream
    const processRecords = async (docs: ChangeStreamDocument[]) => {
      for (const doc of docs) {
        await setTimeout(8)
        processed.push(doc)
      }
    }
    const changeStream = await sync.processChangeStream(processRecords)
    changeStream.start()
    // Let change stream connect
    await setTimeout(ms('1s'))
    // Change documents
    coll.updateMany({}, { $set: { createdAt: new Date('2022-01-02') } })
    // Wait for some change stream events to be processed
    await setTimeout(ms('2s'))
    // Only a subset of the documents were processed
    assert.ok(processed.length < numDocs)
    // Stop
    await changeStream.stop()
    // Resume change stream
    changeStream.start()
    // Wait for all documents to be processed
    await setTimeout(ms('5s'))
    // All change stream docs were processed
    assert.strictEqual(processed.length, numDocs)
    await changeStream.stop()
  })

  test('change stream handle missing oplog entry properly', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    let cursorError: any
    sync.emitter.on('cursorError', (event: CursorErrorEvent) => {
      cursorError = event
    })

    await initState(sync, db, coll)

    // Set missing token key
    await redis.set(
      sync.keys.changeStreamTokenKey,
      '{"_data":"8263F51B8F000000012B022C0100296E5A1004F852F6C89F924F0A8711460F0C1FBD8846645F6964006463F51B8FD1AACE003022EFC80004"}'
    )

    // Change stream
    const processRecords = async () => {
      await setTimeout(5)
    }
    const changeStream = await sync.processChangeStream(processRecords)
    changeStream.start()
    // Let change stream connect
    await setTimeout(ms('1s'))

    assert.ok(missingOplogEntry(cursorError.error))
    await changeStream.stop()
  })

  test('change stream handle invalid oplog entry properly', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    let cursorError: any
    sync.emitter.on('cursorError', (event: CursorErrorEvent) => {
      cursorError = event
    })

    await initState(sync, db, coll)

    // Set missing token key
    await redis.set(sync.keys.changeStreamTokenKey, '{"_data":"123"}')

    // Change stream
    const processRecord = async () => {
      await setTimeout(5)
    }
    const changeStream = await sync.processChangeStream(processRecord)
    changeStream.start()
    // Let change stream connect
    await setTimeout(ms('1s'))

    assert.ok(missingOplogEntry(cursorError.error))
    await changeStream.stop()
  })

  test('change stream should handle empty collection', async () => {
    const { coll, db } = await getConns()
    const sync = await getSync()
    let cursorError = false
    sync.emitter.on('cursorError', () => {
      cursorError = true
    })

    await initState(sync, db, coll)
    // Delete all documents
    await coll.deleteMany({})

    // Change stream
    const processRecords = async () => {
      await setTimeout(5)
    }
    const changeStream = await sync.processChangeStream(processRecords)
    changeStream.start()
    // Let change stream connect
    await setTimeout(ms('1s'))

    assert.strictEqual(cursorError, false)
    await changeStream.stop()
  })

  test('Should resync when resync flag is set', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    let resyncTriggered = false
    const processed = []

    const processRecords = async (docs: ChangeStreamInsertDocument[]) => {
      await setTimeout(50)
      processed.push(...docs)
    }

    const initialScan = await sync.runInitialScan(processRecords)
    const resync = sync.detectResync(250)
    sync.emitter.on('resync', async () => {
      // Stop checking for resync
      resync.stop()
      resyncTriggered = true
      // Stop the initial scan
      await initialScan.stop()
      // Reset keys
      await sync.reset()
      // Reset processed
      processed.length = 0
      // Start initial scan
      initialScan.start()
    })
    // Start initial scan
    initialScan.start()
    // Start resync detection
    resync.start()
    // Allow for initial scan to start
    await setTimeout(500)
    // Trigger resync
    await redis.set(sync.keys.resyncKey, 1)

    // Wait for initial scan to complete
    await setTimeout(ms('5s'))
    assert.ok(resyncTriggered)
    assert.strictEqual(processed.length, numDocs)
    await initialScan.stop()
  })

  test('Resync is pausable', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)

    let resyncTriggered = false
    const resync = sync.detectResync(250)
    sync.emitter.on('resync', async () => {
      resyncTriggered = true
    })
    // Start resync detection
    resync.start()
    // Pause
    sync.pausable.pause()
    // Trigger resync
    await redis.set(sync.keys.resyncKey, 1)
    await setTimeout(ms('1s'))
    assert.ok(!resyncTriggered)
  })

  test('Resync start/stop is idempotent', async () => {
    const sync = await getSync()

    const resync = sync.detectResync()
    resync.start()
    resync.start()
    resync.stop()
    resync.stop()
  })

  test('Detect schema change', async () => {
    const { db, coll } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)
    // Look for a new schema every 250 ms
    const schemaChange = await sync.detectSchemaChange(db, {
      shouldRemoveUnusedFields: true,
      interval: 250,
    })
    let schemaChangeEventTriggered = false
    sync.emitter.on('schemaChange', ({ currentSchema }: SchemaChangeEvent) => {
      console.dir(currentSchema, { depth: 10 })
      schemaChangeEventTriggered = true
    })
    // Start detecting schema changes
    schemaChange.start()
    // Modify the schema
    const modifiedSchema = _.set(
      'properties.email',
      { bsonType: 'string' },
      schema
    )
    await db.command({
      collMod: coll.collectionName,
      validator: { $jsonSchema: modifiedSchema },
    })
    await setTimeout(ms('1s'))
    assert.ok(schemaChangeEventTriggered)
    schemaChange.stop()
  })

  test('Detect schema change is pausable', async () => {
    const { db, coll } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)
    // Look for a new schema every 250 ms
    const schemaChange = await sync.detectSchemaChange(db, {
      shouldRemoveUnusedFields: true,
      interval: 250,
    })
    let schemaChangeEventTriggered = false
    sync.emitter.on('schemaChange', () => {
      schemaChangeEventTriggered = true
    })
    // Start detecting schema changes
    schemaChange.start()
    // Pause
    sync.pausable.pause()
    // Modify the schema
    const modifiedSchema = _.set(
      'properties.email',
      { bsonType: 'string' },
      schema
    )
    await db.command({
      collMod: coll.collectionName,
      validator: { $jsonSchema: modifiedSchema },
    })
    await setTimeout(ms('1s'))
    assert.ok(!schemaChangeEventTriggered)
    schemaChange.stop()
  })

  test('Schema change start/stop is idempotent', async () => {
    const { db } = await getConns()
    const sync = await getSync()

    const schemaChange = await sync.detectSchemaChange(db)
    schemaChange.start()
    schemaChange.start()
    schemaChange.stop()
    schemaChange.stop()
  })

  test('can extend events', async () => {
    const { coll, redis } = await getConns({})
    const sync = initSync<'foo' | 'bar'>(redis, coll)
    let emitted = ''
    sync.emitter.on('foo', (x: string) => {
      emitted = x
    })
    sync.emitter.emit('foo', 'bar')
    assert.strictEqual(emitted, 'bar')
  })

  test('should update the resume token, even if there were no records updated', async () => {
    const { coll, db, redis } = await getConns()
    const sync = await getSync()
    await initState(sync, db, coll)
    const processRecords = async () => {
      await setTimeout(5)
    }

    // Utilities for comparing resume tokens to see if they change over time.
    const { changeStreamTokenKey } = getKeys(coll, {})
    const getCurrentToken = async () => await redis.get(changeStreamTokenKey)
    const assertResumeTokenUpdated = async (
      lastToken: string | null,
      when: string
    ): Promise<string | null> => {
      let currentToken: string | null = null

      await assertEventually(
        async () => {
          currentToken = await getCurrentToken()
          return currentToken !== lastToken
        },
        { message: `Resume token was not updated ${when}` }
      )

      return currentToken
    }

    // The resume token should initially be null, since we ran `initState`
    // above.
    let token = await getCurrentToken()
    assert.equal(token, null)

    const changeStream = await sync.processChangeStream(processRecords, {
      timeout: ms('5s'),
    })
    changeStream.start()
    // Let change stream connect
    await setTimeout(ms('1s'))

    // Waiting for a while should result in an updated resume token.
    token = await assertResumeTokenUpdated(token, 'after waiting')

    // Updating records results in an updated resume token.
    await coll.updateMany({}, { $set: { createdAt: new Date('2022-01-03') } })
    token = await assertResumeTokenUpdated(token, 'after updating records')

    // Waiting for a while after an update should result in the resume token
    // updating again.
    token = await assertResumeTokenUpdated(
      token,
      'after waiting again after an update'
    )

    await changeStream.stop()
  })
})
