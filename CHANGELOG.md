# 0.59.0

- The resume token stored in Redis is now updated regularly, even if the
  collection has not been updated in a long time. This enables mongochangestream
  to work in cases where older entries in the oplog (including the previously
  stored, old resume token) have been truncated, and generally improves
  performance because there are less oplog records to scan through.

- Removed `hasNext`. It turns out that it blocks forever once you reach the end
  of the change stream. It's better to use `getNext` and check whether the
  result is null. When the end of the change stream is reached, `getNext` will
  block for a minute or so and then return null.

# 0.58.0

- Wrap retry in try/catch and emit `processError` on exception.

# 0.57.0

- Added `renameKeys` utility function used in downstream libraries.
- Bumped packages.

# 0.56.0

- Add retry logic to `processRecords`. Downstream libraries should only throw if
  the error is transient and can be retried with eventual success.
- Changed to ES module.

# 0.55.0

- Bug Fix: `processRecords` could mutate so we need to derive values for state
  tracking prior to calling the function.

# 0.54.0

- Bump packages, including latest `prom-utils` which adds `lastFlush` to `batchQueue`.
- Added `lastFlush` to `stats` event.

# 0.53.0

- Add `documentKey` to pseudo `ChangeStreamInsertDocument` event for initial scan.

# 0.52.0

- Use `$match` to filter `operationType`.

# 0.51.0

- Added `pausable` to `pause` and `resume` all syncing operations.
- Removed deprecated `shouldRemoveMetadata` option.

# 0.50.0

- Bump packages, including latest `prom-utils` which allows for throttling of items/sec and bytes/sec.
- New event: `stats` which emits stats for items/sec and bytes/sec when a batch of
  records is processed for the initial scan or change stream.

# 0.49.0

- Don't assume that `updateDescription` will be on the change stream event.

# 0.48.0

- Fixed bug where `processChangeStream` exits prematurely.
- Fixed bug when omitting an updated nested field.
- Omit fields from `updateDescription.removedFields` to prevent downstream issues.

# 0.47.0

- Bumped peer dependencies for `mongodb`.
- Reworked `safelyCheckNext`.

# 0.46.0

- Bumped peer dependencies for `ioredis` and `mongodb`.

# 0.45.0

- Added `uniqueId` option to allow the same collection to be synced in parallel.
- Deprecated option `shouldRemoveMetadata` in `detectSchemaChange`. Prefer `shouldRemoveUnusedFields`.

# 0.44.0

- Bump dependencies.

# 0.43.2

- Escape period in regex.

# 0.43.1

- Fix issue where omitting nested paths failed to remove the field from `updateDescription.updatedFields` do to dotted field name.

# 0.43.0

- Optionally pass an array of operation types (`insert`, `update`, ...) to `processChangeStream`.
  This allows you to skip operations you don't care about. For example, a `delete` operation
  due to a collection TTL index.

# 0.42.0

- NOTE: Breaking change!
- `processChangeStream` is now batched, meaning the callback now receives an array of
  change stream events.

# 0.41.0

- More robust error code handling for `missingOplogEntry`.

# 0.40.0

- Don't emit the `cursorError` event when stopping.

# 0.39.0

- Expose FSM via `state` property for `runInitialScan` and `processChangeStream`.

# 0.38.1

- Fix bug with fsm. Added accompanying test.

# 0.38.0

- Remove all health check code in favor of using the `cursorError` event.
- Simplify some code.
- Add support for sort order (asc, desc) on initial scan.

# 0.37.0

- Emit `cursorError` when an error occurs when calling `hasNext`. Useful for debugging
  syncing issues like a missing oplog entry caused by an inadequate oplog window.
- `missingOplogEntry` utility function.
- Change stream health check behavior has changed. The `healthCheck.field` option has been
  removed since determining a failed health check relies on a delayed check of a
  Redis key and not a MongoDB query.
- Type parameter on `initSync` to allow for extending the event emitter. Useful for downstream
  libraries like `mongo2mongo` and `mongo2elastic`.

# 0.36.0

- Optional `pipeline` for `runInitialScan`.

# 0.35.0

- Handle master failover scenario properly for initial scan.
- Emit `initialScanComplete` when initial scan is complete.

# 0.34.0

- Bump default `maxSyncDelay` to 5 minutes.

# 0.33.0

- Fixed cursor exhausted bug with initial scan.
- Simplified change stream cursor consumption.
- Resync flag via `detectResync`.
- Changed health check options for both initial scan and change stream.
- Change stream health checker requires a date field to determine if syncing has stopped.

# 0.32.0

- Better handling of state transitions.

# 0.31.0

- Explicit types for health check failure.
- Emit the following field to differentiate failure types:
  `failureType` with values `initialScan` and `changeStream`.

# 0.30.0

- Fixed bug preventing a previously completed initial scan from being stopped once restarted.
- Renamed `maintainHealth` to `enableHealthCheck`.
- Health check only emits now. You must call `restart` manually to reproduce the previous behavior.

# 0.29.0

- Make schema change event more specific - `change` is now `schemaChange`.

# 0.28.0

- Don't type event emitter so that it can be reused by downstream libraries like `mongo2crate` and `mongo2elastic`.

# 0.27.0

- BREAKING CHANGE: Moved emitter to top level.
- Emit event `healthCheckFail` if a health check fails.

# 0.26.1

- `Document` type should refer to type from `mongodb` not DOM.

# 0.26.0

- BREAKING CHANGE: `processChangeStream` now takes an option object instead of a pipeline for the second argument.
- Added `restart` fn to `runInitialScan` and `processChangeStream`.
- Added the following options to `runInitialScan` and `processChangeStream`: `maintainHealth` and `healthCheckInterval`. When `maintainHealth` is set to `true` a failure to write a record when performing an initial scan within `healthCheckInterval` will cause `runInitialScan` to restart. When `maintainHealth` is set to `true` a failure to process the latest insert within `healthCheckInterval` when processing a change stream will cause `processChangeStream` to restart.
- Use more generic `Record<string, any>` for type `JSONSchema`.

# 0.25.0

- Latest `prom-utils` that includes `batchBytes` option.

# 0.24.0

- Use `eventemitter3`.

# 0.23.0

- `JSONSchema` type.

# 0.22.0

- Option to strip metadata from a JSON schema for `detectSchemaChange`.
- You can now await `changeStream.start` which resolves after the initial
  change detection has completed.

# 0.21.0

- Ensure that you can call `start` after calling `stop`.

# 0.20.0

- Bump `mongodb` peer dep.

# 0.19.1

- Fix issue with `runInitialScan` where calling `stop` before the scan had finished
  would incorrectly set the scan completed key in Redis. Also, `stop` now awaits flushing
  the queue.

# 0.19.0

- BREAKING CHANGE: Changed API for `runInitialScan`. You must explicitly call `start` now.
  The change stream can be stopped by calling `stop`.
- It is now possible to cleanly stop `runInitialScan` and `processChangeStream`, allowing
  for a smooth restarting behavior if a schema change is detected.

# 0.18.0

- Await processing of event when calling `stop`.

# 0.17.0

- BREAKING CHANGE: `initSync` now takes `collection`.
- NEW: `detectSchemaChange`.

# 0.16.0

- BREAKING CHANGE: Changed API for `processChangeStream`. You must explicitly call `start` now.
  The change stream can be stopped by calling `stop`.

# 0.15.0

- Export `getCollectionKey` util.

# 0.14.0

- Bump peer dependencies.

# 0.13.0

- Omit nested fields for `update` operations.

# 0.12.0

- Fix `Document` type.

# 0.11.0

- Prepend `pipeline` with omit pipeline if `omit` is passed to `initSync`.

# 0.10.0

- Renamed `Options` to `SyncOptions`.

# 0.9.0

- Moved `omit` to `initSync` so that it can be used in the `processChangeStream` pipeline.

# 0.8.0

- Don't delete the last scan id key when initial scan is completed.
- Allow an initial scan to resume after completion by calling `clearCompletedOn` first.
- Export `getKeys` fn.
- Changed Redis key prefix from to `mongoChangeStream`.

# 0.7.0

- `omit` option for excluding undesired fields.

# 0.6.0

- `sortField` option for overriding the default sorting field of `_id`.

# 0.5.0

- Pass `QueueOptions` to `runInitialScan`.

# 0.4.0

- Bumped `prom-utils` to latest.

# 0.3.0

- Batch `runInitialScan`.

# 0.2.0

- Separate out event stream handling into the `processChangeStream` method.

# 0.1.0

- Initial release.
