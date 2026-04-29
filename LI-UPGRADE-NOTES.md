# LinkedIn Kafka Fork — Upgrade Notes

LinkedIn-specific operational guidance for upgrading `linkedin/kafka` deployments.
Apache's standard upgrade documentation lives in `docs/upgrade.html`; this file
covers behaviors that diverge from upstream because of LI patches.

---

## 3.0-li → 3.6-li

### ⚠ P0 — Wire-protocol version-number collision (`MaxBrokerEpoch`)

A 3.0-li → 3.6-li **rolling broker upgrade is not safe at IBP > 2.7** without
the procedure below. The 3.0-li fork added an `int64 MaxBrokerEpoch` field to:

- `LeaderAndIsrRequest v3`
- `UpdateMetadataRequest v6`
- `StopReplicaRequest v2`

Apache Kafka 3.1+ then reused those exact same version numbers for unrelated
schema changes (KIP-227 `AddingReplicas`/`RemovingReplicas`, the first
flexible-versions transitions, KIP-866 KRaft controller id). 3.6-li adopts the
upstream apache definitions verbatim and has no `MaxBrokerEpoch` field.

In a mixed cluster:

- A 3.0-li controller serializing `LeaderAndIsr v3` with `MaxBrokerEpoch` will
  be parsed by a 3.6-li broker as flexible v3 with `AddingReplicas`/`RemovingReplicas`.
  Result: deserialization errors on the broker's request thread, or worse,
  silent partition state corruption.
- The reverse direction (3.6-li controller → 3.0-li broker) fails the same
  way: 3.6-li sends flexible v3, 3.0-li reads fixed v3 + `MaxBrokerEpoch`.

The only mutually-safe versions are `LeaderAndIsr v0/v1/v2`, `UpdateMetadata
v0..v5`, `StopReplica v0/v1`. Those are negotiated only when the cluster IBP
is ≤ `KAFKA_2_7_IVx`.

### Rolling upgrade procedure (operator runbook)

1. **Before any 3.6-li broker rollout**, lower the cluster IBP to a value
   ≤ `2.7-IVx`. For most LI fleets running on `3.0-IV1`, set:

   ```properties
   inter.broker.protocol.version=2.7-IV0
   log.message.format.version=2.7-IV0
   ```

   Apply via dynamic broker config or rolling restart on 3.0-li. Verify with
   `kafka-configs.sh --bootstrap-server ... --describe --entity-type brokers`.

2. **Rolling restart** brokers onto 3.6-li, one at a time. The cluster will
   exchange controller-to-broker requests at v0/v1/v2 only — these are
   byte-identical between 3.0-li and 3.6-li, so mixed-state communication is
   safe.

3. After **every broker** is on 3.6-li, ratchet the IBP forward:

   ```properties
   inter.broker.protocol.version=3.6-IV2
   log.message.format.version=3.6-IV2
   ```

4. Once all brokers acknowledge the new IBP, the cluster speaks the apache 3.6
   wire schemas natively. The LI `MaxBrokerEpoch` request-cacheability
   optimization is permanently lost in this version of the fork; see "Future
   work" below if you need it back.

### Why option (b) was deferred

The audit considered re-introducing `MaxBrokerEpoch` in 3.6-li under new,
non-colliding version numbers (`LeaderAndIsr v8`, `UpdateMetadata v9`,
`StopReplica v5`) with version negotiation. That requires:

- New schema entries in `clients/src/main/resources/common/message/*.json`.
- A new `MetadataVersion` (IBP) marker that gates the new versions.
- Controller code that serializes `MaxBrokerEpoch` only when the IBP is at
  or above the new marker.
- Broker code that reads `MaxBrokerEpoch` only at the new versions.
- Updated `validVersions` in API metadata so controllers and brokers
  negotiate down to the apache version when talking to apache peers.

This is KIP-level work and is best handled as a separate, focused effort with
team review. The procedure above is sufficient for any 3.0-li → 3.6-li
upgrade as long as operators apply the IBP fence.

### Other audit findings already fixed in 3.6-li

These were addressed in the post-#538 LI commit series and require no
operator action:

- `RecommendedLeaderElectionCommand` Scala class restored (was missing,
  breaking `bin/kafka-recommended-leader-election.sh` with
  `ClassNotFoundException`).
- `li.drop.corrupted.files.enable` LogLoader recovery hook re-wired
  (config was parsing but had no runtime effect).
- `maintenance.broker.list` config consumer + `MaintenanceBrokerCount` JMX
  gauge restored (controller no longer treated maintenance brokers
  specially; gauge was missing from active-controller).
- `OffsetResetStrategy.LICLOSEST` enum constant restored (LI consumer
  configs of the form `auto.offset.reset=licloasest` were failing
  `IllegalArgumentException` at parse).
- LI metrics restored: `MaintenanceBrokerCount`,
  `OneAboveMinIsrPartitionCount`, `live-cleaner-thread-count`,
  `IncrementalFetchSessionCacheMissesPerSec`,
  `BytesInTotal`, `MessagesInTotal` (with full
  `CounterWrapper` infrastructure plus a new
  `KafkaMetricsGroup.newCounter` API to fill the gap left when
  metrics group migrated Scala→Java in 3.6).
- Dead `li.async.fetcher.enable` config removed (the underlying
  feature was retired in the squash; the stub left a no-op config
  that operators may have set in `server.properties` — see "Operator
  notes" below).
- Parallel-ZK controller startup feature restored (LIKAFKA-44768).
  `li.num.controller.init.threads > 1` once again splits ZK work
  across multiple `KafkaZkClient` instances during controller
  failover/init, restoring the ~3x speedup on 15k+ partition
  clusters. Default is 1 (sequential) so operators must opt in.

### Known regressions vs 3.0-li (acceptable for upgrade)

- **`MaxBrokerEpoch` request-cacheability optimization** — see above.
- **Async/event-based replica fetcher series** — `TransferLeaderManager`,
  `AbstractAsyncFetcher`, `AsyncReplicaFetcher`, `FetcherEventBus`,
  `FetcherEventManager` are absent. Brokers fall back to the synchronous
  fetcher. Config keys for the feature still parse but are no-ops.
- **Some LI metrics still not restored**:
  - `recompressionRate` — broker-side recompression detection.
    Restoration requires plumbing a `recompressApplied` field through
    `LogValidator` results and `LogAppendInfo`, plus an `AtomicLong`
    counter + gauge in `ReplicaManager`. Not blocking but used by LI
    compression-analysis dashboards.
  - `message-produce-latency-{avg,max}` — *client-side* sensor on
    `KafkaProducer`. Producer end-to-end latency tracking. Not affected
    by the broker upgrade but the metric is missing from the LI client
    library variant of this fork.
  - `totalTimeBucketHist` — broker request latency in custom buckets.
    Requires a custom `Histogram` class plus
    `totalTimeHistogramEnabledMetrics` and `requestMetricsTotalTimeBuckets`
    config plumbing through `KafkaConfig`. Mostly redundant with the
    existing percentile metrics.
  Dashboards and alerts that read these specific names will produce empty
  time series until follow-up commits restore them.
- **`rearrangePartitionReplicaAssignmentForNewTopics`** (LI method that
  consumed `getMaintenanceBrokerList` during topic auto-creation) was
  removed in the squash. Maintenance brokers are no longer excluded from
  new-topic placement at the topic-creation path. Follow-up needed.

### Operator notes

- If your `server.properties` contains `li.async.fetcher.enable=...`,
  remove that line. The config no longer exists in 3.6-li; brokers will
  log an "Unknown configuration" warning at startup but still start.
  The feature it would have toggled was retired in the squash — the
  config was a no-op anyway.

### Future work / known regressions vs 3.0-li

- **`MaxBrokerEpoch` request-cacheability optimization permanently lost**
  in this fork version. Re-introducing it under new non-colliding wire
  versions (audit option b) is KIP-level work.
- **Async/event-based replica fetcher series retired** —
  `TransferLeaderManager`, `AbstractAsyncFetcher`, `AsyncReplicaFetcher`,
  `FetcherEventBus`, `FetcherEventManager` are absent. Brokers fall
  back to the synchronous fetcher.
- **`rearrangePartitionReplicaAssignmentForNewTopics`** (LI method that
  consumed `getMaintenanceBrokerList` during topic auto-creation) was
  removed entirely from `KafkaController`. Maintenance brokers are no
  longer excluded from new-topic placement at the topic-creation path.
  Restoration requires either (a) reintroducing
  `AdminZkClient.assignReplicasToAvailableBrokers` (the LI variant that
  accepted a broker-exclusion set) plus
  `KafkaConfig.rackIdMapperForRackAwareReplicaAssignment`, or
  (b) reimplementing the topic-rewrite flow against the 3.6
  topic-id-aware ZK assignment write path (KIP-516). Both are
  KIP-level changes warranting team review. Mitigation in the
  meantime: operators can manually rebalance after topic creation via
  the reassignment tool, or coordinate creation timing to avoid
  maintenance windows.
- **3 LI metrics not restored** — see "regressions" above.

These regressions are listed in priority order. The wire-collision
fence in section 1 is the only operationally blocking item; everything
else affects observability or efficiency, not broker correctness.
