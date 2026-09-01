# Moving `3.0-li` to `3.9-li`

## The short version

Upgrading Apache Kafka from 3.0 to 3.9 is not unusual. Upgrading our fork is harder because `3.0-li` changed several internal Kafka protocols in ways that later collided with Apache's changes.

We cannot safely put an ordinary 3.9 broker into the current cluster. A `3.0-li` controller may send it a message that 3.9 interprets differently. The reverse is also true: if a 3.9 broker becomes controller, it may send a message that a `3.0-li` broker misreads.

The safe route is to give the two releases a temporary common language:

```text
current 3.0-li protocol
        ↓
common bridge protocol
        ↓
native Apache 3.9 protocol
```

That leads to five broad moves:

1. Observe the current system so that we know which LI features are actually used.
2. Roll a bridge-capable `3.0-li` binary to every broker, without changing behavior.
3. Turn on bridge mode across the all-3.0 cluster.
4. Replace brokers with bridge-capable `3.9-li`, one at a time.
5. After the last 3.0 broker is gone, turn off bridge mode and move to the native 3.9 protocol.

Clients stay where they are. Producers, consumers, Streams applications, Connect workers, and ordinary AdminClients should not need new binaries or new configuration. If a required client change appears during testing, the broker migration is not ready.

This document describes that journey, the safety checks at each boundary, and the behavior we expect to lose.

## Why the fork cannot roll directly to 3.9

Three messages carry most of Kafka's controller-to-broker work:

- `LeaderAndIsr` tells a broker to become leader or follower and gives it the current replica state.
- `UpdateMetadata` refreshes the broker's view of partition leaders and live brokers.
- `StopReplica` tells a broker to stop or delete a replica.

`3.0-li` inserted `MaxBrokerEpoch` into new versions of these messages. It also shifted the version numbers of later fields. Apache Kafka later used some of the same version numbers for different layouts.

The collision is dangerous because both brokers recognize the numeric version. Negotiation succeeds, but the two sides disagree about the bytes that follow. A clean failure would be fortunate; a request that parses into the wrong state would be worse.

The last request versions with the same meaning on both sides are:

| Message | Bridge version |
|---|---:|
| `LeaderAndIsr` | 2 |
| `UpdateMetadata` | 5 |
| `StopReplica` | 1 |

The source supports this conclusion. Cross-version byte tests and mixed broker tests must still prove it before production.

## What bridge mode does

The `3.0-li-3.9-bridge` branch adds this cluster-level dynamic setting:

```properties
li.protocol.bridge.mode.enable=true
```

When it is false, `3.0-li` behaves as it does today.

When it is true, a `3.0-li` controller sends:

```text
LeaderAndIsr v2
UpdateMetadata v5
StopReplica v1
```

It stops merging newly queued work into `LiCombinedControl`, private API 1001. A finite batch that was already removed from the queue and merged before the dynamic update is allowed to drain so its callbacks are not abandoned. Deployment must wait for the API 1001 count to become stable and then force a controller election before admitting 3.9.

The setting is cluster-wide. Per-broker dynamic overrides are rejected. The controller publishes a `LiProtocolBridgeModeEnabled` gauge and logs the selected message versions when the mode changes.

The bridge binary continues to understand all current `3.0-li` messages. That is what makes the first rolling deployment safe: an old controller can still talk to a new bridge broker, and a bridge controller can talk to an old broker using the older common versions.

The 3.0 bridge deliberately leaves LI's tiered-storage protocol behavior unchanged:

| Meaning | LI value | Apache value |
|---|---:|---:|
| Earliest offset still stored locally | `-104` | `-4` |
| Offset moved to remote storage | error 1107 | error 109 |

During the mixed period, the 3.9 bridge must accept LI values `-104` and 1107 and must use LI-compatible follower-recovery behavior when talking to old brokers. This keeps the 3.0 bridge behavior unchanged while its mode is disabled and avoids asking 3.0 to advertise ListOffsets v8, which it does not support. After every broker is on 3.9, native mode uses Apache values `-4` and 109.

The deployment configuration says remote storage is disabled. Live configuration and state must confirm that before tiered-storage compatibility is treated as a defensive path rather than an active production requirement.

## Why 3.9 needs bridge mode too

A bridge-capable 3.0 controller can safely send v2/v5/v1 to an ordinary 3.9 broker. That solves only half the problem.

Any broker in a ZooKeeper cluster can become controller. If a plain 3.9 broker wins the election while 3.0 brokers remain, it selects Apache request versions. Some of those version numbers have different meanings in `3.0-li`.

The mixed cluster therefore needs this symmetry:

| Controller | Broker | Required behavior |
|---|---|---|
| 3.0 bridge | 3.0 bridge | send v2/v5/v1 |
| 3.0 bridge | 3.9 bridge | send v2/v5/v1 |
| 3.9 bridge | 3.0 bridge | send v2/v5/v1 |
| 3.9 bridge | 3.9 bridge | send v2/v5/v1 |

The 3.9 implementation must preserve Apache's native schemas. It should not copy LI's conflicting standard versions. It needs an outbound bridge mode that selects v2/v5/v1, plus narrow compatibility code for LI behavior that old brokers and tools still use.

Once every broker is on 3.9, the symmetry is no longer needed. We turn bridge mode off and use Apache's schemas.

## Before changing a broker

The repository tells us what code exists. It does not tell us everything production uses. Before building the target, collect facts from the running clusters.

### Read the live configuration

For every cluster:

- collect rendered broker configuration;
- describe static and dynamic broker configuration;
- describe dynamic topic configuration;
- record the ZooKeeper version, JDK, broker plugin set, and deployed client-library floor;
- inspect LI-specific ZooKeeper state; and
- measure private API traffic.

The source configuration currently says:

| Feature | Observed setting |
|---|---|
| Combined control | enabled |
| LI async fetcher | enabled |
| Parallel controller initialization | 10 threads |
| ZooKeeper pagination | enabled, except explicit development overrides |
| LI rack mapper | configured |
| Preferred controllers | used by role and cluster overrides |
| Controlled-shutdown safety | enabled by default |
| Remote storage | disabled |
| Drop-corrupted-files | disabled |

Do not promote the last two observations to facts until live configuration confirms them.

`tests/bin/li_bridge_preflight.py` turns the rendered configuration and ZooKeeper checks into a repeatable, machine-readable gate. Supply every effective 3.9 broker properties file with `--broker-config`, every remaining 3.0 bridge file with `--legacy-broker-config`, and optionally a Kafka distribution plus the live ZooKeeper connection:

```bash
python3 tests/bin/li_bridge_preflight.py \
  --phase mixed \
  --require-all-gates \
  --legacy-broker-config rendered-3.0-broker-0.properties \
  --broker-config rendered-3.9-broker-1.properties \
  --kafka-home /path/to/kafka \
  --zk-connect host:port \
  --output-json bridge-preflight-evidence.json
```

The mixed phase rejects IBP 3.2 or newer, missing generation-appropriate protocol gates, KRaft process roles, LI combined control or async fetcher, enabled remote storage or corrupted-file dropping, mismatched live broker inventory, and non-empty corrupted-broker state. Legacy brokers require bridge mode; 3.9 brokers require the protocol and retained-API gates. The tool also captures the disposition of the other LI ZooKeeper paths. `--require-all-gates` additionally verifies every 3.9 wrapper compatibility gate.

### Find the clients of private APIs

We know that:

- `ktool` calls API 1000 to bypass the controlled-shutdown safety check for a broker epoch;
- `ktool` calls API 1002 to force a ZooKeeper controller election;
- the custom authorizer reads federated-topic state from ZooKeeper; and
- LI AdminClient uses the `ExcludePartitions` Metadata tag for `listTopics`.

We do not yet know every caller of federated APIs 1003-1005. If that remains unknown, the first 3.9 release must keep them.

We found no local use of `LICLOSEST` or MirrorMaker passthrough compression. That does not prove that externally deployed clients do not use them.

### Inspect persistent state

The LI fork adds or consumes state under paths including:

```text
/brokers/preferred_controllers
/brokers/corrupted
/brokers/shutdown
/federatedTopics
/topic_deletion_flag
```

A 3.9 controller that ignores one of these paths may change placement, shutdown, deletion, corruption recovery, or authorization behavior. Every path needs an owner and a disposition.

If any cluster uses remote storage, stop and write a separate migration plan. LI and Apache 3.9 remote metadata layouts are not identical. The internal metadata topic, snapshots, plugin interfaces, local cache, and remote object layout all need a converter or compatibility layer.

## Build 1: observe without changing behavior

The safest first release adds telemetry to `3.0-li` and changes no protocol behavior.

Measure:

- request versions for the three controller messages;
- API 1001 sends and request sizes;
- APIs 1000-1005;
- recommended election type 2;
- ListOffsets values `-104`, `-4`, and replica ID `-100`;
- errors 1107, 109, and 2000;
- controller queue time, serialization time, memory, and network volume;
- controller failover time; and
- client software names and errors.

Roll this build one broker at a time. Clients should see an ordinary broker restart and nothing else. If equivalent production telemetry already exists, this build may be combined with the next one, but doing so gives up a clean baseline.

## Build 2: deploy the dormant 3.0 bridge

Next, roll the bridge-capable `3.0-li` binary to every broker with bridge mode still false.

During this roll:

- old controllers continue sending the current LI protocol;
- bridge brokers continue accepting it;
- bridge controllers also behave normally because the mode is false; and
- Cruise Control, topic operators, and clients continue as before.

This stage should produce no intentional behavior change.

Before activation, deployment inventory must show that every live broker is running the bridge build. It must also prevent an old binary from being restarted by automation. A dynamic flag cannot make an old binary bridge-capable.

## Turn on the common protocol

After every broker has the bridge code, set the cluster-level dynamic config:

```properties
li.protocol.bridge.mode.enable=true
```

Wait until every broker reports the new value. Then trigger a second deployment of the same bridge build so that the deployment system performs another rolling restart. No code or configuration changes are required in that deployment; its purpose is to guarantee that the active controller restarts after bridge activation. The replacement controller starts with empty controller-channel queues and selects v2/v5/v1 from its first request.

This second roll is operationally redundant for ordinary brokers, but it is preferable to a manual controller move when the deployment system can perform and monitor rolling restarts more reliably. Before admitting 3.9, verify that the roll included the broker that was controller when the roll began and that the controller epoch advanced after bridge activation.

The cluster must not depend on a quiet period. Cruise Control and topic-management systems may issue requests at any time, and their full caller set is not known. Bridge activation must work while reassignments, creations, and deletions are in flight.

The controller epoch transition fences stale work from the previous controller. The new controller reconstructs ongoing reassignment and deletion state from ZooKeeper and continues using the bridge-compatible message forms.

The implementation is not production-ready merely because version-selection unit tests pass. Activation tests must run while continuously submitting:

- replica-set expansion and shrink;
- reassignment cancellation;
- topic creation and deletion;
- rapid delete and recreate;
- leader movement;
- clean and hard broker restarts; and
- controller failover.

The current branch has tests showing that reassignment can complete with bridge mode enabled, a reassignment submitted before dynamic activation can still complete afterward, and topic deletion works in bridge mode. Those are useful beginnings, not a substitute for mixed-binary, concurrent-mutation, and production-scale tests.

### What changes at this point

Combined control stops. The controller now sends three standard requests instead of one merged request.

The common versions omit fields available in the normal `3.0-li` requests:

- `MaxBrokerEpoch`;
- `AddingReplicas` and `RemovingReplicas`;
- flexible encoding;
- topic IDs in these control requests;
- full/incremental `LeaderAndIsr` type;
- per-partition StopReplica leader epochs; and
- per-partition StopReplica delete flags.

Kafka supported reassignment and deletion before these fields existed. The bridge must use and test the corresponding legacy semantics; changing only the serialized version is not enough.

Expect controller request count, allocation, network traffic, and convergence time to rise. The first full `UpdateMetadata` can be very large. Combined control was introduced partly to avoid building and retaining large per-broker requests. Test controller startup and failover with production-scale metadata before proceeding.

### How to back out

While every broker is still the 3.0 bridge build:

1. set `li.protocol.bridge.mode.enable=false` at cluster level;
2. wait for propagation;
3. trigger another rolling deployment of the bridge build so the active controller restarts; and
4. verify that current LI request versions and combined control resume.

Do not introduce 3.9 until the all-3.0 cluster has run successfully in bridge mode under production load.

## Build 3: make a bridge-capable `3.9-li`

Create `3.9-li` from an Apache 3.9 maintenance tag, preferably 3.9.2. Do not merge the 3.0 fork forward, and do not use `3.6-li` as the implementation base.

The first 3.9 build needs four kinds of work. Its LI compatibility flags default to false so the binary behaves like upstream unless the LinkedIn server wrapper enables them. For the mixed-version deployment, the wrapper must enable:

```properties
li.protocol.bridge.mode.enable=true
li.protocol.bridge.follower.recovery.enable=true
li.protocol.bridge.recommended.leader.election.enable=true
li.protocol.bridge.metadata.exclude.partitions.enable=true
li.protocol.bridge.move.controller.enable=true
```

The wrapper must render and expose the effective values so deployment can verify them before a 3.9 broker joins the old cluster. Every ZooKeeper broker publishes the effective values as 0/1 gauges under `kafka.server:type=LiProtocolBridgeMetrics,broker-id=<id>` with metric names `ModeEnabled`, `FollowerRecoveryEnabled`, `RecommendedLeaderElectionEnabled`, `ExcludePartitionsEnabled`, and `MoveControllerEnabled`.

Bridge mode also requires `inter.broker.protocol.version` to remain older than 3.2; the migration uses the existing 3.0 value. Kafka rejects bridge activation at 3.2 or newer because those metadata versions can contain a non-default leader recovery state that `LeaderAndIsr` v2 cannot encode. Disable bridge mode before raising the IBP.

### 1. Outbound bridge selection

When `li.protocol.bridge.mode.enable=true`, a 3.9 controller must send v2/v5/v1 and must not assume that every broker understands native 3.9 control versions.

When the flag is false, it must use normal Apache version selection.

### 2. Compatibility for old brokers

During the mixed roll, preserve the semantics still emitted by 3.0 brokers:

- recommended election type 2 and its tagged fields;
- earliest-local ListOffsets values `-104` and `-4`;
- tiered-storage errors 1107 and 109 if live state can produce them;
- controller ListOffsets replica ID `-100` used by delayed corruption elections; and
- preferred-controller controlled-shutdown error 2000.

Some of these may later be removed. They cannot be removed in the first mixed-version release.

### 3. Compatibility for old tools and clients

Preserve:

- API 1000 until `ktool` has another shutdown-override mechanism;
- API 1002 until `ktool` has another controller-movement mechanism;
- APIs 1003-1005 while their usage remains unknown;
- `MetadataRequest.ExcludePartitions` for old LI AdminClient; and
- current authorization semantics, including federated-topic state.

API 1001 is different. The 3.0 bridge stops sending it before 3.9 appears, so 3.9 does not need to continue combined control if production telemetry proves the send rate is zero.

### 4. Operational parity

Port or replace the behavior production depends on:

- preferred-controller placement and fallback;
- controlled-shutdown safety;
- recommended-leader transfer;
- corrupted-broker recovery and delayed election;
- rack-ID mapping;
- ZooKeeper pagination;
- parallel controller initialization;
- maintenance-zone and maintenance-broker handling;
- LI topic-creation policy;
- custom authorizer, observer, shutdown checker, and broker-ID integration; and
- required metrics and logs.

The LI async fetcher is enabled today. The likely target is Apache's 3.9 fetcher rather than a port of the old implementation, but that is a deliberate behavior change. Measure replication throughput, lag, thread use, truncation, and recovery before accepting it.

## Roll 3.9 one broker at a time

Start with a broker that is not controller. Keep bridge mode true and keep the existing IBP.

For the first canary:

1. Let it join the cluster.
2. Move replica leadership and workload to it.
3. Test an old leader with a new follower.
4. Test a new leader with an old follower.
5. Exercise Produce, Fetch, transactions, groups, AdminClient, and `ktool` without changing those clients.
6. Test reassignment and deletion involving the canary.
7. Only then let the canary become controller.
8. Repeat controller operations with old brokers still present.

If the canary fails, replace it with the 3.0 bridge binary. Keep bridge mode true. Do not roll back to the original, non-bridge 3.0 binary after a 3.9 broker has entered the cluster.

Continue by failure domain and then by progressively larger waves. Force controller movement between 3.0 and 3.9 brokers during the rollout. Keep Cruise Control and topic-management traffic running; a feature that works only in an idle test cluster is not ready.

Stop automatically for any unexplained:

- control request above v2/v5/v1;
- API 1001 send;
- parse or unsupported-version error;
- offline partition;
- persistent under-replication;
- ISR recovery failure;
- controller-election loop;
- authorization regression;
- `ktool` failure;
- acknowledged-record loss; or
- client-visible error or latency regression outside the normal rolling-restart SLO.

## Stay in bridge mode after the last 3.0 broker is gone

Do not switch protocols immediately after replacing the last broker. Run the all-3.9 cluster in bridge mode for a defined bake period.

During that period:

- force controller elections;
- run reassignments, deletions, creations, and cancellations;
- validate every retained LI feature;
- compare controller resource use with the old baseline;
- test the oldest supported clients unchanged; and
- rehearse a rollback in staging with production-like state.

A rollback to the 3.0 bridge is allowed only if tests prove that 3.9 has not written incompatible local state, ZooKeeper state, internal-topic records, or plugin state. Common wire versions are necessary for rollback, but they are not sufficient.

## Return to the Apache protocol

After the all-3.9 bake, cross the final 3.0 rollback boundary.

Set:

```properties
li.protocol.bridge.mode.enable=false
```

Every broker is now 3.9, so either controller mode is understood by every peer. Verify that the controller uses the native Apache versions appropriate for the existing IBP.

This restores Apache's native:

- reassignment fields;
- flexible control encoding;
- topic-ID control behavior; and
- per-partition StopReplica leader-epoch behavior.

It does not restore:

- LI combined-control batching;
- LI `MaxBrokerEpoch` caching; or
- the LI async fetcher, if the migration accepted Apache's implementation.

After this switch, do not roll a broker back to `3.0-li`.

Bake again before changing IBP.

## Raise the IBP to 3.9

The last step is a separate change. Set `inter.broker.protocol.version` to 3.9 using the Apache ZooKeeper-mode procedure and roll brokers if required.

Do not combine this with disabling bridge mode. Separating the changes leaves one cause to investigate at each boundary.

Old clients should continue to work through Kafka's normal API negotiation. Test the oldest supported LI producer, consumer, transactional client, Streams application, Connect worker, AdminClient, and operational tool before production.

Once IBP 3.9 is active, downgrade options narrow further. Recovery should stay on the tested 3.9 release line.

## What we lose

### Only while bridge mode is active

- newer controller-message fields;
- combined-control batching;
- `MaxBrokerEpoch` caching; and
- some controller efficiency.

The native Apache fields return when bridge mode is disabled.

### Permanently, unless reimplemented later

- API 1001 combined-control batching;
- the LI `MaxBrokerEpoch` optimization at the colliding versions;
- the LI async fetcher if Apache's fetcher is accepted; and
- compatibility with the old LI tiered-storage implementation if live checks confirm it is unused.

We also give up the ability to return to `3.0-li` after native 3.9 activation.

### What we must not lose silently

- preferred-controller behavior;
- safe controlled shutdown;
- `ktool` APIs 1000 and 1002;
- recommended leader transfer;
- corruption recovery;
- federated authorization;
- rack and maintenance-aware placement;
- ZooKeeper pagination;
- parallel controller initialization;
- `ExcludePartitions` metadata behavior;
- custom security and observer integrations; and
- operationally consumed metrics.

A missing item in this list requires a migration decision, not a release note after the fact.

## Proof required before production

Unit tests are the first layer, not the last.

### Byte compatibility

Compile 3.0 and 3.9 independently. Serialize on one side and parse on the other for:

- `LeaderAndIsr` v2;
- `UpdateMetadata` v5;
- `StopReplica` v1;
- recommended ElectLeaders v2;
- Metadata with `ExcludePartitions`;
- ListOffsets `-104` and `-4`;
- Fetch errors 1107 and 109;
- controlled-shutdown error 2000; and
- retained private APIs.

Compare decoded values and resulting behavior. “Did not throw” is not enough.

### Mixed brokers

Run separate broker processes for all four controller/broker combinations. Keep workload and metadata mutations active. Kill controllers during reassignment and deletion. Restart old and new followers at awkward points. Verify data, ISR, assignment, and authorization after recovery.

### Unchanged clients

Run supported old client artifacts against:

- all-3.0 before bridge activation;
- all-3.0 in bridge mode;
- mixed 3.0/3.9 in bridge mode;
- all-3.9 in bridge mode;
- all-3.9 with native control protocol; and
- all-3.9 at IBP 3.9.

The migration does not ask applications to understand bridge mode. If a required application needs a new client or configuration, stop and fix broker compatibility.

## Final sequence

```text
1. Measure the current system.
2. Roll bridge-capable 3.0 code with bridge mode off.
3. Verify every broker has the bridge code.
4. Enable bridge mode dynamically.
5. Redeploy the same 3.0 bridge build so every broker rolls and the active controller restarts.
6. Verify and soak the bridge-mode cluster under live mutations.
7. Build 3.9 with the matching outbound bridge and required LI compatibility.
8. Roll 3.9 brokers one at a time, exercising both controller directions.
9. Bake all-3.9 while bridge mode remains on.
10. Disable bridge mode and move to native Apache control messages.
11. Bake again.
12. Raise IBP to 3.9 as a separate change.
13. Remove compatibility code only after telemetry and owners say it is safe.
```

The bridge is temporary machinery. Its purpose is simple: make the old and new brokers agree long enough to replace every old broker without asking clients to participate.

## Implementation audit workspace

This section is an uncommitted working checklist. It is not a declaration of production readiness.

### Concrete deliverables

1. Both artifacts select the common controller protocol in bridge mode.
2. Every production-active 3.0-li extension has either a gated 3.9 implementation or an explicit, validated replacement.
3. Every compatibility gate defaults off in Kafka and is enabled explicitly by the internal wrapper.
4. Both artifacts expose the same effective-state metric names.
5. Kafka compiles with the production Scala 2.12 toolchain and the default Scala 2.13 toolchain.
6. The internal wrapper compiles against the actual bridge artifacts.
7. Cross-artifact wire fixtures and mixed-process tests prove both controller directions and unchanged clients.
8. Live rendered configuration and ZooKeeper state confirm that features classified as dormant are actually dormant.

### Evidence currently present

- Controller request selection tests cover `LeaderAndIsr` v2, `UpdateMetadata` v5, and `StopReplica` v1 on the 3.9 branch.
- The current 3.0 bridge archive compiles main and test sources with Scala 2.12 and Java 17.
- The current 3.9 bridge compiles main and test sources with Scala 2.12 and 2.13 and Java 17.
- All twenty-two `LiProtocolBridge*EnableProp` settings are defined with `false` defaults and are mapped explicitly by the wrapper branch.
- The 3.0 and 3.9 `LiProtocolBridgeMetrics` objects currently have identical metric-name sets.
- Static inspection and `javap` resolve every wrapper `KafkaConfig.*()` reference against the Scala 2.12 bridge classes. Compatibility tests also compile a subclass of the restored non-final `NoOpConsumerRebalanceListener`, preserving the 3.0 class shape used by old LinkedIn client test utilities.
- `tests/bin/stage_li_bridge_ivy.sh` reproducibly builds and stages the Scala 2.12 main/test artifacts in the LinkedIn local Ivy layout, including split 3.9 modules and the server-common test classifier. It requires Java 17, writes a SHA-256 manifest with the source commit and dirty-worktree state for all 25 staged files, and is explicitly local-only, not a replacement for signed publication. Locally staged artifacts now compile the wrapper's main, Java test, and Scala test sources against its real internal dependency graph. Without an external archive the wrapper suite passes 127 tests and skips only the opt-in external-binary test; with `KAFKA_30_TGZ` set, all 128 tests pass. Five authorizer integration tests that previously compiled but were not discovered now run normally.
- `tests/bin/verify_li_bridge.sh` is the single Java 17 verification entry point. Complete verification requires `KAFKA_30_TGZ`; omitting it fails fast unless the caller explicitly chooses `ALLOW_PARTIAL=1`. It forces fresh focused test execution, compiles Scala 2.12 and 2.13, stages artifacts, runs all 128 wrapper tests with the external 3.0 broker, builds the 3.9 release, executes the complete mixed-to-native/IBP smoke test, and retains per-command logs, timings, source revisions, and nested process evidence. It uses single-use Gradle daemons and stops build daemons before launching brokers so retained compiler workers cannot starve the process test. `BRIDGE_VERIFY_RESUME=1` reuses only commands already recorded as passed in the same evidence directory and only when SHA-256 fingerprints of every tracked and untracked source file in both checkouts still match. The verifier recomputes the same fingerprints before writing its final summary, so edits during a run also invalidate the evidence. This allows an interrupted long run to continue without accepting stale results or treating a failed attempt as success. `BRIDGE_VERIFY_FULL=1` additionally runs the complete clients, server, and storage suites. Every command stage has passed locally; an official clean-checkout invocation remains required.
- `tests/bin/audit_li_bridge_evidence.py` rejects incomplete or internally inconsistent evidence: missing/failed commands, absent phase transitions or resource samples, failed preflights, missing protocol selections, source revision omissions, and archive checksum mismatches. Strict options require full suites, clean checkouts, and retained archives. The auditor passes against a real resumed cross-artifact evidence bundle with archive re-hashing and no warnings. Together, the preflight and evidence auditors have thirteen unit tests.
- `tests/bin/li_bridge_preflight.py` has unit coverage for mixed/native phase validation, generation-specific and all-gate enforcement, rendered Java-properties parsing, and ZooKeeper child parsing. It emits JSON evidence suitable for attaching to a rollout review. The mixed-process test runs it against both rendered broker files and the live disposable ZooKeeper ensemble before admitting workload.
- The complete Kafka `clients:test`, `server:test`, and `storage:test` suites pass on Java 17. Focused core suites cover API advertisement, request quotas, KafkaConfig validation, produce instrumentation signatures, and the bridge-specific controller paths; a monolithic core run exceeds the local execution window.
- Bidirectional, non-empty byte fixtures now cover the common controller requests, recommended elections, metadata partition exclusion, and all retained private request APIs. Both artifacts decode the opposite artifact's fixtures and require byte-for-byte re-encoding.
- Matching constant tests pin private API IDs, recommended election value, LI local-offset timestamp, error 1107 mapping, and preferred-controller error 2000 on both branches. The 3.9 schema-scope test also requires all retained private APIs to be advertised only by ZooKeeper brokers, never KRaft brokers or controllers.
- The wrapper's opt-in `testChangingAclsWith30Broker` test loads the production `LiKafkaAuthorizerV2` and `CostToServeObserver` on a bridge-mode 3.9 broker, starts an external 3.0 bridge broker against the same ZooKeeper ensemble, creates an explicitly assigned two-replica topic, and runs the production authorization/observer flow while requiring the old broker to remain registered and free of control-protocol errors. Run it from the wrapper with `KAFKA_30_TGZ=<archive> ./gradlew :likafka:kafka-impl_2.12:test --tests '*.testChangingAclsWith30Broker'`.
- `tests/bin/li_bridge_mixed_cluster_smoke.sh` launches independently built 3.0-li and 3.9-li release archives against one ZooKeeper ensemble. It retains 50 metadata-volume partitions by default across every failover; `SCALE_TOPIC_COUNT` and `SCALE_PARTITION_COUNT` make the same test reusable with larger metadata sets. A local 2,000-partition run completes the full mixed-to-native and IBP 3.9 scenario successfully; controller readiness checks retry metadata mutations because broker registration can precede controller-cache convergence at this scale. This is not a substitute for production-scale metadata. It proves both controller directions, both broker rejoin directions, topic creation/deletion, replica-order reassignment, retained operational APIs 1000 and 1002-1005 under each controller, and 60 records of produce/consume across controller failovers. The first 40 records are produced and consumed with the unchanged 3.0 artifact, including while 3.9 is controller; the final phase uses 3.9 clients. A separately compiled 3.0 client commits a ten-record transaction and consumes it through a subscribed `read_committed` group, a 3.0 Streams application processes ten records end to end, and a 3.0 Connect standalone worker moves ten records through file source and sink connectors, all while 3.9 is controller. A third broker and a throttled 10 MB replica move keep reassignment active while control fails from 3.9 to 3.0; after the original replica rejoins, cancellation restores the original replica set. Dedicated recovery phases produce 20 MB by default while each binary is offline and require the returning 3.9 and 3.0 followers to rejoin the ISR in the opposite leader direction. A combined local run with 500 retained metadata partitions and 100 MB catch-up in each direction completes the entire bridge-to-native and IBP 3.9 sequence successfully. A disposable unclean-election scenario also proves the Apache 3.9 fetcher truncates a longer local log to a stale 3.0 leader using bridge-compatible epoch recovery. After the mixed phases, the test replaces the last 3.0 process with the 3.9 binary on its existing log directories, verifies an all-3.9 bridge-mode bake, disables bridge mode dynamically, mutates metadata before and after an active-controller restart, and verifies native mode remains effective. It then updates the rendered static configs, rolls both brokers separately to IBP 3.9, and verifies another ten records plus final metadata mutation. The test verifies each bridge controller logged v2/v5/v1 selection, verifies native-mode selection, rejects protocol parse/version errors in the process logs, and requires no unavailable or under-replicated partitions at every final boundary. It exposed and now covers idempotent creation of compatibility ZooKeeper roots on broker restart. Set `EVIDENCE_DIR` to retain mixed/native preflight JSON, archive hashes and run parameters, operation timings, protocol-selection logs, per-phase broker RSS, and `jcmd` heap summaries when available; `EVIDENCE_INCLUDE_LOGS=1` also writes a compressed process-log bundle.

### Wrapper-config classification

| 3.0-li production setting | 3.9 disposition |
|---|---|
| preferred controllers and fallback | gated implementation |
| controlled-shutdown safety and API 1000 | gated implementation |
| combined control | rejected because API 1001 collides; common control protocol replaces it |
| LI asynchronous fetcher | rejected; Apache 3.9 fetcher selected for qualification |
| parallel controller initialization | assignment and leader-state reads parallelized |
| ZooKeeper pagination | disabled-by-default setting retained |
| rack-ID mapper | gated implementation |
| long-tail produce instrumentation | gated implementation |
| request size/latency buckets | gated implementation |
| request-channel watchdog and heap dump | gated implementation |
| minimum segment roll interval | gated implementation |
| reassignment cancellation safety | gated implementation |
| leader transfer before unsafe ISR shrink | gated implementation |
| ListOffsets usage instrumentation | gated implementation |
| static producer/consumer quota defaults | gated implementation |
| replica request timeout | gated implementation |
| offsets-topic creation defaults | gated implementation |
| dynamic topic deletion | gated implementation |
| maintenance assignment exclusion | retained through broker and direct ZooKeeper admin paths |
| observer, KafkaActions, topic policy | wrapper-compatible interfaces and hooks retained |
| federated topic APIs/state | gated implementation |
| corrupted-file recovery and delayed corrupt elections, including ListOffsets replica ID `-100` | not forwarded: production values disable both and repository has no overrides |
| producer batch validation skip | rejected; Apache 3.9 always validates through its optimized path |
| startup lazy sanity checks | not forwarded: absent from the deployed 3.0.1.82 artifact despite stale wrapper settings |
| unofficial-client warning log | not forwarded: disabled in rendered repository configuration; observer classification retained |
| remote storage | Apache 3.9 implementation retained; repository configuration is disabled, pending live confirmation |

### Still required before completion

- Publish the LinkedIn 3.9.2 bridge artifacts, including the server-common test classifier required by wrapper integration tests, and regenerate the wrapper dependency spec from those official artifacts on the LinkedIn network. For local verification only, run `JAVA_HOME=<jdk17> LI_IVY_REPO=~/local-repo tests/bin/stage_li_bridge_ivy.sh` first.
- Push the wrapper `3.9-li-bridge` branch after the GitHub IP allow-list permits access.
- Qualify the Apache 3.9 replica fetcher against the removed LI async fetcher at production throughput and partition counts, including lag and thread use. Mixed-process functional coverage now proves up to 100 MB catch-up in both leader directions and an Apache-follower truncation to a stale 3.0 leader.
- Confirm remote storage is disabled in live rendered configurations and state before treating LI tiering compatibility as defensive only.
- Confirm no live cluster has corrupted-broker ZooKeeper state before relying on the production settings that disable corrupted-file recovery and delayed elections.
- Measure bridge-mode controller memory and queue behavior on production-scale metadata because v2/v5/v1 cannot use `MaxBrokerEpoch` sharing or `LiCombinedControl` merging. The 2,000-partition local run completed, but sampled pre-GC heap reached 864 MB on a 1 GB heap during the all-3.9 native phase; treat this as a reason to collect production-shaped heap/GC evidence, not as a capacity result.
- Obtain and review a fully green Java 17 CI run for all added tests, including mixed-process coverage. The local equivalent is `JAVA_HOME=<jdk17> KAFKA_30_TGZ=<archive> EVIDENCE_DIR=<dir> tests/bin/verify_li_bridge.sh`; use `BRIDGE_VERIFY_FULL=1` for complete clients/server/storage suites.
