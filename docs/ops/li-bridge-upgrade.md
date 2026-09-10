<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Moving `3.0-li` to `3.9-li`

## Status and source of truth

**This plan is not approval to deploy.** The cluster stays in ZooKeeper mode. Before rollout, we need source tests, tests of the published binaries, live configuration and state checks, capacity results, security review and rollout approval.

The implementation base is Apache **3.9.2** with the reviewed LI bridge stack. Pin the final internal `3.9.2.N`, matching `3.0.1.N`, wrapper commit, archive checksums, JDKs and ZooKeeper runtime in the release record. A maintenance-baseline change requires a new qualification run; do not substitute a newer tag during rollout.

The current implementation is the split stack through `3.9-li-bridge/runbook`, with the companion `3.0-li-bridge/stray-log-cleanup` branch. It is not the closed aggregate PR 542. The canonical mergeable runbook is `docs/ops/li-bridge-upgrade.md` in that stack. This top-level file is the review workspace copy. Historical experiments are evidence, not current acceptance criteria.

**Clients do not change.** The supported producer, consumer, transactional client, Streams application, Connect worker, LI AdminClient and operational-tool artifacts/configuration must remain unchanged across every phase. Discovery must name their deployed version floor and owners. One 3.0 test archive is not proof for every externally deployed client.

## Why we need two bridge-capable binaries

LI 3.0 inserted `MaxBrokerEpoch` into standard controller requests and shifted later fields. Apache reused some numeric versions with different layouts. Negotiation alone cannot detect this collision.

| Controller request | Common version |
|---|---:|
| `LeaderAndIsr` | 2 |
| `UpdateMetadata` | 5 |
| `StopReplica` | 1 |

Both controller generations must send these versions throughout the mixed phase, regardless of the target broker generation. Any ZooKeeper broker can become controller; an ordinary Apache 3.9 binary is not an admissible replacement while 3.0 remains.

The bridge keeps native Apache schemas intact on 3.9. It does not merge the entire old fork forward. Bridge mode omits reassignment fields, flexible control encoding, topic IDs in these requests, full/incremental control type, and per-partition StopReplica epochs/delete flags. Legacy deletion and reassignment semantics must be tested, not merely serialized.

Deletion recovery also needs a shared rule: with `li.protocol.bridge.topic.deletion.state.cleanup.enable` enabled, a new ZooKeeper controller sends a full first metadata image. Each broker replaces its cache only on the first update of a newer controller epoch. This removes ghost topics whose deletion update was lost during failover. Same-epoch updates remain incremental. Enable this setting on every broker and verify its gauge before restarting the controller. Keep it enabled after bridge mode is disabled. The same gate retires an unhosted local log when metadata reports its deletion; hosted replicas still wait for `StopReplica`. Qualification must include persisted logs, canceled reassignments and name reuse across restart, not just empty-topic metadata operations.

Broker-to-controller ISR updates also need a bridge-mode retry fix on 3.9. A queued `AlterPartition` request must build from fresh data when controller failover changes the negotiated version. Check bridge mode when building each retry, including requests queued before activation; do not reuse data changed by an earlier version's builder.

LI combined control (private API 1001) is intentionally not retained on 3.9. The demonstrated collisions concern the **versions of standard control APIs**, not an asserted Apache assignment of key 1001. Disabling combined control and `MaxBrokerEpoch` sharing increases request count, allocations and potentially controller heap/queue pressure.

## The phase contract

`tests/bin/li_bridge_contract.py` defines the phase rules used by preflight, the process test and the evidence checker. If those rules change, run the tests again. Do not relabel old results.

| Preflight phase | Broker generations | IBP | `li.protocol.bridge.mode.enable` | Allowed next state |
|---|---|---|---|---|
| `dormant` | all 3.0 bridge | 3.0 | false | `legacy-bridge` |
| `legacy-bridge` | all 3.0 bridge | 3.0 | true | `mixed`, or validated return to `dormant` |
| `mixed` | both 3.0 bridge and 3.9 bridge | 3.0 | true | `all-39-bridge`, or qualified rollback to `legacy-bridge` |
| `all-39-bridge` | all 3.9 bridge | 3.0 | true | `native`, or qualified rollback through `mixed` |
| `native` | all 3.9 | **3.0** | false | `ibp-39` |
| `ibp-39` | all 3.9 | 3.9 | false | tested recovery on the 3.9 release line |

Do not combine bridge deactivation and IBP elevation. Kafka rejects bridge mode at IBP 3.2 or newer because `LeaderAndIsr` v2 cannot encode non-default leader recovery state. IBP is static: update rendered configuration and roll brokers separately. There is deliberately no native-to-3.0 rollback transition.

For a phase change, supply `--previous-phase` to preflight and retain both reports. The deployment system must independently enforce the binary/image allowlist and the approved transition; config files cannot prove which executable automation will restart.

## Feature contract and lifecycle

Kafka's 23 LI compatibility gates default false. The wrapper explicitly enables the production profile. With `--require-all-gates`, every gate below except the phase-dependent mode gate must stay true on 3.9 **including native and final-IBP phases**. Turning bridge mode off is not permission to turn off operational compatibility.

All names below have the prefix `li.protocol.bridge.` and suffix `.enable`. Effective gauges are under `kafka.server:type=LiProtocolBridgeMetrics,broker-id=<id>`.

| Setting suffix | Effective gauge | Change scope | Owner / disposition |
|---|---|---|---|
| `mode` | `ModeEnabled` | cluster dynamic; controller restart fence | Protocol: temporary control versions and version-changing ISR retries |
| `topic.deletion.state.cleanup` | `TopicDeletionStateCleanupEnabled` | cluster dynamic; enable on all brokers before controller restart | Controller: clear deletion blocks and replace stale metadata on a new controller epoch |
| `follower.recovery` | `FollowerRecoveryEnabled` | cluster dynamic | Replication: retain until old recovery callers are retired |
| `recommended.leader.election` | `RecommendedLeaderElectionEnabled` | cluster dynamic | Controller: retain old election type 2 |
| `metadata.exclude.partitions` | `ExcludePartitionsEnabled` | cluster dynamic | Clients: retain LI AdminClient behavior |
| `move.controller` | `MoveControllerEnabled` | cluster dynamic | Operations: retain API 1002 until tool replacement |
| `shutdown.safety.override` | `ShutdownSafetyOverrideEnabled` | cluster dynamic | Operations: retain API 1000 until tool replacement |
| `preferred.controller` | `PreferredControllerEnabled` | restart | Controller: placement and shutdown parity |
| `federated.topics` | `FederatedTopicsEnabled` | cluster dynamic | Security: retain APIs 1003–1005 and authorization state |
| `rack.id.mapper` | `RackIdMapperEnabled` | restart | Placement: production parity |
| `dynamic.topic.deletion` | `DynamicTopicDeletionEnabled` | restart | Controller: production parity |
| `produce.request.instrumentation` | `ProduceRequestInstrumentationEnabled` | cluster dynamic | Observability: production parity |
| `request.metric.buckets` | `RequestMetricBucketsEnabled` | restart | Observability: production parity |
| `request.channel.watchdog` | `RequestChannelWatchdogEnabled` | restart | Operations: production parity |
| `minimum.log.roll` | `MinimumLogRollEnabled` | restart | Storage: production parity |
| `reassignment.cancellation.safety` | `ReassignmentCancellationSafetyEnabled` | cluster dynamic | Controller: production parity |
| `list.offsets.instrumentation` | `ListOffsetsInstrumentationEnabled` | restart | Observability: production parity |
| `static.default.quotas` | `StaticDefaultQuotasEnabled` | restart | Operations: production parity |
| `replica.request.timeout` | `ReplicaRequestTimeoutEnabled` | restart | Replication: production parity |
| `offsets.topic.config` | `OffsetsTopicConfigEnabled` | restart | Storage: internal-topic creation parity |
| `leader.transfer.on.isr.shrink` | `LeaderTransferEnabled` | restart | Replication: safe leader transfer |
| `legacy.request.metrics` | `LegacyRequestMetricsEnabled` | restart | Observability: production dashboards |
| `log.truncation.metrics` | `LogTruncationMetricsEnabled` | restart | Observability: production dashboards |

Per-broker dynamic overrides of cluster-dynamic compatibility gates are rejected. The smoke profile intentionally enables only wire/tool compatibility and cancellation safety; it is not the complete production wrapper profile. Wrapper qualification and live admission use the full profile.

Two additional settings are not members of the 23-gate bundle: `li.zookeeper.pagination.enable` and `li.num.controller.init.threads`. Record their effective values and `ZookeeperPaginationEnabled` / `ControllerInitializationThreads` gauges. The documented source profile enables pagination and uses ten controller-init threads; confirm live values.

### Generation-specific behavior

- Leave `li.async.fetcher.enable=true` and `li.combined.control.request.enable=true` on legacy brokers if that is their existing production configuration. The bridge suppresses merging independently. Preflight rejects these obsolete implementations on **3.9 only**.
- Test Apache's replacement fetcher against the enabled LI async fetcher, not the old default synchronous path. Test recovery and truncation first. Then measure throughput, thread use and lag at production scale.
- In bridge mode, deletion must wait for every replica to acknowledge `StopReplica`, including replicas that were offline. LI's native fast-delete path can remove ZooKeeper state before sending a metadata deletion update. Without full-state control messages, that leaves ghost topics or orphaned replicas. Do not force-delete the ZooKeeper entry to bypass this wait. The old fast-delete behavior is unchanged while bridge mode is off.
- Preserve LI local-offset timestamp `-104` and error 1107 alongside Apache `-4` and 109. Bridge followers emit LI-compatible values without advertising unsupported ListOffsets v8 on 3.0. Native outbound recovery uses Apache values. Retained inbound compatibility does not migrate remote storage.
- Remote storage must be confirmed unused, including previously written topic/plugin/remote metadata and objects. Otherwise stop and create a separate conversion/compatibility plan.
- Corrupted-file dropping and delayed corrupt elections are not ported. Require `li.drop.corrupted.files.enable=false`, `li.leader.election.on.corruption.wait.ms=0`, and an approved disposition for corrupted-broker state. An empty znode alone is insufficient.
- Determine usage/disposition of LICLOSEST, passthrough clients, federated APIs, observer/security hooks and operational metrics. Unknown usage is not permission to remove a feature.

## Discovery and fail-closed admission

Assign one named owner for each release role above and one rollout approver per cluster. Before production admission, collect:

1. Every rendered broker config and deployment image/build identity.
2. Effective static/dynamic broker settings and dynamic topic settings, including defaults/synonyms.
3. Live broker IDs, cluster ID, controller epoch and compatibility gauges.
4. ZooKeeper client/Jute/server versions, loaded jar hashes, authentication and response-size settings.
5. Persistent state under `/brokers/preferred_controllers`, `/brokers/corrupted`, `/brokers/shutdown`, `/federatedTopics`, `/topic_deletion_flag`, plus remote/plugin state.
6. Actual client/tool versions, private-API traffic, production plugin set, JDK and operational metric consumers.

### Read-only live collector

Compile the helper using the selected 3.9 release jars. Client credentials remain in the supplied properties file and are not emitted into inventory JSON:

```bash
javac --release 11 -cp '/releases/kafka-39/libs/*' -d /tmp/bridge-tools \
  tests/bin/LiBridgeLiveInventory.java tests/bin/LiBridgeRuntimeProbe.java
java -cp '/tmp/bridge-tools:/releases/kafka-39/libs/*' LiBridgeLiveInventory \
  broker:9092 /secure/admin-client.properties /evidence/live-inventory.json
```

The collector reads the effective protocol and safety settings in bounded requests. It includes internal topics and checks that the broker/topic inventory did not change during collection. Retry a changed inventory; do not omit brokers or topics to make it pass.

This is not a dump of every broker property. Keep the full rendered configurations and other operational settings in protected storage alongside the report. Preflight records hashes of its input files. Credentials are not copied into the inventory JSON. The collector is read-only; ordinary metadata mutations can continue.

Run the runtime probe on **each actual packaged broker/wrapper classpath**, not on the isolated vendor-test classpath:

```bash
java -cp '/tmp/bridge-tools:/actual/package/lib/*' LiBridgeRuntimeProbe \
  /evidence/broker-0-runtime.json true
```

A stock client without `getAllChildrenPaginated` must fail when pagination is required. Broker startup also validates this capability before opening its ZooKeeper session. Do not disable pagination to evade this failure on a large cluster.

The owner-reviewed state-disposition JSON must contain contract version 2, the cluster ID, a timezone-qualified collection timestamp, and `decisions` keyed by every inspected ZooKeeper path plus `remote-storage`, `plugin-state`, `client-floor`, and `artifact-admission`. Each decision needs `owner`, `evidence`, and `disposition`. Require `remote-storage.disposition=unused` and `artifact-admission.disposition=bridge-artifacts-only`.

Its `broker_runtimes` map, keyed by broker ID, must reference the probe's `pagination_supported`, `zookeeper_sha256` and `jute_sha256`, plus the deployed `server_version` and retained `qualification_evidence`. These are reviewed observations, not values to fabricate to satisfy the validator. The isolated 3.6.3-23 pagination test does not qualify a different deployed combination.

### Production preflight

```bash
python3 tests/bin/li_bridge_preflight.py \
  --previous-phase legacy-bridge --phase mixed \
  --require-live --require-all-gates --cluster-id '<expected-cluster-id>' \
  --legacy-broker-config /rendered/broker-0.properties \
  --broker-config /rendered/broker-1.properties \
  --live-inventory /evidence/live-inventory.json \
  --state-dispositions /evidence/state-dispositions.json \
  --kafka-home /releases/kafka-39 --zk-connect '<ensemble/chroot>' \
  --output-json /evidence/admission.json
```

Before starting a replacement, pass the **current** cluster's live gate and lint the prospective mixed configuration without `--require-live`/ZooKeeper arguments; also verify the candidate's approved image and packaged runtime. After registration, run the **new** phase's live gate before assigning workload or advancing the wave. A not-yet-started broker cannot supply a live config observation. The candidate must already have the symmetric bridge enabled because it could win a controller election immediately.

Repeat config arguments for **every** live broker. Reports distinguish `configuration-check` from `live-admission`. Production automation must require the latter, `passed=true`, contract version 2 and the expected cluster ID. Missing/stale/future-dated observations, unsafe overrides, missing dispositions and incomplete inventory block admission. Default freshness is 900 seconds; choose an approved limit for the cluster. Configure ZooKeeper shell authentication/TLS through the approved runtime environment; never bypass ACLs to obtain a green report.

## Qualification before the first canary

Run qualification against independently built archives, then repeat it against the actual published archives and official wrapper dependencies. Retain checksums, source identities, commands, phase results and logs.

```bash
JAVA_HOME=/path/to/jdk17 \
LI_BRIDGE_JAVA_30_HOME=/path/to/jdk11 \
KAFKA_30_TGZ=/releases/kafka-30.tgz \
KAFKA_39_TGZ=/releases/kafka-39.tgz \
SKIP_LOCAL_STAGE=1 WRAPPER_ROOT=/checkouts/kafka-server \
BRIDGE_VERIFY_FULL=1 EVIDENCE_DIR=/evidence/bridge \
tests/bin/verify_li_bridge.sh
```

For the protected release gate, use `tests/bin/verify_li_bridge_release.sh` with published `KAFKA_30_SHA256`, `KAFKA_39_SHA256`, approved full `KAFKA_30_COMMIT`, `KAFKA_39_COMMIT`, and `WRAPPER_COMMIT` in addition to the inputs above. It rejects dirty/unapproved checkouts, archive/source/checksum mismatches and partial verification, and runs a strict final audit.

Run the process test through `tests/bin/li_bridge_mixed_cluster_smoke.sh`. The Python runner starts and stops the processes, applies the phase rules, checks results, limits command runtime and saves failure logs. It tests:

- all-3.0 dormant operation with combined control and async fetching enabled;
- activation and all-3.0 backout while metadata mutations continue, with an old-controller restart at each boundary;
- both mixed controller/leader directions;
- canary and all-3.9 **persisted-state rollback** on the same log directories;
- old producer, subscribed consumer/group, transaction commit/abort/fencing, AdminClient and Streams instances kept alive across all phases;
- an unchanged Connect worker, and old/new private-API helpers in each phase;
- exact acknowledged record histories, recovery bytes verified after promoting each recovered replica, cancellation during failover, hard broker/controller termination, and delete/recreate completion;
- native control at IBP 3.0, then a separate IBP 3.9 roll.

Only the disposable truncation topic permits intentional unclean loss. The no-loss ledger is separate. File connectors are at-least-once: validate complete unchanged contents and report duplicate delivery separately; do not reinterpret normal connector replay as broker record duplication.

Set `SCALE_TOPIC_COUNT`, `SCALE_PARTITION_COUNT`, `RECOVERY_RECORD_COUNT`, and `RECOVERY_RECORD_SIZE` for a larger scenario. Effective defaults, deadlines and runtime profile are fingerprinted. `BRIDGE_VERIFY_RESUME=1` may reuse stages only for the same source/archive/scenario inputs; changing scale or runtime invalidates reuse. Functional smoke evidence is not a throughput benchmark.

The public `bridge-process` CI job checks out the matching 3.0 review branch, fixes that checkout to one commit, and builds the two binaries separately. The evidence records the commit and archive hash. `LI_BRIDGE_LEGACY_REF` selects the companion branch or commit; change it to the release branch after the 3.0 review branch is retired. This is review feedback, not release approval. Release CI requires approved commit hashes and published archive checksums. The protected `li-bridge-release-qualification.yml` workflow runs final published artifacts and the actual wrapper on an approved-network runner. Release owners must provision the `li-bridge-qualification` runner, protect the `li-bridge-release` environment, and authorize the wrapper checkout. A missing runner, token, archive or private dependency is **blocked**, not green. Never expose internal credentials to untrusted PR code.

### Qualification that cannot be inferred from the public smoke

Before admission, retain separate owner-approved results for:

- actual deployed client floor and `ktool`, production authentication/authorization and all enabled wrapper features;
- `LiKafkaAuthorizerV2.authorizeByResourceType`, producer-ID allocation policy and positive/negative security tests;
- packaged pagination against the actual server, including large lists, watch renewal/session expiry and authorization errors;
- rollback of any additional plugin/remote/internal state absent from the disposable fixture;
- disaster recovery and forward recovery where binary rollback is prohibited;
- production-shaped controller heap, GC, queue growth and failover, and replacement-fetcher capacity.

Each cluster's qualification record must name a baseline, tested metadata/partition shape and workload, acceptable p99 client impact, ISR recovery/failover bounds, heap/GC headroom, queue limits, bake durations, approver and evidence URL. **Do not proceed with blank thresholds.** There are no universal safe values to invent here.

## Rollout procedure and stop conditions

1. Collect baseline telemetry without changing behavior: control request versions, API 1001 sends/size, private APIs, recovery values/errors, controller memory/queues/serialization/network, failover, client software/errors and SLOs. Existing telemetry may replace a separate observation build only with an owner's completeness check.
2. Roll the 3.0 bridge with mode false. Confirm every live broker has the approved bridge build and automation cannot restart an original non-bridge image. The wire protocol stays unchanged. After every broker has the code, enable `li.protocol.bridge.topic.deletion.state.cleanup.enable` at cluster level. Wait for `TopicDeletionStateCleanupEnabled=1` on every broker before restarting the controller. This separately gated fix clears blocked-deletion state and reconciles stale metadata on a new controller epoch. Keep it enabled after disabling protocol bridge mode.
3. Set cluster-level bridge mode true. Wait for every broker's effective gauge. The sender rechecks activation after dequeue and during draining; work already admitted before its observation may finish. Config publication can race admission, so **do not rely on a quiet queue or a zero counter alone**.
4. Roll the same 3.0 bridge build again. Confirm the active controller restarted, its epoch advanced after activation, and the replacement selected v2/v5/v1 with fresh queues. Require no new API 1001 sends after that fence. Keep creations, deletions, reassignments, cancellations and client traffic running.
5. Bake all-3.0 bridge mode to the approved limits. Backout here is mode false plus propagation, another controller restart and verified resumption of LI native control/combined behavior.
6. Admit the first 3.9 canary only after final-artifact, persisted-state rollback and live admission gates pass. Start as a non-controller; exercise both leader/follower directions, clients/tools and mutations; then force it to become controller while old brokers remain.
7. Roll by failure domain and approved waves. Exercise controller movement in both directions. A failed canary may return to the **3.0 bridge only if the exact state/profile has qualified rollback**. Otherwise use the approved forward-recovery procedure. Never fall back to the original non-bridge 3.0 binary.
8. Bake all-3.9 with bridge mode still true. Revalidate retained features and performance; this remains the final qualified 3.0 rollback window.
9. Cross the recorded rollback boundary: disable outbound bridge mode, leave retained gates enabled, verify native control, restart the controller, and bake at IBP 3.0. From this point do not downgrade to 3.0.
10. Separately render IBP 3.9 and roll brokers. Re-run unchanged-client/tool checks and the `ibp-39` admission gate. Recovery stays on the tested 3.9 release line.

Automatically stop for unexpected control versions, post-fence API 1001 traffic, parse/version errors, offline partitions, persistent under-replication, ISR recovery failure, controller loops, security/tool regressions, acknowledged-record loss, or client impact outside approved rolling-restart SLOs. Preserve the evidence and identify the failing boundary before taking a recovery action. Do not “fix” a failed compatibility test by upgrading clients.

## PR inventory and merge order

All 32 open public PRs are covered below. Upgrade PRs carry `kafka-upgrade-august-2026`; CI foundations 558 and 559 do not. All current diffs are below 1,000 changed lines. These checks do not grant approval to deploy.

Closed PRs 542 and 555 are superseded. GitHub automatically closed 563 and 564 during the dependency reorder because their new heads were contained in their former base branches. No release branch was merged. Their restored, separate reviews are 579 and 578.

| PR | Responsibility / reviewer |
|---|---|
| [559](https://github.com/linkedin/kafka/pull/559) | 3.0 CI/publication — release engineering |
| [575](https://github.com/linkedin/kafka/pull/575) | 3.0 wire fixtures — protocol |
| [541](https://github.com/linkedin/kafka/pull/541) | 3.0 bridge, activation and deletion recovery — controller |
| [577](https://github.com/linkedin/kafka/pull/577) | 3.0 unhosted-log cleanup — storage/controller |
| [558](https://github.com/linkedin/kafka/pull/558) | 3.9 CI/publication — release engineering |
| [543](https://github.com/linkedin/kafka/pull/543) | 3.9 outbound bridge — protocol/controller |
| [544](https://github.com/linkedin/kafka/pull/544) | old wire/client/recovery compatibility — protocol/replication |
| [565](https://github.com/linkedin/kafka/pull/565) | control RPC wire definitions and fixtures — protocol |
| [545](https://github.com/linkedin/kafka/pull/545) | gated shutdown and controller-movement handlers — operations |
| [546](https://github.com/linkedin/kafka/pull/546) | federated APIs/state — security/controller |
| [547](https://github.com/linkedin/kafka/pull/547) | restored Admin surface and retries — clients/tools |
| [548](https://github.com/linkedin/kafka/pull/548) | wrapper extension points — wrapper/security |
| [560](https://github.com/linkedin/kafka/pull/560) | leader-transfer controller client — replication |
| [549](https://github.com/linkedin/kafka/pull/549) | ZooKeeper operational parity — controller/placement |
| [550](https://github.com/linkedin/kafka/pull/550) | quotas, timeouts, log/internal-topic defaults — server/storage |
| [561](https://github.com/linkedin/kafka/pull/561) | heap dump and termination — operations/runtime |
| [566](https://github.com/linkedin/kafka/pull/566) | storage metric primitives and cleanup — observability |
| [551](https://github.com/linkedin/kafka/pull/551) | broker metrics/watchdog wiring — observability/operations |
| [567](https://github.com/linkedin/kafka/pull/567) | gated deletion-state and metadata-cache recovery — controller |
| [576](https://github.com/linkedin/kafka/pull/576) | 3.9 unhosted-log cleanup — storage/controller |
| [568](https://github.com/linkedin/kafka/pull/568) | version-changing ISR retries — replication |
| [578](https://github.com/linkedin/kafka/pull/578) | packaged-client rejection and vendor pagination tests — ZooKeeper |
| [579](https://github.com/linkedin/kafka/pull/579) | supplied archives and wrapper-jar identity — release |
| [552](https://github.com/linkedin/kafka/pull/552) | phase/live preflight contract — deployment |
| [569](https://github.com/linkedin/kafka/pull/569) | unchanged-client and record-ledger helpers — verification |
| [570](https://github.com/linkedin/kafka/pull/570) | continuous old clients and metadata mutations — verification |
| [571](https://github.com/linkedin/kafka/pull/571) | live configuration and runtime collection — deployment |
| [553](https://github.com/linkedin/kafka/pull/553) | complete migration process runner — verification |
| [572](https://github.com/linkedin/kafka/pull/572) | strict evidence audit and negative tests — verification |
| [554](https://github.com/linkedin/kafka/pull/554) | verifier, local staging and command handling — verification/release |
| [573](https://github.com/linkedin/kafka/pull/573) | public process CI and protected release gate — release |
| [574](https://github.com/linkedin/kafka/pull/574) | runbook and review records — operations |

Merge 558 into `3.9-li` and 559 into `3.0-li` first. They have different release bases, so do not put them in one dependent Git stack. Rebase/retarget 575 to `3.0-li` and 543 to `3.9-li`; do not merge feature work into temporary CI branches.

The GitHub stack rooted at PR 575 has the 3.0 order **575 → 541 → 577**. The stack rooted at PR 543 has the 3.9 order:

**543 → 544 → 565 → 545 → 546 → 547 → 548 → 560 → 549 → 550 → 561 → 566 → 551 → 567 → 576 → 568 → 578 → 579 → 552 → 569 → 570 → 571 → 553 → 572 → 554 → 573 → 574**.

Retarget remaining layers after each independent merge. The wrapper branch contains the ACL test repair (`6ddf2a87`) and cleanup mapping/tests (`1a9ecccf`); its source suite passes 132 tests. Add the approved wrapper/dependency/security PR and named deployment-gate owner to the release record.

Publish the final matching archives and all required modules/test classifiers, regenerate wrapper dependency metadata from official artifacts, and run final-artifact qualification again. Automatic publication of an intermediate stack commit is not permission to deploy it. Remove temporary compatibility only after telemetry, caller owners and a separate retirement decision establish that it is safe.
