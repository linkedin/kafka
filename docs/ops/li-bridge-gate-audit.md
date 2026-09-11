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

# Runtime gate audit — review evidence draft

Scope: upgrade-stack runtime changes relative to `3.9-li`, plus the paired 3.0
bridge changes. This records code paths, not a claim that all release tests passed.
The current full verifier uses 6ea4d367d2 and predates the final default-off fixes
being tested in `/tmp/li-admin-placement-gate`.

All suffixes below use `li.protocol.bridge.<suffix>.enable`. The 3.9 Active getters
require empty process.roles as well as the Boolean flag. Every Boolean defaults
false. Construction parameters and immutable protocol/data declarations are
listed separately from actions taken by the broker.

| Gate | Runtime boundary / disabled behavior | Verification surface |
|---|---|---|
| mode | ControllerChannelManager snapshots the flag once per batch and chooses v2/v5/v1; false uses the original metadata-version branches. RemoteLeaderEndPoint uses -104 only with follower recovery too. AlterPartition retries copy mutable data only while the flag is active. | ControllerChannelManagerTest; BridgeAlterPartitionRetryTest; protocol fixtures; retained process-selection logs |
| config.metrics | LiProtocolBridgeMetrics registers/removes gauges only when opted in. Final fix extends the same opt-in to the three new KafkaController diagnostic gauges; native gauges remain. | LiProtocolBridgeMetricsTest; KafkaControllerTest.testCompatibilityControllerMetricsRequireOptIn; shutdown ownership tests |
| topic.deletion.state.cleanup | New-controller complete metadata images, cache replacement, unhosted-log reconciliation and topic-ID recovery occur only under cleanup. KRaft requests are excluded. 3.0 deletion acknowledgement fencing persists when mode is off but cleanup stays on. | BridgeMetadataCacheEpochTest; BridgeTopicIdentityTest; BridgeStrayLogDeletionTest; TopicDeletionManagerTest; native deletion real-broker before/after probes and scenario revision 5 |
| follower.recovery | KafkaApis admits the private -104 query and emits 1107 only through this flag. Generic timestamp helpers alone do not admit a wire request. | KafkaApisTest; BridgeProtocolConstantsTest; both recovery directions |
| recommended.leader.election | KafkaApis rejects election type 2 when disabled. KRaft ControllerApis always rejects it. The election helper restricts the target to live ISR members. | LiControllerOperationsTest; controller/partition tests |
| metadata.exclude.partitions | KafkaApis requires both the request field and the feature flag before suppressing partition metadata. | KafkaApisTest; request/response wire fixtures |
| move.controller | ApiVersionManager filters advertisement/admission; KafkaApis requires CLUSTER_ACTION and the feature before deleting the controller znode. | ApiVersionManagerTest; LiControllerOperationsTest; unchanged private-API clients |
| shutdown.safety.override | Advertisement and handler admission are gated; override grant is broker-epoch fenced. Previously admitted work has its documented lifecycle. | LiShutdownSafetyTest; LiControllerOperationsTest |
| preferred.controller | KafkaServer registers/watches preferred IDs and KafkaController changes election/fallback/shutdown behavior only under the flag. ZkAdminManager's broker API filters through it. Final raw AdminZkClient fix makes optional config explicitly opt in too. | AdminZkClientTest (none/false/true plus manual assignment); controller and shutdown tests |
| federated.topics | Handler gate precedes ZooKeeper operations. Create/delete require topic authorization; list requires cluster describe. Extra startup roots are conditional. | KafkaApisTest; LiKafkaAdminClientTest; private API fixtures |
| rack.id.mapper | KafkaConfig loads a nonempty configured mapper only with the gate; otherwise identity mapping. | RackIdMapperTest |
| dynamic.topic.deletion | Only the gated controller watcher changes deletion pause state. A paused operation retains admitted callbacks. False uses the native delete.topic.enable state. | LiDynamicTopicDeletionTest; TopicDeletionManagerTest |
| produce.request.instrumentation | New per-request collector only while enabled; Disabled does not collect stages. Final fix makes it ignore partition setters and prevents later activation from logging an uncollected request. Logger also checks the dynamic flag. | ProduceRequestInstrumentationTest; acks=0/callback source checks |
| request.metric.buckets | RequestChannel creates size/time buckets only from the gated optional config. Empty maps produce no additional request groups. Empty or malformed configured boundary lists are rejected, not supported as a disabling syntax. | RequestMetricBucketsTest; KafkaConfigTest boundary cases |
| request.channel.watchdog | Data-plane histogram, health scheduler and PoisonPill construction/actions are gated. Old constructors default the watchdog off. | RequestChannelWatchdogTest; KafkaServerTest interval case; PoisonPillProcessTest |
| minimum.log.roll | KafkaConfig passes zero when disabled. Storage's explicit li.min.log.roll.ms also defaults zero. Old RollParams constructor supplies zero. Size/index/relative-offset rolling checks remain separate. | LogSegmentTest in full storage suite; configuration tests |
| reassignment.cancellation.safety | Only cancellation with the gate invokes the minimum-live-original-replica check. Ordinary reassignment and flag-off cancellation keep native behavior. | KafkaControllerTest; reassignment cancellation process case |
| list.offsets.instrumentation | Data-plane and flag conjunction reaches a collector with disabled registration/usage early returns otherwise. Snapshot/reset is synchronized. | ListOffsetsRequestInstrumentationTest |
| static.default.quotas | QuotaFactory passes Long.MaxValue when disabled. Explicit dynamic/callback limits retain native arithmetic; static fallback is used only for absent limits. | QuotaFactoryTest; ClientQuotaManagerTest including PR 590 regressions; RequestQuotaTest |
| replica.request.timeout | effectiveReplicaRequestTimeoutMs selects requestTimeoutMs when disabled. BrokerBlockingSender uses that accessor. | ReplicaRequestTimeoutConfigTest (added to focused verifier selection) |
| offsets.topic.config | AutoTopicCreationManager copies the original properties; overrides only under the gate. | AutoTopicCreationManagerTest (added to focused selection) |
| leader.transfer.on.isr.shrink | Partition suppresses shrinking below minimum ISR and submits a live ISR target only while enabled. KafkaServer defaults to NoOp manager and ReplicaManager schedules transfers only under the gate. | PartitionTest; LeaderTransferManagerTest; legacy constructor tests |
| legacy.request.metrics | Constructors receive false by default; additional broker/replica/request counters, metadata egress and topic-name diagnostics are conditional. | LegacyRequestMetricsTest; MetadataOutgoingBytesTest; BrokerTopicMetricsTest; LogDirFailureChannelTest |
| log.truncation.metrics | KafkaConfig passes false when disabled; explicit internal log setting defaults false; meters are referenced/updated only when enabled. | UnifiedLogTest.testTruncateTo; full storage suite |

## Other explicit configuration and API boundaries

- `li.zookeeper.pagination.enable` defaults false. Native GetChildren requests are
  retained unless it selects the paginated request. Capability failure occurs before
  connection/registration. Positive tests pin the vendor client/Jute bytes. The
  deployment's real server pairing remains a separate release input.
- `li.num.controller.init.threads` defaults one. At one, the extra-client range is
  empty and initialization uses the original synchronous client. Extra clients and
  the executor exist only for a configured count above one. Failure causes propagate.
- `observer.class.name` defaults NoOpObserver; blank also selects NoOp without an
  error. Real observer code requires an explicit configured class. Existing callback
  parameters default NoOp and old JVM constructors remain available.
- `create.topic.policy.class.name` is the opt-in for LiCreateTopicPolicy. The class
  is not installed by the native broker default; the wrapper selects its policy.
- `controlled.shutdown.safety.check.enable` defaults false and independently gates
  the minimum-ISR safety check. Preferred-controller minimum checks additionally
  require the preferred-controller gate.
- `maintenance.broker.list` defaults empty. Its nonempty value is the explicit
  placement opt-in; it is not enabled merely by a preferred-controller znode.
- Storage-specific `li.min.log.roll.ms` (zero) and
  `li.internal.log.truncation.metrics.enable` (false) are explicit config opt-ins,
  including topic overrides. Do not mistake a helper accepting those values for
  an unconfigured broker activating them.
- New Admin methods send private requests only on explicit invocation. Existing
  electLeaders uses the original builder with an empty recommendation map. Private
  error decoding and message schemas are compatibility vocabulary, not independent
  broker actions. Advertisement, handler gates and authorization control execution.

## New audit regressions and corrections

- PR 590 repaired the ungated explicit-large-quota window regression; callback
  arithmetic no longer treats every value >= Long.MaxValue as absent.
- Raw default/config-disabled AdminZkClient assignment excluded a preferred ID:
  before test failed with replication factor 3 > available brokers 2. Opt-in fix
  restores all three brokers, while explicit enablement excludes the preferred ID.
- KafkaController registered three new diagnostic gauges with all flags off:
  before test observed a forbidden ActivePreferredControllerCount registration.
- Disabled produce instrumentation retained request partitions and could be logged
  after activation: before tests failed on retained data and request access. The
  singleton setter is now a no-op and the logger skips uncollected requests.
- The old reply claiming empty buckets were supported was incorrect. Source and
  tests reject them; an explicit correction was posted and the ledger updated.

## Audit boundary

This matrix must be checked against final published source and actual test results.
A passing process run covers its configuration and actions, not every disabled-path
claim. The newly added default-off fixes are not covered by the still-running full
verifier on the prior source. Final-source qualification, publication/readback and
remaining requirement checks are still needed before goal completion.
