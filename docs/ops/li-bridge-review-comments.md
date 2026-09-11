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

# Review comment dispositions

All 62 original threads have replies with published source decisions. A fresh GraphQL readback across 41 PRs verified every expected reply, found no mismatches and found no new review threads. Resolved status alone was not accepted as proof. Re-fetch after the final publication and check the actual source/test coverage before closing the review.

## Later qualification findings

| Finding | Published response and evidence | Remaining limit |
|---|---|---|
| [F18: offline replica misses deletion](https://github.com/linkedin/kafka/pull/576#issuecomment-5629453741) | Paired complete-image recovery in 583/584; unassigned/leaderless/incremental tests and exact records after promotion. | Assignment does not establish topic identity; also require F19. |
| [F19: recreated topic already assigned to returning replica](https://github.com/linkedin/kafka/pull/584#issuecomment-5631079569) | Paired identity recovery in 585/586; missing/zero IDs, errors, retries, current/future copies and mixed-batch tests. [Qualification update](https://github.com/linkedin/kafka/pull/586#issuecomment-5638005151). | All four revision-4 record checks passed, but full final-source and wrapper qualification remain open. |
| [F20: rotated protocol logs omitted](https://github.com/linkedin/kafka/pull/584#issuecomment-5638004839) | PR 584 retains and scans hourly rotations; 586 is restacked on it. New positive and negative tests fail before the fix and pass afterward. | The previous failed CI job stays failed. Re-run the corrected collector. |
| [F21: churn exits during controller movement](https://github.com/linkedin/kafka/pull/586#issuecomment-5638182322) | PR 584 fixes the workload retry policy without changing the upstream broker response. [Test and code update](https://github.com/linkedin/kafka/pull/584#issuecomment-5638482746). | The complete revision-4 process run and audit pass; final F22/wrapper qualification remains required. |
| F22: bridge-state MBeans register by default | Paired [588](https://github.com/linkedin/kafka/pull/588)/[589](https://github.com/linkedin/kafka/pull/589) add a default-off diagnostics flag. Tests cover disabled registration, enabled readings, restart scope, KRaft and cleanup. | All 72 Python tests pass. The matching-jar wrapper suite now passes 133 tests; the complete final-source verifier remains open. |
| [F24: dormant-backout churn stalls](https://github.com/linkedin/kafka/pull/589#issuecomment-5639716967) | [592](https://github.com/linkedin/kafka/pull/592) keeps acknowledgement fencing while cleanup remains enabled; [593](https://github.com/linkedin/kafka/pull/593) requires native offline deletion in scenario revision 5. Unit and real-broker probes fail before and pass after. | Targets remain partial migrations. Complete final-source qualification is still required. |
| F26: merged non-delete response reaches deletion callback | [595](https://github.com/linkedin/kafka/pull/595) uses the native response delete bit under cleanup, preserving the old-wire fallback and genuine deletion errors. The before regression fails; controller/callback/deletion suites pass after repair. | Updated paired CI and final qualification remain required. |

## PR 541

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/541#discussion_r3926640391) | Reject bridge mode below IBP 2.2. | KafkaConfig validation; KafkaConfigTest and ControllerChannelManagerTest. | [reply](https://github.com/linkedin/kafka/pull/541#discussion_r3984220462) |
| [2](https://github.com/linkedin/kafka/pull/541#discussion_r3926640437) | The same IBP guard covers StopReplica v1. | KafkaConfig validation; controller version-selection tests. | [reply](https://github.com/linkedin/kafka/pull/541#discussion_r3984220659) |
| [3](https://github.com/linkedin/kafka/pull/541#discussion_r3926640487) | Treat malformed minimum-log-roll originals as disabled; do not throw from an MBean read. | LiProtocolBridgeMetricsTest.testMalformedMinimumLogRollValueDoesNotBreakGauge. | [reply](https://github.com/linkedin/kafka/pull/541#discussion_r3984220917) |
| [4](https://github.com/linkedin/kafka/pull/541#discussion_r3926885180) | Recheck activation after dequeue and while draining. Finish callbacks for work already admitted; do not admit a new combined request. | RequestSendThreadBridgeTest: blocked dequeue, sustained queue and admitted deletion callback. | [reply](https://github.com/linkedin/kafka/pull/541#discussion_r3984221128) |
| [5](https://github.com/linkedin/kafka/pull/541#discussion_r3926885240) | Tightened the sender and documented the admitted-work exception and required controller restart. | ControllerChannelManager and KafkaConfig docs; RequestSendThreadBridgeTest; runbook activation steps. | [reply](https://github.com/linkedin/kafka/pull/541#discussion_r3984221320) |
| [6](https://github.com/linkedin/kafka/pull/541#discussion_r3927016213) | Validate all three arguments before parsing. | CI 559: ClientUtils uses Objects.requireNonNull; ClientUtilsTest. | [reply](https://github.com/linkedin/kafka/pull/541#discussion_r3984221566) |

## PR 547

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/547#discussion_r3925893534) | Refresh controller metadata and retry NOT_CONTROLLER for create. | KafkaAdminClient handleNotControllerError; LiKafkaAdminClientTest.testCreateFederatedTopicRetriesAfterControllerChange. | [reply](https://github.com/linkedin/kafka/pull/547#discussion_r3984221855) |
| [2](https://github.com/linkedin/kafka/pull/547#discussion_r3925893591) | Refresh controller metadata and retry NOT_CONTROLLER for delete. | LiKafkaAdminClientTest.testDeleteFederatedTopicRetriesAfterControllerChange. | [reply](https://github.com/linkedin/kafka/pull/547#discussion_r3984222148) |
| [3](https://github.com/linkedin/kafka/pull/547#discussion_r3925893635) | Corrected the create-options Javadoc signature. | CreateFederatedTopicZnodesOptions Javadoc links to the Admin method. | [reply](https://github.com/linkedin/kafka/pull/547#discussion_r3984222348) |
| [4](https://github.com/linkedin/kafka/pull/547#discussion_r3925893670) | Corrected the delete-options Javadoc signature. | DeleteFederatedTopicZnodesOptions Javadoc links to the Admin method. | [reply](https://github.com/linkedin/kafka/pull/547#discussion_r3984222574) |

## PR 548

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/548#discussion_r3925879715) | Catch observer failures on response delivery; continue sending the response. | RequestChannel.sendResponse exception boundary; full core test suite is required in final verification. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984222804) |
| [2](https://github.com/linkedin/kafka/pull/548#discussion_r3925879779) | Catch observer failures before produce handling. | KafkaApis observeProduceRequest exception boundary; core request tests. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984223047) |
| [3](https://github.com/linkedin/kafka/pull/548#discussion_r3925879817) | Use Locale.ROOT and isolate the library-tracking callback. | KafkaApis client-software normalization and trackClientLibrary exception boundary. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984223356) |
| [4](https://github.com/linkedin/kafka/pull/548#discussion_r3925879855) | Use the declared replication-factor default when originals omit the setting. | LiCreateTopicPolicyTest.testDefaultReplicationFactorIsUsedWhenConfigIsAbsent. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984223549) |
| [5](https://github.com/linkedin/kafka/pull/548#discussion_r3925879891) | Catch LinkageError separately and fall back to NoOpObserver. | Observer.apply class-loading boundary; startup and observer tests. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984223801) |
| [6](https://github.com/linkedin/kafka/pull/548#discussion_r3925879933) | Corrected the interface documentation to name observe, observeProduceRequest and trackClientLibrary. | Observer Scaladoc; the nonexistent record reference is removed. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984223963) |
| [7](https://github.com/linkedin/kafka/pull/548#discussion_r3926406046) | Trim the class name and use NoOpObserver without reflection when it is blank. | ObserverTest.testBlankObserverClassUsesNoOpObserver. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984224156) |
| [8](https://github.com/linkedin/kafka/pull/548#discussion_r3927367747) | Preserved the old RequestChannel JVM constructor. | ObserverTest.testObserverParametersPreserveOldJvmConstructors. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984224332) |
| [9](https://github.com/linkedin/kafka/pull/548#discussion_r3927367833) | Preserved the old SocketServer JVM constructor. | ObserverTest.testObserverParametersPreserveOldJvmConstructors. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984224508) |
| [10](https://github.com/linkedin/kafka/pull/548#discussion_r3927367895) | Preserved the old KafkaServer JVM constructor. | ObserverTest.testObserverParametersPreserveOldJvmConstructors. | [reply](https://github.com/linkedin/kafka/pull/548#discussion_r3984224724) |

## PR 549

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/549#discussion_r3925880084) | Create the deletion-flag znode when SetData returns NONODE, tolerating another creator. | KafkaZkClientTest.testSetTopicDeletionFlagCreatesMissingZnode. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984224958) |
| [2](https://github.com/linkedin/kafka/pull/549#discussion_r3925880153) | Check the pagination result code and propagate failures. Also reject an enabled but unsupported packaged client at startup. | KafkaZkClient.getAllEntitiesWithConfig; vendor missing-path/authorization tests; stock-client startup rejection. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984225182) |
| [3](https://github.com/linkedin/kafka/pull/549#discussion_r3925880188) | Exclude out-of-sync replicas from the recommendation. | Partition.maybeTransferToNewLeader; PartitionTest.testLeaderTransferSelectsLowestInSyncFollower. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984225389) |
| [4](https://github.com/linkedin/kafka/pull/549#discussion_r3925880226) | Reuse a controller-owned executor instead of making one per load. | KafkaController.controllerInitExecutor lifecycle and load methods. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984225626) |
| [5](https://github.com/linkedin/kafka/pull/549#discussion_r3925880272) | Report a ConfigException naming the malformed maintenance setting. | KafkaConfigTest.testInvalidMaintenanceBrokerList. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984225815) |
| [6](https://github.com/linkedin/kafka/pull/549#discussion_r3925880321) | Report an AdminOperationException for malformed ZooKeeper maintenance values. | AdminZkClientTest.testInvalidMaintenanceBrokerListHasAdminError. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984226000) |
| [7](https://github.com/linkedin/kafka/pull/549#discussion_r3925880371) | Use one named daemon pagination thread per client instead of ten unnamed non-daemon threads. | ZooKeeperClient paginationExecutor; isolated pagination lifecycle tests. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984226183) |
| [8](https://github.com/linkedin/kafka/pull/549#discussion_r3926416124) | Classify leader transfer as restart-only, matching manager and partition construction. | DynamicBrokerConfig excludes the setting; dynamic-scope test and phase contract both require restart. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984226331) |
| [9](https://github.com/linkedin/kafka/pull/549#discussion_r3926416173) | Choose the lowest eligible in-sync broker ID. | PartitionTest.testLeaderTransferSelectsLowestInSyncFollower. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984226522) |
| [10](https://github.com/linkedin/kafka/pull/549#discussion_r3926416211) | Acquire initializationLock inside the executor task before accessing the session. | ZooKeeperClient paginated-request task; session-expiry/watch-renewal vendor test. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984226676) |
| [11](https://github.com/linkedin/kafka/pull/549#discussion_r3926416257) | Shut down the executor through the two-phase ThreadUtils helper before closing the session. | ZooKeeperClient.close; vendor fixture teardown and lifecycle checks. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984226898) |
| [12](https://github.com/linkedin/kafka/pull/549#discussion_r3927365912) | Always invoke the response callback, including executor rejection and unexpected failures. | ZooKeeperClient uses execute with task and submission exception handling; no thenAccept-only path remains. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984227099) |
| [13](https://github.com/linkedin/kafka/pull/549#discussion_r3927365992) | Unwrap CompletionException and throw its cause. | KafkaController.joinControllerInit is used by both parallel load methods. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984227279) |
| [14](https://github.com/linkedin/kafka/pull/549#discussion_r3927426758) | No source change needed: Scala emits a valid static forwarder on this trait. | javap of the compiled LeaderTransferManager shows public static noOp(); ReplicaManagerBuilder compiles against it. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984227448) |
| [15](https://github.com/linkedin/kafka/pull/549#discussion_r3927426849) | Retry only retriable top-level errors and retain only retriable partition recommendations. | LeaderTransferManager.handleResponse; LeaderTransferManagerTest retriable/non-retriable cases. | [reply](https://github.com/linkedin/kafka/pull/549#discussion_r3984227639) |

## PR 550

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/550#discussion_r3925887787) | Make usage state private and initialize it to an empty map; synchronize snapshots and resets. | ListOffsetsRequestInstrumentation; testConcurrentUsageTrackingDoesNotLoseRequests. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984227814) |
| [2](https://github.com/linkedin/kafka/pull/550#discussion_r3925887866) | Use numPartitionsInIsrState and update the instrumentation caller; retain the old name as a deprecated alias. | ReplicaManager and ProduceRequestInstrumentation use the clearer method name. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984228039) |
| [3](https://github.com/linkedin/kafka/pull/550#discussion_r3927365937) | Create and register the produce instrumentation logger only on the data plane. | KafkaApis checks DataPlaneAcceptor.MetricPrefix before constructing the logger. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984228224) |
| [4](https://github.com/linkedin/kafka/pull/550#discussion_r3927366021) | Rewrote the instrumentation description to state what is counted. | ListOffsetsRequestInstrumentation class and usage-logging documentation. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984228409) |
| [5](https://github.com/linkedin/kafka/pull/550#discussion_r3927434319) | Log completion on the acks=0 path as well as the response callback. | KafkaApis.handleProduceRequest invokes maybeLog before the acks=0 response path. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984228587) |
| [6](https://github.com/linkedin/kafka/pull/550#discussion_r3927434386) | Do not record zero-size samples for absent timestamp modes. | ListOffsetsRequestInstrumentationTest.testAbsentTimestampModesDoNotRecordZeroSizedSamples. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984228793) |
| [7](https://github.com/linkedin/kafka/pull/550#discussion_r3927434447) | Use optional callbacks when the data-plane logger is absent. | KafkaApis uses Option.foreach/map for both completion paths. | [reply](https://github.com/linkedin/kafka/pull/550#discussion_r3984228954) |

## PR 551

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/551#discussion_r3925877328) | Capture the post-truncation offset and size while holding the log lock. | UnifiedLog.truncateTo; truncation test and mixed stale-leader process scenario. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984229174) |
| [2](https://github.com/linkedin/kafka/pull/551#discussion_r3925877401) | Reject empty bucket lists with ConfigException, as requested. The previous reply and ledger incorrectly claimed support for empty lists. | KafkaConfigTest.testEmptyRequestMetricBuckets asserts rejection; malformed, negative and unordered cases have separate assertions. | [correction](https://github.com/linkedin/kafka/pull/551#discussion_r3992669600) |
| [3](https://github.com/linkedin/kafka/pull/551#discussion_r3925877442) | Close cumulative counters when their associated ingress metric is removed. | BrokerTopicMetricsTest.testCloseMetricClosesCumulativeIngressCounters; moved to the storage-metrics split. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984229507) |
| [4](https://github.com/linkedin/kafka/pull/551#discussion_r3926413418) | Reject negative and non-increasing boundaries. | KafkaConfigTest.testRequestMetricBucketsMustBeOrderedAndNonNegative. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984229700) |
| [5](https://github.com/linkedin/kafka/pull/551#discussion_r3926413465) | Assert on the specific metric names, not global registry size. | BrokerTopicMetricsTest uses per-metric presence checks; storage split tests pass. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984229872) |
| [6](https://github.com/linkedin/kafka/pull/551#discussion_r3927365059) | Fall back to a non-atomic move when the filesystem rejects ATOMIC_MOVE. | PoisonPill; separate-process heap-dump test in PR 561. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984230077) |
| [7](https://github.com/linkedin/kafka/pull/551#discussion_r3927365100) | Synchronize close and clear the metrics map. | RequestChannel.Metrics.close uses the same monitor as apply. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984230263) |
| [8](https://github.com/linkedin/kafka/pull/551#discussion_r3927365140) | Prefix the watchdog histogram with the request-channel metric prefix. | RequestChannel and RequestChannelWatchdogTest. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984230448) |
| [9](https://github.com/linkedin/kafka/pull/551#discussion_r3927365177) | Derive the watchdog check interval from the configured timeout. | KafkaServerTest.testRequestChannelWatchdogIntervalTracksConfiguredTimeout. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984230674) |
| [10](https://github.com/linkedin/kafka/pull/551#discussion_r3927365245) | Remove the hard-coded count. Check uniqueness and compare the actual registry with the shared Python contract. | LiProtocolBridgeConfigTest; preflight/scenario registry-consistency tests; 24 gates are currently registered. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984230900) |
| [11](https://github.com/linkedin/kafka/pull/551#discussion_r3927475761) | Cache the total topic-name length and update it with topic membership. | ControllerContextTest.testTopicNameLengthTotalTracksTopicChanges; controller gauge reads the cached total. | [reply](https://github.com/linkedin/kafka/pull/551#discussion_r3984231122) |

## PR 552

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/552#discussion_r3926398457) | Test ZooKeeper state parsing and issue generation. | test_inspect_zookeeper_parses_state and test_inspect_zookeeper_reports_inventory_and_corruption. | [reply](https://github.com/linkedin/kafka/pull/552#discussion_r3984231299) |
| [2](https://github.com/linkedin/kafka/pull/552#discussion_r3927340912) | Split native control at IBP 3.0 from final IBP 3.9. Test each phase instead of requiring a premature IBP bump. | test_native_phase_rejects_premature_ibp_change and test_all_phase_and_retained_gate_contracts. | [reply](https://github.com/linkedin/kafka/pull/552#discussion_r3984231483) |
| [3](https://github.com/linkedin/kafka/pull/552#discussion_r3927416052) | Require IBP 3.0 in mixed mode and assert the current message. | test_mixed_config_rejects_new_ibp_and_unsafe_features. | [reply](https://github.com/linkedin/kafka/pull/552#discussion_r3984231717) |
| [4](https://github.com/linkedin/kafka/pull/552#discussion_r3927470913) | Check the dynamic import specification and loader and report a clear error. | li_bridge_preflight_test.py import guard. | [reply](https://github.com/linkedin/kafka/pull/552#discussion_r3984231952) |
| [5](https://github.com/linkedin/kafka/pull/552#discussion_r3927470956) | Test corruption, ID mismatch and deletion-flag values, including the normal null/empty znode. | ZooKeeper parsing tests and test_deletion_flag_value_is_retained_and_invalid_data_is_rejected. | [reply](https://github.com/linkedin/kafka/pull/552#discussion_r3984232154) |

## PR 554

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/554#discussion_r3926405123) | Reject missing transitions and malformed summaries. Also report invalid UTF-8 as an audit issue. | test_missing_process_transition_is_rejected; test_malformed_verification_summary_is_rejected; test_invalid_utf8_is_an_issue_not_an_auditor_crash. These now belong to the preceding evidence-audit PR. | [reply](https://github.com/linkedin/kafka/pull/554#discussion_r3984232332) |

## PR 558

| Comment | Decision | Code/test evidence | Reply |
|---|---|---|---|
| [1](https://github.com/linkedin/kafka/pull/558#discussion_r3962628747) | Exercise release-note API arguments and returned content with a gh stub. | li_release_test.py fixture suite; six cases pass on both release lines. | [reply](https://github.com/linkedin/kafka/pull/558#discussion_r3984232512) |
| [2](https://github.com/linkedin/kafka/pull/558#discussion_r3962628803) | Build the archive name from the supplied Scala version. | li_release.py archive handling and Scala 2.13 fixture. | [reply](https://github.com/linkedin/kafka/pull/558#discussion_r3984232707) |
| [3](https://github.com/linkedin/kafka/pull/558#discussion_r3962628838) | Build the notes text from the supplied Scala version. | li_release.py notes handling and fixture assertions. | [reply](https://github.com/linkedin/kafka/pull/558#discussion_r3984232915) |
