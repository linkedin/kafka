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

# Review: 3.0-li to 3.9-li rolling upgrade

## Current verdict

**Do not deploy without approved published-artifact qualification and the release approvals.** A complete scenario-6 full-verifier run now passes on f919812ba4 / 8086d17968 / wrapper 1764cc95, followed by an independent strict clean/full/archive audit. The test-only gate follow-up adds separately verified assertions against unchanged runtime code. F28's existing client-bootstrap limitation and deployed client-floor decision remain open. The inventory covers 49 open PRs. Earlier passing or failed bundles retain their original source and coverage limits.

The findings below record what the initial review and later tests found. Instructions in an original finding describe the repair that was needed; use the current disposition and evidence sections for status. This is not a line-by-line approval of every Kafka change.

### Reviewed revisions

- Workspace plan: `LI-3.0-TO-3.9-ROLLING-UPGRADE-PLAN.md`. Canonical runbook: `docs/ops/li-bridge-upgrade.md`.
- Current 3.9 implementation and qualification: PR 599, `3.9-li-bridge/request-gate-regressions`, including F25, F27 and mandatory scenario-6 checks.
- Complete local qualification: runtime source `f919812ba4255f07412aefcfb8d59246174dc266`. Later changes are documentation, tests and test selection, not runtime code.
- Current 3.0 source: PR 596, `8086d1796816bd2383844b7b9f9b05ebd8b33c9b`.
- CI: PR 558, `3e799b08ea`; PR 559, `a86214e2da`.
- Wrapper: `1764cc95bfa21e19d3ff89e0164e5896808b5507`, including the diagnostic opt-in and the earlier ACL test fix.

GitHub heads, bases, labels and sizes were read back after publication. The upgrade stacks rooted at PRs 543 and 575 retain separate release histories. CI foundations have no upgrade label. PRs 563/564 were automatically closed during the reorder; replacement reviews 579/578 preserve those scopes. No release branch was merged by this work. Original findings began from aggregate `a658c512df`; aggregate PR 542 and old docs PR 555 remain closed.

## Findings

### F1 — P1: Activation does not provide the promised bounded merging cutoff

**Owner:** PR 541. **Plan:** lines 71 and 223–239.

In the 3.0 `core/src/main/scala/kafka/controller/ControllerChannelManager.scala:350–425`, `nextRequestAndCallback` snapshots bridge mode before it can block in `queue.take()`. Neither the subsequent dequeue/merge nor the queue-draining loop rechecks the setting. Consequently, work enqueued after activation can still enter `LiCombinedControl`. A continuously replenished queue can also extend the merge loop after activation; it is not bounded to work already removed and merged.

A deterministic review-only reproduction against the local compiled 3.0 artifact blocked the sender on an empty queue, enabled bridge mode, then enqueued one UpdateMetadata request. It returned:

```text
bridge=true, requests dequeued and merged AFTER activation=1, selected=Builder, api=LI_COMBINED_CONTROL
```

The existing `testProtocolBridgeModeStopsNewLiCombinedControlRequests` compares counts after two post-activation batches. It allows the first batch to merge and does not test activation while blocked or continuously draining.

**Required change:** enforce a cutoff when deciding whether a dequeued item can be merged, and recheck activation while draining without losing or duplicating callbacks. Keep the mandatory controller restart before 3.9 admission; fixing the race is not a replacement for that fence. Alternatively, explicitly redesign the operational contract around an unconditional restart rather than claiming a bounded pre-restart drain.

**Acceptance tests:** latch-controlled activation during a blocked dequeue and during sustained queue replenishment; assert no newly accepted work enters the merger after the defined cutoff, every callback has the correct lifecycle, and controller replacement completes while mutations continue.

### F2 — P1: Preflight rejects the production 3.0 configuration, and the mixed test avoids it

**Owners:** PRs 552 and 553. **Plan:** lines 126–127, 138–151, and 587.

`tests/bin/li_bridge_preflight.py:179–184` rejects `li.async.fetcher.enable=true` and `li.combined.control.request.enable=true` for **both** generations. Both settings are enabled in the documented 3.0 production configuration. Bridge mode suppresses new combined sends without requiring the original combined-control setting to change, and the plan deliberately leaves the 3.0 fetcher unchanged.

Calling the current preflight inspector with a bridge-enabled, IBP-3.0 legacy configuration reproduces both errors:

```text
legacy.properties: unsupported 3.9 behavior li.combined.control.request.enable is enabled
legacy.properties: unsupported 3.9 behavior li.async.fetcher.enable is enabled
```

The mixed-process harness omits both properties from its legacy broker configuration. They default to false in 3.0 `KafkaConfig`, so its recovery tests do not exercise production's LI async fetcher. This is a functional coverage gap, not just the already-documented throughput qualification gap.

**Required change:** make rejection generation-specific. Preserve the old fetcher and combined-control settings on legacy brokers, while requiring effective bridge mode and verifying the actual combined-send cutoff. Reject these settings on 3.9 as intended.

**Acceptance tests:** a production-shaped legacy config passes; the equivalent 3.9 config fails; mixed recovery runs with the old async fetcher enabled in both leader directions; dormant-to-active bridge testing starts with combined control enabled.

### F3 — P1: Qualify rollback before promising it for the first canary

**Owners:** PR 553 or a dedicated rollback qualification PR; deployment owner. **Plan:** lines 353 and 371–384.

The canary instructions unconditionally say to replace a failed 3.9 broker with the 3.0 bridge. Only later does the plan condition rollback on persisted-state compatibility. The process harness upgrades the final old broker on its existing log directory, but never runs the reverse replacement.

Wire compatibility does not prove that 3.0 can reopen state written by a 3.9 leader, group/transaction coordinator, controller, or plugin. This condition applies to the first canary as well as the all-3.9 bake.

**Required change:** make successful persisted-state rollback qualification an entry gate before any production 3.9 admission, or document a tested forward-recovery alternative instead of promising binary rollback.

**Acceptance tests:** 3.0 → 3.9 → 3.0 on the same log directories and ZooKeeper state, with acknowledged data, committed offsets, transaction/coordinator state, topic IDs, dynamic configs, and retained plugin state. Exercise both a canary rollback and rollback from an all-3.9 bridge-mode cluster. Verify exact records and resumed application progress, not only successful registration.

### F4 — P1: The unchanged-client matrix is not implemented through the final transitions

**Owners:** PR 553; internal wrapper/tool qualification. **Plan:** lines 348 and 486–497.

The harness runs short-lived old producer/consumer, transaction/group, Streams, and Connect cases during the mixed phase. Later produce/consume phases use `KAFKA_39_HOME`, including native mode and IBP 3.9 (`tests/bin/li_bridge_mixed_cluster_smoke.sh:726,761,786`). No old workload remains alive across the complete transition sequence.

The private-API helper is also compiled and executed against **3.9** jars (`:345–349,610–611`), despite PR 553's description saying its private-API traffic comes from the 3.0 archive. That proves the restored new Admin surface, not unchanged deployed `ktool` or LI AdminClient behavior.

**Required change:** keep supported old client artifacts running across broker replacements, controller changes, bridge deactivation, and IBP changes. Run the private-API helper with old jars as well as new jars, and qualify the actual operational tools. Define the supported deployed client floor rather than equating it with one 3.0 archive.

**Acceptance tests:** the six phases listed in the plan have results for old producer, consumer, transactional client, Admin/tool, Streams, and Connect artifacts. Include coordinator movement, reconnect/rebalance, transaction commit/abort/fencing, and existing group-offset recovery. No client binary/configuration changes are allowed between phases.

### F5 — P1 release gate: Pagination is tested with vendor jars, not established for the deployed runtime

**Owners:** PRs 549 and 564; wrapper/deployment dependency PR. **Plan:** lines 129 and 526.

The pagination implementation invokes `ZooKeeper.getAllChildrenPaginated` reflectively (`core/src/main/scala/kafka/zookeeper/ZooKeeperClient.scala:206–234`). A stock client lacking that method produces `SYSTEMERROR`; there is deliberately no silent fallback.

PR 564 correctly qualifies the adapter with isolated, hash-pinned LinkedIn ZooKeeper 3.6.3-23 jars. Its normal Kafka runtime remains stock ZooKeeper 3.8.4. Meanwhile, the wrapper's `config/app/kafka-war/application.src:29` enables pagination. The test-only dependency change does not establish which client implementation the final wrapper package will actually load, or compatibility with the live server ensemble.

**Required change:** identify and pin the actual deployed client/Jute/server combination in the release/deployment contract. Add a startup/preflight capability check and qualification using the packaged wrapper runtime with pagination enabled. Do not disable pagination merely to make a large production cluster start.

**Acceptance tests:** inspect actual loaded class locations and hashes; launch the packaged broker/wrapper against the qualified server; enumerate topic, topic-config, and federated paths above the response limit; verify watches, authorization failures, reconnect/session expiry, and controller startup. If deployment already substitutes the vendor client, retain evidence of that substitution rather than assuming it.

### F6 — P2: The preflight state model contradicts the separate native/IBP steps

**Owners:** PRs 552 and 554. **Plan:** lines 396–419.

The current preflight's `native` phase requires IBP 3.9 (`tests/bin/li_bridge_preflight.py:148–151`). The runbook explicitly requires a native-control bake at IBP 3.0 before raising IBP. That legitimate intermediate state is rejected:

```text
new.properties: native phase requires IBP 3.9, found 3.0
```

Conversely, at IBP 3.9 the inspector returns no issues with **all** compatibility gates false, even with `require_all_gates=True`. Required-gate enforcement only runs in `mixed`. Native control does not retire `ktool`, federated APIs, authorization/placement behavior, or operational metrics.

**Required change:** model the real states explicitly: dormant 3.0, all-3.0 bridge, mixed bridge, all-3.9 bridge, native-at-old-IBP, and final IBP 3.9. Define retained gates independently of the outbound protocol gate. Make the evidence auditor consume the same phase expectations.

**Acceptance tests:** each allowed state passes, invalid transitions fail, the native-at-old-IBP state passes without changing IBP, and disabling a still-required retained feature fails in every applicable phase. Document which settings require a restart.

### F7 — P2: Green public CI does not include the mixed-process migration

**Owners:** PRs 553, 554, 558, 559, and internal CI. **Plan:** line 591.

The current `.github/workflows/li-ci.yml` runs bridge Python tests and shell syntax checks, plus Kafka suites and the isolated pagination test. It does not execute `li_bridge_mixed_cluster_smoke.sh` or the wrapper verifier. Therefore, all PR checks being green does not satisfy the plan's explicit mixed-process CI requirement.

The local evidence bundles are valuable and a retained supplied-artifact bundle passes the current strict auditor. They are not a repeatable CI gate for future protocol changes or the final published artifacts.

**Required change:** add an automated cross-artifact job using pinned independently built release inputs, and an internal wrapper/security job where private dependencies are available. Require these results for release qualification. Record which checks are per-PR gates versus slower release gates.

**Acceptance tests:** final merged/published archives are the actual process-test inputs; evidence and JUnit/process logs are retained; absence of a required archive or private test environment yields an explicit blocked result, never a successful partial qualification.

### F8 — P2: Strengthen the process test's behavioral assertions

**Owner:** PR 553. **Plan:** lines 368 and 472–484.

Several assertions are too weak for the stated safety claims:

- `consume_count` checks line count, not exact record identity, duplicates, or ordering. Old transaction consumption checks a prefix and count.
- `create_and_delete_topic` submits deletion but does not wait for metadata, ZooKeeper, and replica/log removal; it never verifies immediate recreation of the same name.
- Catch-up asserts ISR membership; it does not compare the recovered records on both replicas.
- The common process configuration sets `controlled.shutdown.enable=false`, and stop helpers use normal termination rather than hard failures. Other focused tests cover some shutdown behavior, but the mixed binary shutdown/crash contract is not established by this harness.

**Required change:** use a per-partition record ledger/checksum, acknowledged-write tracking, explicit deletion/recreation completion checks, and bounded crash/recovery cases. Keep intentional unclean-election data loss isolated from the no-loss workload. Add explicit process/command timeouts so a hang yields useful failure evidence.

**Acceptance tests:** deliberate dropped/duplicated records and incomplete deletion make the test fail; production-like controlled shutdown and hard controller/broker termination recover under active mutations.

### F9 — P2: The dormant-feature gate does not collect or validate all required inputs

**Owner:** PR 552 and live-discovery/deployment tooling. **Plan:** lines 113–119, 151, 578, and 588–589.

The preflight validates supplied properties and selected ZooKeeper paths. It does not collect effective dynamic broker/topic configuration or inspect remote-storage metadata. It also does not reject `li.leader.election.on.corruption.wait.ms > 0`, even though delayed corrupt elections are explicitly not ported. A config with all bridge gates true and that setting at 1000 currently produces no issues.

An empty `/brokers/corrupted` at one instant is not proof that a legacy broker with recovery enabled cannot create new state during the roll. A broker-level remote-storage boolean alone does not establish the disposition of previously written remote/topic/plugin state.

**Required change:** give the live configuration/state collector an owner and a versioned evidence format. Validate delayed-election settings on legacy brokers, dynamic topic/broker overrides, remote metadata/state disposition, and collection completeness/freshness. Distinguish offline config lint from a live admission gate. Require live inputs for production approval.

**Acceptance tests:** dangerous static/dynamic overrides and enabled delayed election fail; missing live evidence is blocked; the report includes source, collection time, cluster identity, and disposition for every required persistent path.

### F10 — P2: Resume does not fingerprint workload qualification parameters

**Owners:** PRs 554 and 563.

`verification_fingerprints` in `tests/bin/audit_li_bridge_evidence.py:92–103` covers source, archives, staging mode, and build version, but not `SCALE_TOPIC_COUNT`, `SCALE_PARTITION_COUNT`, `RECOVERY_RECORD_COUNT`, or `RECOVERY_RECORD_SIZE`. Changing these on a resumed invocation can reuse the previous successful `mixed-process` stage instead of running the requested larger workload.

The old workload parameters remain visible in nested evidence, so this is not evidence that results were fabricated. It is nevertheless an unsafe mismatch between requested verification and reused verification.

**Required change:** fingerprint a normalized scenario specification, including effective defaults, and validate its match to the process summary. Rerun affected stages or refuse resume when the scenario changes.

**Acceptance tests:** increasing metadata/recovery scale or changing the required runtime profile invalidates reuse; identical scenarios still resume successfully.

### F11 — P2: Make the reviewed plan and merge/release dependencies authoritative

**Owner:** a replacement documentation PR; Kafka/wrapper release owners.

The requested plan still describes the aggregate implementation and has no inventory of the current split PRs. The open stack does not track this plan; the old docs PR 555 is closed. Later local working notes are useful but are not a substitute for a reviewed runbook attached to the actual release line.

The wrapper's official dependency update and security review also remain separate dependencies. A published branch and local compilation are not an approved wrapper release. In particular, `LiKafkaAuthorizerV2.authorizeByResourceType` changes producer-ID authorization behavior and needs an explicit security-owner decision plus positive/negative tests with production authentication.

**Required change:** publish a single reviewed runbook with the PR inventory below, exact release/wrapper provenance, merge order, owners, and evidence links. Separate historical experiment notes from current acceptance criteria. Do not describe green CI or a branch push as approval/publication.

**Acceptance tests/checks:** the merged release references the runbook; artifact versions/checksums map to approved commits; the wrapper resolves official main/test classifiers; required security and operational approvals are recorded; rollback/admission automation cannot start a non-bridge old binary or an ordinary 3.9 binary during the mixed phase.

### F12 — P1: The old offline-deletion shortcut needs a bridge-mode replacement

**Owner:** PR 541. This was found while testing the fixes above.

The continuous metadata test stopped making progress after a 3.9-to-3.0 controller change. The old controller removed the topic's ZooKeeper entry while its only replica was offline. It skipped the metadata deletion update. The running brokers kept a ghost topic: CreateTopics reported that it existed, while DeleteTopics reported that it did not exist.

The failure is in the 3.0 `TopicDeletionManager.resumeDeletions` shortcut that counts `OfflineReplica` as successful deletion. That shortcut relies on native full-state control messages, which the bridge cannot send.

The new regression `TopicDeletionManagerTest.testBridgeDeletionWaitsForOfflineReplicaAfterControllerFailover` failed before the fix: ZooKeeper deletion ran without a metadata update. The bridge now uses the older Kafka behavior: send the metadata deletion update, retain offline replicas for retry when they return, and wait for their deletion acknowledgements. The native 3.0 path is unchanged. The full deletion-manager suite and activation regressions pass. A complete mixed-process rerun is still required before closing this finding.

### F13 — P1: A recreated topic inherits stale deletion state

**Owners:** PR 541 and the 3.9 controller fixes after PR 549. This was found in the next process run.

The 3.9 controller finished deleting a topic while a reassignment had marked it ineligible for deletion. `ControllerContext.removeTopic` cleared the other deletion sets but left `topicsIneligibleForDeletion` unchanged. Recreating the name then made every DeleteTopics request time out. The controller thread was idle, not deadlocked; the stale marker prevented it from starting deletion.

The same missing cleanup exists in 3.0. The first regression failed before the fix. To keep every upgrade behavior opt-in, cleanup now runs in `TopicDeletionManager` only when `li.protocol.bridge.topic.deletion.state.cleanup.enable` is true. The flag defaults false, is cluster-wide and dynamic, and has the same gauge on both versions. The wrapper enables it explicitly and keeps it enabled after protocol bridge mode is disabled. `testTopicNameReuseCleanupRequiresItsFlag` passes with both flag settings on both versions. The final process rerun is still required.

### F14 — P2: The wrapper ACL test verifies an unfinished async read

**Owner:** the wrapper test update.

The full wrapper run failed in `KafkaAclManagerTest.testAccessDenyWithFederatedAcls`. Initialization reads the topic list once on the calling thread and once on a background reader. The test expected two calls but verified the mock without waiting for the second call. It also left background readers running when teardown reset their mocks.

The test now waits on a latch with a ten-second limit and closes both readers before resetting mocks. It still requires exactly two reads; no assertion was removed. All 18 tests in the class passed in five fresh runs. This changes tests only, not authorization policy. The complete wrapper suite is part of the new full verification run.

### F15 — P1: Controller failover can leave a ghost metadata-cache entry

**Owners:** the 3.0 and 3.9 controller follow-ups.

A controller can remove a topic's ZooKeeper entry, then stop before every broker receives the metadata deletion update. The next controller no longer knows that it must send that deletion. The incremental metadata cache keeps the topic, so a later create/delete loop stalls.

With `li.protocol.bridge.topic.deletion.state.cleanup.enable` enabled, each new ZooKeeper controller sends a full first partition image. Brokers replace their cached partition and topic-ID maps only on the first update of a newer controller epoch. Same-epoch updates stay incremental, including after dynamic activation. The flag-off path is unchanged. Enable the setting on every broker before restarting the controller; it is a shared protocol assumption, not a receiver-only cleanup switch.

`BridgeMetadataCacheEpochTest` and the existing metadata-cache suites pass on both generations. The next process run passed all sixteen phase checkpoints, including active deletion/recreation with records, but its final log audit found F16. It is not a qualifying pass.

### F16 — P1: An ISR retry loses fields when the controller version changes

**Owner:** the 3.9 broker retry follow-up.

`AlterPartitionRequest.Builder` changes its stored data when building an older request version. A queued request built for a 3.0 controller can then reach a 3.9 controller after failover. Building version 3 from that changed data fails with `UnsupportedVersionException: Attempted to write a non-default newIsr at version 3`. The final process-log audit caught this even though ISR recovery eventually completed.

In bridge mode, the broker now builds each retry from a fresh copy. The dynamic flag is checked when the request is built, not only when it is queued. The existing JVM constructor and flag-off builder behavior stay intact. The new test covers versions 3, 1, 3, 1, 3 on the same queued builder and activates the flag after queueing. That activation case failed with the first, allocation-time-only check. The corrected build-time check and the existing ISR-manager suite pass. The clean-source complete process run and evidence audit now pass. The earlier failed runs remain recorded.

### F17 — P1: Reassignment can leave an unhosted log behind after restart

**Owners:** the 3.0 and 3.9 stray-log follow-ups.

A short mixed-controller diagnostic failed the empty-log check after recreating a topic. The retained segment contains `cycle-5`, written before deletion. Broker 0 loaded that segment during restart, received the metadata deletion update, but never removed the unhosted local log. The next create reused its directory. This is not a late producer write: the log was loaded with end offset 1 before the deletion and no new write was sent before the failed check.

With the cleanup flag enabled, `ReplicaManager.maybeUpdateMetadataCache` now retires local logs that have no hosted replica when metadata reports their deletion. Hosted replicas still wait for `StopReplica`; unrelated logs and stale-controller requests do not trigger this cleanup. The 3.9 path excludes KRaft-controller updates. This is a targeted repair, not a claim that every offline name-reuse case has been qualified.

`BridgeStrayLogDeletionTest` failed on the old 3.0 implementation and passes with the fix on both versions. It covers both flag settings, stale controller fencing, hosted and unrelated logs, and empty-log recreation. The complete clean-source migration and process audit also pass with this repair and without diagnostic logging. Broader offline name-reuse cases still require qualification.

### F18 — P1: An offline former replica misses every deletion notification

A former replica was kept offline while its assignment was removed and its topic was deleted and recreated. After the broker returned, the recreated topic was reassigned to it. ISR recovery completed, but promoting the old broker returned the old 64-byte record at offset 0 instead of the new 128-byte record. The retained log contains that stale first record and a new second record; checking offsets alone would miss the corruption.

PRs 583 (3.0) and 584 (3.9) extend the existing default-off cleanup gate. A complete initial metadata image retires unhosted logs that are no longer assigned to the broker. Leaderless assignments are retained. Same-epoch incremental updates, including those received after dynamic activation, do not scan all logs. Existing JVM methods are preserved.

The targeted test failed before the fix and passes after it, including promotion and exact record comparison. Scenario revision 3 added both broker generations and mandatory evidence entries. It does not cover F19 below; revision 4 is now required. The expanded full verifier stopped at the private wrapper dependency refresh, which requires internal network/VPN access.

### F19 — P1: Assignment does not identify a recreated topic generation

The next test recreated the topic with an explicit assignment that already included the offline former replica. The full image therefore retained that broker's old log. The broker returned to ISR, was promoted, and served the old record. This is confirmed in `/tmp/li-assigned-offline-name-reuse-2`, not inferred from source alone.

Bridge `LeaderAndIsr` v2 and `UpdateMetadata` v5 omit topic IDs. Assignment, offsets and leader epoch cannot establish a log's generation. PRs 585/586 read existing ZooKeeper topic IDs on the control path, persist identity before a new bridge log accepts records, and retire only known mismatched generations. The existing default-off cleanup gate controls this behavior and requires an IBP with topic IDs. A nonempty log without verifiable identity is retained and rejected for automatic reuse; it needs an operator recovery decision.

Current/future log copies are checked together because the deletion API retires both. Conflicting copies remain for recovery. A failed full-image identity check stays pending after cache publication; retrying the same update cannot bypass it. A genuinely deleted topic gets a per-partition unknown-topic response and leaves the queued control batch without blocking unrelated partitions. An existing topic without an ID is not treated as deleted. ZooKeeper errors still propagate.

Both complete `ReplicaManagerTest` suites and the dedicated identity tests pass. Coverage includes disabled behavior, unknown/zero IDs, wire-ID disagreement, ZooKeeper failures, current/future conflicts, retrying the same full image, and mixed deleted/live control batches. Scenario revision 4 requires both returning generations and both assignment timings, with exact records after promotion. All four checks passed in `/tmp/li-scenario-4-batch-fixed`; that scoped result is not by itself a complete migration or wrapper qualification. No failed evidence was relabelled.

### F20 — P2: Hourly log rotation removes protocol evidence

PR 584's process job reached the last IBP-3.9 checkpoint, then reported a missing 3.9 controller bridge-selection line. The shipped Log4j configuration rotates `controller.log` hourly, but the checker read only the current file and the evidence archive omitted rotated files. This could lose a selection line or hide an earlier protocol error.

PR 584 now scans and retains rotated logs; PR 586 is restacked on it. The new tests failed on the previous runner and pass with the fix. They cover both generation markers, missing-marker rejection, errors inside rotated logs, and retention in a failed-run archive. All 70 Python tests pass. This is a collector repair, not a Kafka behavior change or a waiver for the failed job.

### F21 — P2: Churn retry policy misses controller transitions and masks data errors

The revision-4 run stopped after the native client checkpoint when topic creation returned `ControllerMovedException`. Apache 3.9's unchanged ZooKeeper config-write fence returns this error if the controller disappears during creation. Both clients map response code 11 to a non-retriable `ApiException` subclass, so the helper stopped even though controller movement was intentional.

A focused test against both real client archives reproduces that failure. It also exposes a second gap: `KafkaStorageException` and `CorruptRecordException` inherit `RetriableException`, so the old broad policy could silently retry data errors. PR 584's helper now explicitly retries controller transitions but rejects storage and corrupt-record errors. Authorization, malformed requests/configuration, oversized records and unexpected failures remain fatal. Broker response codes and client libraries are unchanged.

The positive/negative classifier test passes against both archives and is run during process setup for both generations. All 71 Python tests pass, including a test that checks this setup wiring. Record comparisons, retry delays, phase-progress checks and deadlines are unchanged. The complete rerun now passes in `/tmp/li-scenario-4-churn-fixed`. The earlier failed run stays failed. Final qualification must also cover the later F22 change and matching wrapper.

### F22 — P2: Bridge-state MBeans register without an opt-in

Both generations constructed `LiProtocolBridgeMetrics` and registered new MBeans even with bridge behavior flags disabled. Diagnostics are not exempt from the requirement that upgrade behavior be config-gated.

PRs 588/589 add `li.protocol.bridge.config.metrics.enable`, default false, ZooKeeper-only and restart-scoped. Enabled diagnostics still report disabled behavior flags without activating them. The existing constructors remain available. A disabled instance does not remove an enabled instance's gauges during cleanup.

The default-off test failed on both previous implementations and passes with the repair. Tests also cover enabled readings, dynamic behavior flags, ignored live updates to the restart-only setting, KRaft exclusion and cleanup. The selected 3.0 and 3.9 suites passed 22 and 39 tests respectively. All 72 Python tests pass, including missing/false opt-in rejection in every migration phase. The process profile enables diagnostics explicitly. The wrapper mapping and its new negative test pass with matching staged jars: 133 tests, no failures or skips, unchanged main/test-classifier hashes. This scoped pass does not replace the complete verifier.

### F23 — P2: Static quota fallback changes a flag-off native limit

`ClientQuotaManager.getMaxValueInQuotaWindow` treated any limit at or above `Long.MaxValue` as absent. That also changed an explicit dynamic quota when the bridge static-default flag was off. A new regression expected the native finite window limit `9.223372036854776E19`, but got `Double.MaxValue`.

PR 590 restores the original callback calculation and uses the gated static default only when the callback returns no limit. The large explicit-quota and fallback-removal regressions pass with the full `ClientQuotaManagerTest`, `QuotaFactoryTest` and `RequestQuotaTest` suites. No default is enabled by this repair.

### Review-record correction — empty metric buckets are rejected

The original reply and ledger for PR 551 comment 2 incorrectly said empty buckets were supported. The published parser and `testEmptyRequestMetricBuckets` reject them with `ConfigException`, as the reviewer requested. The [correction](https://github.com/linkedin/kafka/pull/551#discussion_r3992669600) is explicit; the original reply remains in the history. No code was changed to match the inaccurate reply.

### F24 — P1: Native backout deletes an offline topic without a metadata tombstone

The failed full run elected controller 1 while the churn topic's only replica was offline. At 12:30:23, deletion completed without starting the metadata/replica-deletion path. With bridge mode off, 3.0 counted `OfflineReplica` as deleted even though the cleanup gate remained enabled. ZooKeeper lost the assignment while the online broker retained the topic in its cache.

PR 592 requires real replica acknowledgements while either bridge mode or the cleanup gate is enabled. Both flags off preserve the old native path. The four-combination unit regression failed before the fix and passes afterward; the deletion, controller-context and metadata-cache suites pass 18 tests.

A real-broker probe failed before: cached topic present, assignment absent. With the repair it passes: cache cleared, assignment retained until replica return, and replacement records verified after recreation. The actual runner method also passes these checks. PR 593 makes them mandatory in scenario revision 5 and rejects revision-4 or incomplete evidence. All 82 Python tests pass. These targets deliberately remain incomplete migrations, not full qualification passes.

### F25 — P2: Remaining default-off paths need explicit guards

The raw `AdminZkClient.createTopic` and `addPartitions` utilities excluded preferred-controller IDs without consulting their optional KafkaConfig. A regression failed with replication factor 3 but only two available brokers, although no preferred-controller flag was supplied. PR 594 requires the existing gate for that exclusion. Maintenance config still applies independently, and explicit manual assignments may include preferred brokers. The normal broker API was already gated and is not the failing path.

The audit also found three added `KafkaController` diagnostic gauges registered while the diagnostics opt-in was false. They now use the existing `config.metrics` gate, including conditional cleanup. Native controller metrics remain unchanged. The before regression observed the unwanted registration; both enabled/disabled cases pass after the fix.

Finally, the shared disabled produce-instrumentation object retained partition references and could be read by a logger activated after the request started. It now ignores partition setters, and the logger skips uncollected requests. The getter/setter JVM methods remain. Before tests caught retained state and request access after activation; enabled stage/partition tests and both disabled-path tests pass after repair.

The affected suites pass on Scala 2.12 and 2.13; all 82 Python tests pass. The focused verifier now includes the complete raw-admin suite and the existing replica-timeout/offsets-topic configuration tests. `docs/ops/li-bridge-gate-audit.md` maps the reviewed runtime boundaries to configuration and tests. Final-source qualification must cover these fixes rather than reuse a predecessor run.

### F26 — P1: Merged non-delete responses can poison deletion state

PR 593's failed CI run selected the repaired 3.0 archive and passed the native offline-deletion test, then stalled during later churn. Unlike F24, its topic assignment still existed. The controller treated a `FENCED_LEADER_EPOCH` response as a deletion failure, reset the replica to `OfflineReplica`, then rejected the subsequent successful deletion acknowledgements.

The 3.0 merger keeps one callback per request type while queued partition states may produce several responses. The StopReplica callback ORed its captured delete predicate with the response bit. A later delete request's predicate could therefore misclassify an earlier non-delete response. PR 595 uses the authoritative native-v4 response delete bit under the cleanup gate. Older wire versions keep the request-predicate fallback; cleanup disabled keeps existing behavior.

The four-configuration regression fails before the repair and passes after. The later native startup test exposed another boundary: direct StopReplica responses did not set the v4 deletion bit. PR 596 now echoes actual request intent under cleanup, including deletion failures. Older wire versions and cleanup-off behavior remain unchanged. The new handler test failed before and passes after a real v4 serialization round trip. The complete selected 3.0 suites pass 171 tests with no failures/errors and one existing disabled `testAlterReplicaLogDirs` case. Actual RAT and the archive build pass. The failed evidence remains failed.

### F27 — P1: Interrupted deletion repeatedly breaks controller startup

`/tmp/li-final-default-off-full` failed while producing recovery records 200–399. The controller repeatedly tried to resume a reassignment whose topic parent remained but whose leader/ISR znode had been removed during deletion. The deletion marker was still present. RPCProducerIdManager logged repeated AllocateProducerIds timeouts during the failed producer command.

PRs 596/597 recover only marked deletions with missing leader state, under cleanup and while deletion is enabled. They retain every replica, persist the plain assignment with controller fencing, and remove only selected reassignment entries. Unmarked topics, partitions with leader state and disabled paths remain unchanged. Ordinary replica acknowledgements still control deletion.

Real-broker probes failed on both generations before the repair. The maintained mode-off prelude also caught the direct-response gap described in F26. With both repairs it finishes deletion, recreates the name and verifies exact old-client records on both generations. The focused 3.9 suites pass on Scala 2.12 and 2.13. Scenario revision 6 makes both generations' deletion and record rows mandatory; negative fixtures reject revision 5 or any missing row. All 84 Python tests pass. Scoped bundles deliberately keep their complete-migration status false.

### F28 — Open: Old-client bootstrap waits need qualification

PR 594's newer CI used the F26-fixed 3.0 archive but failed a different check: a newly started old producer could not obtain metadata for a recreated topic within 60 seconds. The controller stayed active, the new replica became leader at offset 0, and the broker logged adding the partition to its metadata cache. Do not attribute this failure to F27.

Six diagnostic replays passed, but old-client logs exposed a 30-second interval before sending metadata despite an available bootstrap connection. The existing LI 3.0 client can enter bootstrap re-resolution after an initial failed update: its last successful refresh starts at zero. That path randomly selects a bootstrap entry without the normal ready/backoff selection. Probes against the unchanged client jar confirm that the default one-hour expiry selects this path after an initial failure. With a fixed valid random offset, it selects a disconnected node still in reconnect backoff despite a ready node. The existing zero setting selects the ready node. A second deterministic probe uses the actual DefaultMetadataUpdater: the default path sends no metadata request to the ready node for 60 seconds of simulated time; zero sends one immediately.

This is not yet a proven resolution of the 60-second CI failure. No client binary/profile or test deadline has been changed to hide it. The existing zero setting also passed a separate real-broker experiment: the unchanged client jar produced and verified recreated-topic records with each broker generation as the survivor while the other bootstrap broker was offline. This is not the migration profile or full qualification. Decide whether this existing setting may be a pre-upgrade client prerequisite, then record the deployed floor and owner decision before closing the finding. A config opt-out is not automatic approval to change deployed clients.

### F29 — P2: Verifier cleanup can stop unrelated Gradle builds

The verifier ran global `gradlew --stop` commands in both checkouts. Those commands address a shared daemon registry, not just processes owned by this verification. Every verifier/stager Gradle command already uses `--no-daemon`, so the global stops are unnecessary.

PR 598 removes them and their evidence-row requirement. A regression renders the actual full command plan, requires `--no-daemon`, and rejects `--stop`; it fails before and passes after. All 85 Python tests and actual RAT pass. No functional source, wrapper, archive, phase or record check is removed. This is a verifier-only change, not a broker/client behavior change.

### F30 — P2: Gate evidence did not cover every named boundary

Some gate-matrix entries cited helper or enabled-path tests rather than the actual disabled request boundary. PR 599 adds direct recommended-election, metadata-exclusion and follower-recovery request tests, including native behavior and authorization. It tests cancellation on a real controller with an offline original replica, nonzero minimum-roll configuration while the gate is off, and both transfer submission and native ISR shrink. Each of the 24 flags is now checked alone for dynamic scope, so one rejected property cannot hide another allowed override.

Three mutation runs deliberately broke seven guard boundaries; each failed the intended assertion. No mutation was published. Runtime files were restored and compared with the base. The restored suites pass 441 cases on Scala 2.12 and 39 scoped cases on Scala 2.13, with no failures/errors/skips; 85 Python tests and actual RAT pass. The verifier also selects the existing recommended-election decoder/eligibility tests and new cancellation test. Runtime code, client settings and deadlines are unchanged.

## PR dispositions and dependency audit

Every PR below has a distinct migration or CI purpose. Keep these scopes, but do not treat publication, a resolved thread or a green job as release approval. Publication does not mean that a layer is approved. The controller/ZooKeeper, security, storage and operational changes still need the corresponding owners' review.

| PR | Responsibility / reviewer |
|---|---|
| [559](https://github.com/linkedin/kafka/pull/559) | 3.0 CI/publication — release engineering |
| [575](https://github.com/linkedin/kafka/pull/575) | 3.0 wire fixtures — protocol |
| [541](https://github.com/linkedin/kafka/pull/541) | 3.0 bridge, activation and deletion recovery — controller |
| [577](https://github.com/linkedin/kafka/pull/577) | 3.0 unhosted-log cleanup — storage/controller |
| [583](https://github.com/linkedin/kafka/pull/583) | 3.0 complete-image log recovery — storage/controller |
| [585](https://github.com/linkedin/kafka/pull/585) | 3.0 topic-identity validation — storage/controller |
| [588](https://github.com/linkedin/kafka/pull/588) | 3.0 diagnostic-metrics opt-in — observability |
| [592](https://github.com/linkedin/kafka/pull/592) | 3.0 deletion acknowledgement fencing during native backout — controller |
| [595](https://github.com/linkedin/kafka/pull/595) | native StopReplica response classification under cleanup — controller |
| [596](https://github.com/linkedin/kafka/pull/596) | 3.0 interrupted deletion and direct native deletion responses — controller |
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
| [584](https://github.com/linkedin/kafka/pull/584) | 3.9 complete-image recovery, offline-reuse tests and log retention — storage/verification |
| [586](https://github.com/linkedin/kafka/pull/586) | 3.9 topic identity and scenario-revision-4 qualification — storage/verification |
| [587](https://github.com/linkedin/kafka/pull/587) | current findings, evidence limits and completion checklist — operations/review |
| [589](https://github.com/linkedin/kafka/pull/589) | 3.9 diagnostic-metrics opt-in and admission checks — observability/verification |
| [590](https://github.com/linkedin/kafka/pull/590) | native quota-window behavior and gated fallback regression — quotas |
| [591](https://github.com/linkedin/kafka/pull/591) | release-input negative tests and final audit records — verification/release |
| [593](https://github.com/linkedin/kafka/pull/593) | mandatory native offline-deletion checks, scenario revision 5 — verification |
| [594](https://github.com/linkedin/kafka/pull/594) | default-off placement, controller diagnostics and instrumentation audit — operations/verification |
| [597](https://github.com/linkedin/kafka/pull/597) | 3.9 interrupted deletion and mandatory scenario-revision-6 checks — controller/verification |
| [598](https://github.com/linkedin/kafka/pull/598) | verifier process isolation — verification |
| [599](https://github.com/linkedin/kafka/pull/599) | request, cancellation, storage and flag-scope assertions — verification |

Merge CI 558/559 into their own release branches first. Then retarget the upgrade stack bottoms as described in the runbook. Never force a Git dependency between the 3.0 and 3.9 CI branches. When reordering again, change PR bases before pushing a head that becomes an ancestor of its former base; GitHub can otherwise auto-close and delete that branch.

The oversized original scopes were split. Control wire definitions are in 565 (503 lines), handlers in 545 (845); storage metrics are in 566 (221), broker metrics/watchdog wiring in 551 (910); workload helpers precede runner 553 (754). The new evidence auditor, verifier and release gate are separate layers. All 49 PR diffs were below 1,000 changed lines at the latest readback. This documentation follow-up stays separate from the 863-line original runbook PR.

The original 62 review threads now have replies with published source decisions and code/test references. See `docs/ops/li-bridge-review-comments.md`. The static `LeaderTransferManager.noOp()` call is valid: javap confirms the forwarder, and the Java builder compiles. Empty, malformed, negative and unordered metric bucket lists are rejected. The earlier claim that empty lists were supported has been corrected against the actual source and test assertion. The latest readback covers 49 PRs and finds all 62 expected replies, no mismatches and no new review threads. The later F18/F19 issue comments are retained and have follow-up code and qualification records. Re-fetch after the final publication; comment status is not proof that the code is correct.

## Verification and remaining requirements

Passing a suite only covers what that suite asserts. These results are not substitutes for the remaining gates.

This prompt-to-artifact checklist separates observed results from open requirements. A manifest or a green job is not sufficient unless its assertions cover the named requirement.

| Requirement | Artifact and verification surface | Evidence / open work |
|---|---|---|
| Review the named plan and every open public PR | `LI-3.0-TO-3.9-ROLLING-UPGRADE-PLAN.md`; canonical runbook; GitHub inventory | 49 open PRs are listed in both tables. Recheck the exact PR set after any further publication. |
| Explain scope and dependencies | PR responsibility table, heads/bases, stack membership | Separate protocol, handlers, storage metrics, runner, auditor, verifier and release-gate layers. GitHub stack 582 has 38 upgrade PRs; stack 581 has nine. New members were appended and read back. CI 558/559 remain on independent release histories. No release branch was merged. |
| Apply the requested label | GitHub labels | All 47 upgrade PRs have `kafka-upgrade-august-2026`; CI 558/559 do not. |
| Keep PRs below 1,000 changed lines, preferably near 500 | Additions plus deletions, not file length | Established diffs passed the limit; recheck the new metrics follow-ups after publication. The largest established diffs are 981 and 963 lines. |
| Use plain, direct English | Plan, review, comment replies and workflow comments | Final wording/link review remains required. Historical findings are not current deployment instructions. |
| Gate every Kafka behavior change | `KafkaConfig`, `DynamicBrokerConfig`, runtime call sites, metrics, wrapper mapping | 24 default-off 3.9 gates. F18/F19 use the cleanup gate; ISR retry repair uses bridge mode; F22 separately gates diagnostic registration. Dedicated disabled/activation tests pass. The source audit confirms 24 false defaults and ZooKeeper-only Active getters; F30 adds direct boundary assertions and mutation evidence. The gate matrix records those assertions plus non-Boolean opt-ins. |
| Select symmetric v2/v5/v1 control | Schemas, controller selectors, wire fixtures and retained logs | Both generations have fixtures and real-process coverage. F20 makes rotated log checks fail closed too. Final-source process qualification remains required. |
| Fence activation and preserve callbacks | `RequestSendThreadBridgeTest` | Blocked dequeue, sustained queue and admitted-deletion callback tests pass. Controller restart remains mandatory. |
| Enforce all six phases and unchanged clients | `li_bridge_contract.py`, preflight, persistent client/Streams/Connect and private-API helpers | 85 Python tests pass. F28's cold-bootstrap qualification remains open. Native control stays at IBP 3.0 before the separate IBP roll. One 3.0 archive is not the deployed client/tool floor. |
| Prove persisted rollback and recovery | Process runner, record helper, timings and JUnit | The complete scenario-6 run covers canary/all-3.9 rollback, cancellation, crashes, truncation and exact promoted records on the current runtime. Registration or ISR alone is not proof. |
| Prove deletion and name reuse | `TopicDeletionManager`, `ZkMetadataCache`, `ReplicaManager`, `BridgeTopicIdentity` | Gated F12/F13/F15/F17/F18/F19 repairs have tests. Revision 6 retains all four post-promotion record checks and adds both generations' interrupted-deletion startup cases. Scoped before/after checks and the complete scenario-6 runtime qualification pass. They do not grant deployment approval. |
| Handle version-changing ISR retries | `BridgeAlterPartitionRetryTest` | One queued builder crosses versions 3/1 and activation without reusing mutated request data. Earlier failed logs remain failed evidence. |
| Collect real runtime/configuration/state | Live inventory, runtime probe and negative-input tests | Disposable-cluster collection passes. Production binaries, settings, state dispositions, owners and client/tool floor remain required inputs. |
| Qualify pagination with the loaded runtime | `KafkaZkClient`, five vendor-client tests and release runtime probe | Startup rejects an unsupported client. Vendor tests pass. The actual deployed client/Jute/server pairing remains a release gate. |
| Follow Google shell style, including comments | Four wrappers, all extracted workflow Bash blocks, ShellCheck, shfmt, syntax/length checks | The audit covers 54 changed-workflow blocks, four wrappers and nine Bash documentation examples. The broader example check found three continuation-indentation issues and one overlong line; formatting-only fixes preserve their assignment/argument tokens. All examples now pass ShellCheck, shfmt, syntax, no-tab and 80-column checks. Unmodified upstream Docker workflows are outside these PRs. |
| Preserve wrapper/API compatibility | Factory mapping tests, ACL tests, complete wrapper suite and jar comparison | The current 133-test suite passes with matching main jars, stable main/test-classifier hashes and unchanged source. Wrapper commit 1764cc95 is published. The required Mint refresh produced a fresh dependency spec; no TTL or artifact-identity bypass was used. |
| Qualify real archives and reject incomplete evidence | `verify_li_bridge.sh`, `audit_li_bridge_evidence.py`, `verify_li_bridge_release.sh` and negative fixtures | The f919812ba4 / 8086d17968 / 1764cc95 full run and independent strict audit pass. Earlier failures remain retained, including the F28 client-bootstrap failure. Eight new release-input tests cover missing inputs, malformed identifiers, mismatched/unknown archive metadata, dirty/wrong checkouts, forced full mode and rejection before launch. A real-archive identity-only check also passes; neither it nor the fixtures grant release approval. |
| Address every review comment with evidence | Comment ledger, source/test decisions, GraphQL readback | All original 62 replies verified across 49 PRs; no new review threads. Later issue findings have published fixes and explicit qualification limits. Re-fetch after final publication. |
| Keep the three documents consistent | Workspace files and `docs/ops/li-bridge-{upgrade,review,review-comments}.md` | This follow-up synchronizes the records. Verify relative links and actual PR/source/evidence state before calling the review complete. |
| Preserve side-task PR 2039 | ADU worktree, commit history and formatting checks | Rebased/pushed on master at `ae007420`; formatting is one separate commit and is idempotent. Functional patches remain unchanged. |

### Retained local evidence

- `/tmp/li-bridge-clean-final-process`: all sixteen checkpoints pass, source inputs unchanged, strict process audit has no issues or warnings. Diagnostic logging is absent from the archive.
- `/tmp/li-clean-30-build.log`: standalone clean Git checkout `cb85262cb1`, Java 11, targeted controller/cache/deletion tests and archive build pass. Embedded commit ID matches that checkout.
- `/tmp/li-final-stray-39.log`: clean restack source, targeted cache/ISR/stray-log and packaged-client tests, archive build pass. The linked-worktree archive embeds `commitId=unknown`; source checkout identity and archive hashes are retained. Final published qualification must verify source identity again.
- `/tmp/li-wrapper-current-verification`: 132 retained JUnit results and identical before/after artifact reports.
- `/tmp/li-stager-real.log`: the new Python stager builds and installs actual artifacts and writes its scoped checksum manifest.
- `/tmp/li-restack-python-tests.log`: 65 tests pass. Test-method AST comparison confirms that splitting files did not remove or change assertions.
- `/tmp/li-publication-pr-audit-result.json`: 32 PRs, no head/base/label/size issues.

Later evidence supersedes the inventory and coverage limits of those historical records:

- `/tmp/li-bridge-published-full`: complete verifier pass on clean `e9065c4156` and wrapper `1a9ecccf`; predates F18/F19.
- `/tmp/li-scenario-3-full`: stopped at the required private dependency refresh. This is blocked, not a process pass.
- `/tmp/li-identity-batch-{30,39}-build.log`: complete `ReplicaManagerTest`, identity/stray-log and real-ZooKeeper identity tests, plus clean archives, pass on `1a5d02403b` / `9aa7416212`. Scala 2.13 compilation also passes.
- `/tmp/li-scenario-4-process`: failed because a deleted churn topic blocked an unrelated identity-checked control batch; the later paired correction addresses that failure.
- `/tmp/li-scenario-4-batch-fixed`: all four offline-reuse record checks passed, but the complete run failed at the native checkpoint. The old metadata-churn helper exited on `ControllerMovedException` during controller movement. Its progress had advanced to 221 cycles. The unchanged upstream fence and helper retry gap are covered by F21; do not waive this failed run. The summary records `passed=false` and unchanged source. Rotated logs omitted by that older collector are retained separately in `/tmp/li-scenario-4-batch-fixed-rotated-logs.tgz`.
- `/tmp/li-log-rotation-before.log`: both new rotation regressions fail before F20. `/tmp/li-log-rotation-restacked.log`: all 70 Python tests pass afterward.
- `/tmp/li-churn-retry-before-{3.0,3.9}.log`: real client error classes expose the missing controller retry and incorrectly retryable storage/corruption errors. `/tmp/li-churn-retry-after-{3.0,3.9}.log` and `/tmp/li-churn-retry-restacked-python.log` pass with the repair.
- `/tmp/li-review-readback-result.json`: 49 PRs, 62 original threads, no missing/mismatched replies and no new threads at that readback.

The complete revision-4 process run `/tmp/li-scenario-4-churn-fixed` passed on clean `d8f255f8a4` / `1a5d02403b`, with all four name-reuse checks, unchanged source and an issue-free process audit. It includes F20/F21 but predates F22. The real `mint --no-metrics dependency create-dependency-spec --detect-variant --overwrite` command has now refreshed the wrapper metadata successfully. An invocation without `--overwrite` returned success without refreshing the expired file; that no-op was not accepted as freshness evidence.

`/tmp/li-wrapper-metrics-qualification` retains the 133-test wrapper result and matching before/after artifact reports. `/tmp/li-release-input-regressions.log` records 80 passing Python tests; `/tmp/li-release-input-real-check.json` is explicitly identity-validation-only. `/tmp/li-native-quota-before.log` preserves the flag-off failure; `/tmp/li-native-quota-after.log` passes after the repair.

`/tmp/li-final-metrics-full` reached the process stage after all required earlier stages passed, then failed during the all-3.0 dormant backout. The ordinary old-client checkpoint completed, but metadata churn stayed at cycle 20: topic creation returned TopicExists while deletion reported a missing ZooKeeper topic. The source was unchanged. This stays a failed qualification, not a deadline to waive. F24 identifies and repairs the skipped tombstone/acknowledgement path.

`/tmp/li-native-offline-deletion-before/target-result.json` retains the real-broker failure. The corresponding fixed target and `/tmp/li-native-deletion-runner-target/target-result.json` pass, while their complete-migration statuses remain false. `/tmp/li-native-deletion-after.log` passes the 18 focused controller/cache tests. `/tmp/li-native-deletion-tools-publish-tests.log` passes 82 Python tests.

`/tmp/li-native-backout-full` passed all full-verifier stages on clean 6ea4d367d2 / 0e15a67639 / wrapper 1764cc95, including scenario revision 5 and an issue-free final audit. It predates F25/F26. `/tmp/li-final-default-off-full` covers F25 but uses the pre-F26 3.0 archive; it failed recovery production and exposed F27. The CI failure at `/tmp/li-ci-593-small-evidence` exposed the response-classification gap despite that local pass.

`/tmp/li-f27-native-echo-fixed` passes the maintained interrupted-deletion cases for both generations on 8086d17968 / c1091c6d27. Its summary remains an incomplete migration. `/tmp/li-f27-contract-before.log` retains six expected negative-fixture failures; `/tmp/li-f27-contract-after.log` passes all 84 tests. The latest PR 594 failed artifact is `/tmp/li-ci-594-f26-small-evidence`; `/tmp/li-reuse-metadata-diagnostic`, `/tmp/li-bootstrap-expiry-probe.log` and `/tmp/li-bootstrap-selection-probe.log` and `/tmp/li-bootstrap-progress-probe.log` record the bounded F28 investigation. `/tmp/li-expiry-off-profile-probe` retains the successful separate config experiment and its exact one-property helper diff; its complete-migration status remains false.

`/tmp/li-verifier-isolation-regression-before.log` preserves the global-stop regression; `/tmp/li-verifier-isolation-after.log` passes all 85 Python tests. The removed evidence rows represented unnecessary daemon cleanup, not functional coverage. `/tmp/li-document-shell-audit/report.json` retains the failing documentation-example audit; `/tmp/li-document-shell-audit-fixed/report.json` passes all nine examples after formatting-only fixes.

`/tmp/li-final-isolated-default-profile-full` now covers the default-off, response-fencing and interrupted-deletion repairs together. Both Scala compiles, focused suites, vendor pagination, full clients/server/storage suites, matching artifacts, all 133 wrapper tests, sixteen unchanged-client checkpoints and final audit pass. `/tmp/li-final-isolated-strict-audit.log` independently checks clean source, full suites and retained archives with no issues/warnings. Archive source IDs match f919812ba4 and 8086d17968. The 58 original focused selectors match actual passed JUnit cases in `/tmp/li-final-f919-junit-audit`; F30's additional cases have separate retained JUnit reports and mutation logs.

The 15 previously cancelled intermediate static-check jobs were rerun successfully. Their actual checkout trees match their PR heads, recorded in `/tmp/li-cancelled-static-checks.json`; the cancelled unit/integration jobs are not relabelled as passes. PRs 597 and 598 also pass public process CI. These successful runs do not disprove F28's deterministic client-liveness failure or approve the separate config-zero profile.

Failed runs remain failed. A source, archive or scenario change must be checked against the fingerprint before reuse. The post-f919 documentation/test-only delta is checked separately; it is not a claim that commit identities are identical.

### Inputs still required before production

- Approved Kafka/wrapper commits, official artifact checksums and regenerated wrapper dependencies.
- Named security/release approvals, including producer-ID authorization policy.
- Live broker/topic settings, binaries, persistent state, client/tool floor and owner dispositions.
- The deployed ZooKeeper/Jute/server combination and qualification results.
- Capacity, bake duration, latency, ISR recovery and failover limits, with evidence and an approver.
- Protected internal runner/environment setup and authorized wrapper access.

These are open release requirements, not assumptions to mark green. The goal remains active until the final prompt-to-artifact audit succeeds.
