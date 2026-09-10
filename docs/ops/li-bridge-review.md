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

**The reviewed implementation and tools are published as 32 open PRs. Every diff is below 1,000 changed lines. The clean-source process scenario, its strict audit, and the 132-test wrapper suite pass locally. This is not rollout approval.** The final requirement audit, full verifier run, remote CI review and production release gates remain open.

The findings below record what the initial review and later tests found. Instructions in an original finding describe the repair that was needed; use the current disposition and evidence sections for status. This is not a line-by-line approval of every Kafka change.

### Reviewed revisions

- Workspace plan: `LI-3.0-TO-3.9-ROLLING-UPGRADE-PLAN.md`. Canonical runbook: `docs/ops/li-bridge-upgrade.md`.
- Qualified 3.9 source snapshot: `1578464defd931f95cecef78b55ebd2769a41381`; the current complete stack ends at PR 574.
- Current 3.0 source: PR 577, `cb85262cb15675a19f628ff0010bacf72065a7b5`.
- CI: PR 558, `3e799b08ea`; PR 559, `a86214e2da`.
- Wrapper: `1a9ecccf`, including the ACL test fix `6ddf2a87`.

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

## PR dispositions and dependency audit

Every PR below has a distinct migration or CI purpose. Keep these scopes, but do not treat publication, a resolved thread or a green job as release approval. New review layers are drafts. The controller/ZooKeeper, security, storage and operational changes still need the corresponding owners' review.

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

Merge CI 558/559 into their own release branches first. Then retarget the upgrade stack bottoms as described in the runbook. Never force a Git dependency between the 3.0 and 3.9 CI branches. When reordering again, change PR bases before pushing a head that becomes an ancestor of its former base; GitHub can otherwise auto-close and delete that branch.

The oversized original scopes were split. Control wire definitions are in 565 (503 lines), handlers in 545 (845); storage metrics are in 566 (221), broker metrics/watchdog wiring in 551 (910); workload helpers precede runner 553 (754). The new evidence auditor, verifier and release gate are separate layers. All 32 current diffs pass the size and dependency audit.

The original 62 review threads now have replies with published source decisions and code/test references. See `li-bridge-review-comments.md`. The static `LeaderTransferManager.noOp()` call is valid: javap confirms the forwarder, and the Java builder compiles. Empty metric bucket lists remain intentionally supported; malformed, negative and unordered boundaries are rejected. A fresh comment/readback audit is still required before closing the goal.

## Verification and remaining requirements

Passing a suite only covers what that suite asserts. These results are not substitutes for the remaining gates.

| Requirement | Files or checks | Current evidence / remaining work |
|---|---|---|
| Include every open public PR | Runbook inventory; GitHub readback | 32 open PRs, exact heads/bases checked. All 30 upgrade PRs have the requested label; CI 558/559 do not. |
| Keep PRs reviewable | GitHub additions plus deletions | Every current PR is below 1,000 lines. Stack memberships are 27 PRs on 3.9 and three on 3.0. |
| Use common control versions | Schemas, fixtures, controller selectors | v2/v5/v1 checked on both generations. Full process test exercises both controller directions. |
| Gate runtime changes | KafkaConfig, DynamicBrokerConfig, bridge metrics and wrapper mapping | 23 default-off 3.9 gates; wrapper enables them explicitly. Flag-off and activation tests exist. Final per-change gate audit remains required. |
| Fence activation and retain callbacks | RequestSendThreadBridgeTest | Blocked dequeue, sustained queue and admitted-callback regressions pass. Controller restart remains mandatory. |
| Validate all six phases | li_bridge_contract.py; preflight | 65 Python tests pass. Native control remains at IBP 3.0 before a separate IBP 3.9 roll. |
| Keep old clients unchanged | Continuous client/Streams/Connect helpers and private APIs | Same old processes remain alive across the migration. Exact records, transaction outcomes, offsets and process identities are checked. Clean process run and audit pass. |
| Prove persisted rollback and recovery | Process runner | Canary and all-3.9 rollback use the same directories. Both recovery directions, cancellation, hard failures and truncation pass. |
| Handle deletion and recreation | TopicDeletionManager, ZkMetadataCache, ReplicaManager | F12/F13/F15/F17 have gated repairs and regressions. Known unhosted-log case passes; broader offline name reuse remains a qualification item. |
| Handle version-changing ISR retries | AlterPartitionManager | Dynamic activation and version changes on one queued builder pass. Clean process run has no unsupported-version error. Earlier failed evidence is retained. |
| Collect actual runtime/state | Live inventory and runtime probe | Disposable-cluster collection and negative-input tests pass. Actual production config, runtime and client floor are still needed. |
| Qualify ZooKeeper pagination | KafkaZkClient and isolated vendor tests | Stock-client startup rejection and five vendor cases pass. This does not qualify a different deployed client/server pair. |
| Follow Google shell style | Four wrappers and extracted workflow run blocks | ShellCheck, shfmt, syntax and wrapper-length tests pass. Complex orchestration/release bookkeeping is Python. Recheck after final docs/workflow edits. |
| Verify wrapper compatibility | Factory mapping, ACL race fix, full wrapper suite | 132 tests pass; no failures or skips. Main and test-classifier hashes match before and after the suite. Main jars match the selected 3.9 archive. |
| Validate new orchestration on real artifacts | Python stager, artifact checker, verifier/release guard | Real staging and artifact/classpath comparison pass. Full new verifier and protected release-guard runs are still needed. |
| Review comments with evidence | Per-thread ledger and published replies | All original 62 decisions posted. Re-fetch threads/reviews and verify new comments before completion. |
| Keep docs consistent | Workspace plan and canonical docs | Current 32-PR inventory is written; final synchronization, link and wording audit remain required. |
| Preserve side task PR 2039 | Wrapper ADU worktree | Rebased and pushed on master; formatting is one commit and is idempotent. No functional patch changes. |

### Retained local evidence

- `/tmp/li-bridge-clean-final-process`: all sixteen checkpoints pass, source inputs unchanged, strict process audit has no issues or warnings. Diagnostic logging is absent from the archive.
- `/tmp/li-clean-30-build.log`: standalone clean Git checkout `cb85262cb1`, Java 11, targeted controller/cache/deletion tests and archive build pass. Embedded commit ID matches that checkout.
- `/tmp/li-final-stray-39.log`: clean restack source, targeted cache/ISR/stray-log and packaged-client tests, archive build pass. The linked-worktree archive embeds `commitId=unknown`; source checkout identity and archive hashes are retained. Final published qualification must verify source identity again.
- `/tmp/li-wrapper-current-verification`: 132 retained JUnit results and identical before/after artifact reports.
- `/tmp/li-stager-real.log`: the new Python stager builds and installs actual artifacts and writes its scoped checksum manifest.
- `/tmp/li-restack-python-tests.log`: 65 tests pass. Test-method AST comparison confirms that splitting files did not remove or change assertions.
- `/tmp/li-publication-pr-audit-result.json`: 32 PRs, no head/base/label/size issues.

The complete verifier has not yet passed as one final bundle. Previous failed runs are not green evidence. Do not relabel or edit them to claim success. Any source, archive or scenario change must be checked against the evidence fingerprint before reuse.

### Inputs still required before production

- Approved Kafka/wrapper commits, official artifact checksums and regenerated wrapper dependencies.
- Named security/release approvals, including producer-ID authorization policy.
- Live broker/topic settings, binaries, persistent state, client/tool floor and owner dispositions.
- The deployed ZooKeeper/Jute/server combination and qualification results.
- Capacity, bake duration, latency, ISR recovery and failover limits, with evidence and an approver.
- Protected internal runner/environment setup and authorized wrapper access.

These are open release requirements, not assumptions to mark green. The goal remains active until the final prompt-to-artifact audit succeeds.
