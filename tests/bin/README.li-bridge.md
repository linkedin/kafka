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

# Bridge artifact verification

Use Java 17 and Python 3.9 or newer. Run the verifier from the current split-stack Kafka checkout with a matching kafka-server wrapper checkout. The [rolling-upgrade runbook](../../docs/ops/li-bridge-upgrade.md) defines the phase, deployment and approval contract.

## Locally built 3.9 artifacts

```sh
JAVA_HOME=/path/to/jdk17 \
KAFKA_30_TGZ=/releases/kafka_2.12-3.0.1.83.tgz \
WRAPPER_ROOT=/checkouts/kafka-server \
EVIDENCE_DIR=/evidence/local-bridge \
tests/bin/verify_li_bridge.sh
```

The verifier stages Scala 2.12 jars for the wrapper and builds the 3.9 archive. `LI_BRIDGE_VERSION` selects the local artifact version. Its default comes from `gradle.properties`.

## Supplied release archives

Download both archives from approved releases and verify their published SHA-256 checksums. Update the wrapper's `product-spec.json` and generated dependency metadata to the selected 3.9 version.

```sh
JAVA_HOME=/path/to/jdk17 \
KAFKA_30_TGZ=/releases/kafka_2.12-3.0.1.83.tgz \
KAFKA_39_TGZ=/releases/kafka_2.12-3.9.2.17.tgz \
SKIP_LOCAL_STAGE=1 \
WRAPPER_ROOT=/checkouts/kafka-server \
EVIDENCE_DIR=/evidence/released-bridge \
tests/bin/verify_li_bridge.sh
```

`KAFKA_39_TGZ` disables the local archive build. It requires `SKIP_LOCAL_STAGE=1` and a 3.0 archive. The verifier checks the version inside each archive and retains the exact input bytes before compilation starts.

The wrapper's declared Kafka and Scala versions must match the 3.9 archive. Every resolved main Kafka jar must match a jar in that archive byte for byte. Test classifiers must use the selected version. Their hashes are recorded. The verifier checks the resolved files before and after wrapper testing.

Source compilation and focused source tests still run. `BRIDGE_VERIFY_FULL=1` adds the complete clients, server, and storage suites. The mixed-process test runs the retained archives.

## Evidence and resume

Archives are copied under `EVIDENCE_DIR/archives` with content-addressed names. `archive-30.json` and `archive-39.json` record versions, source metadata, and jar hashes. `wrapper-artifacts.json` records the wrapper's resolved Kafka files.

Set `BRIDGE_VERIFY_RESUME=1` to reuse passed stages. Source contents, retained archive bytes, staging mode, local build version, effective metadata/recovery scale, deadlines and runtime profile must match. Archive and wrapper-artifact checks run again. If an original build-directory archive has been removed, pass its retained copy on the resumed invocation.

A strict audit is available after verification:

```sh
python3 tests/bin/audit_li_bridge_evidence.py \
  --evidence-dir /evidence/released-bridge \
  --require-clean --require-archives --allow-missing-stage
```

Add `--require-full` for a full-suite run. Omit `--allow-missing-stage` for locally staged artifacts.

## LinkedIn ZooKeeper pagination

Run the positive pagination fixture with its isolated vendor runtime:

```sh
JAVA_HOME=/path/to/jdk17 ./gradlew --no-daemon -PscalaVersion=2.12 \
  -I tests/bin/li_bridge_zookeeper_test.gradle core:liZookeeperPaginationTest
```

The task downloads LinkedIn ZooKeeper `3.6.3-23` and its matching Jute jar from the public LinkedIn ZooKeeper repository. It checks their pinned SHA-256 values. Only this test task uses those jars. The broker build and normal test classpaths retain stock ZooKeeper.

The fixture sets a 1 MiB client and server response limit. It checks complete results for 6,000 long topic names in each paginated path, watch delivery, session-expiry/rescan/watch renewal, missing paths, and authorization failures. The dedicated task has a five-minute timeout. The standard test suites leave this fixture disabled. The required CI `all` job runs the dedicated task and retains its JUnit report.

The verifier runs this fixture as the `vendor-pagination` stage. The packaged-runtime probe and broker startup check separately reject enabled pagination with a stock client. The isolated fixture does **not** select the deployed dependency or qualify an untested server version.

## Phase and workload evidence (contract version 2)

The shell entry point runs `li_bridge_mixed_cluster_smoke.py`. The test starts all-3.0 with bridge mode off, LI async fetching on and combined control on. It tests activation and backout, both binary rollback paths, hard failures, replica recovery, and deletion followed by name reuse. It then tests native control at IBP 3.0 and a separate IBP 3.9 roll.

The same old clients and Connect worker stay alive throughout. A second old Admin client keeps creating, expanding, cancelling, shrinking and deleting a topic while brokers roll. Private APIs run with both old and new jars. The record checks compare contents, not just counts. Ordinary traffic is limited to one record pair every 100 ms so the in-memory test ledger stays bounded; this is not a throughput benchmark.

Per-phase reports are `preflight-dormant.json`, `preflight-legacy-bridge.json`, `preflight-mixed.json`, `preflight-all-39-bridge.json`, `preflight-native.json`, and `preflight-ibp-39.json`. The old meaning of `native` (IBP 3.9) is deliberately removed. Old evidence is not silently accepted as contract-2 qualification.

Optional scenario inputs and defaults:

| Input | Default |
|---|---:|
| `SCALE_TOPIC_COUNT` | 10 |
| `SCALE_PARTITION_COUNT` | 5 |
| `RECOVERY_RECORD_COUNT` | 200 |
| `RECOVERY_RECORD_SIZE` | 100000 bytes |
| `BRIDGE_COMMAND_TIMEOUT_SECONDS` | 180 |
| `BRIDGE_SCENARIO_TIMEOUT_SECONDS` | 2400 |

`LI_BRIDGE_JAVA_30_HOME` optionally selects the legacy broker JDK independently from the Java 17 client/3.9 harness. JVM/runtime option changes invalidate resume. All archive inputs and process logs are retained by default; failed work directories are never silently removed. Connect's at-least-once duplicate deliveries are permitted, but missing/changed/unexpected contents fail; the broker/idempotent/transactional ledgers reject duplicate committed records.

To check a standalone process result, use the same scenario environment as the run:

```sh
python3 tests/bin/audit_li_bridge_evidence.py --process-only --require-archives \
  --evidence-dir /evidence/process-run
```

This checks all phase reports, record checks, process identities and archive hashes. It does not claim that wrapper tests, full suites or release approval passed. Failures retain thread dumps and the relevant ZooKeeper state before cleanup. The process test also writes a JUnit report.

Use `LiBridgeLiveInventory` plus owner-reviewed state dispositions and `li_bridge_preflight.py --require-live --require-all-gates` for production admission. Offline config lint is not live admission. See the runbook for credentials, freshness, cluster identity and runtime-probe requirements.

## CI and protected release gate

The public `bridge-process` job builds both sources separately. It resolves `LI_BRIDGE_LEGACY_REF` (the 3.0 review branch by default) to one commit at checkout and records that commit and archive hash. After retiring the review branch, set the variable to the release branch or a chosen commit. Public CI provides review feedback; release CI requires approved commits and published checksums. The protected `li-bridge-release-qualification.yml` workflow requires approved published archive URLs/checksums, exact Kafka/wrapper commits, an internal runner and authorized wrapper access. `verify_li_bridge_release.sh` also provides that fail-closed entry point for existing internal CI.

Archive/jar checks and source tests establish which bytes were tested. Release approval, official publication, production client/plugin/runtime qualification, actual deployed ZooKeeper versions, live configuration and capacity approval remain separate requirements. No missing approval, runner or private artifact may be treated as a successful partial release gate.
