#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Reproducible verification entry point for the 3.0-li to 3.9-li bridge.
#
# Required for cross-artifact verification:
#   KAFKA_30_TGZ=/path/to/kafka_2.12-3.0.1-SNAPSHOT.tgz
# Optional:
#   WRAPPER_ROOT=~/code/li/kafka-server
#   BRIDGE_VERIFY_FULL=1       run complete clients, server, and storage suites
#   BRIDGE_VERIFY_DRY_RUN=1    print the command plan without executing it
#   BRIDGE_VERIFY_RESUME=1     reuse passed commands in an existing evidence directory
#   ALLOW_PARTIAL=1            allow verification without the required 3.0 archive
#   SKIP_LOCAL_STAGE=1         consume already published/staged wrapper artifacts
#   WRAPPER_GRADLE_OFFLINE=1   require all wrapper build plugins to be cached
#   EVIDENCE_DIR=/path         retain command logs, timings, and mixed-run evidence

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
WRAPPER_ROOT=${WRAPPER_ROOT:-$HOME/code/li/kafka-server}
EVIDENCE_DIR=${EVIDENCE_DIR:-${TMPDIR:-/tmp}/li-bridge-verification-$(date -u +%Y%m%dT%H%M%SZ)}
BRIDGE_VERIFY_FULL=${BRIDGE_VERIFY_FULL:-0}
BRIDGE_VERIFY_DRY_RUN=${BRIDGE_VERIFY_DRY_RUN:-0}
BRIDGE_VERIFY_RESUME=${BRIDGE_VERIFY_RESUME:-0}
ALLOW_PARTIAL=${ALLOW_PARTIAL:-0}
SKIP_LOCAL_STAGE=${SKIP_LOCAL_STAGE:-0}
WRAPPER_GRADLE_OFFLINE=${WRAPPER_GRADLE_OFFLINE:-0}
mkdir -p "$EVIDENCE_DIR"
EVIDENCE_DIR=$(cd "$EVIDENCE_DIR" && pwd)
case "$EVIDENCE_DIR/" in
  "$ROOT_DIR/"*|"$WRAPPER_ROOT/"*)
    echo "EVIDENCE_DIR must be outside both source checkouts" >&2
    exit 2
    ;;
esac
TIMINGS_FILE="$EVIDENCE_DIR/commands.tsv"
if [[ "$BRIDGE_VERIFY_RESUME" != "1" || ! -f "$TIMINGS_FILE" ]]; then
  printf 'command\tduration_seconds\tresult\n' >"$TIMINGS_FILE"
fi

java_command=${JAVA_HOME:+$JAVA_HOME/bin/}java
java_version=$("$java_command" -version 2>&1 | awk -F '[".]' '/version/ {print $2; exit}')
if [[ "$java_version" != "17" ]]; then
  echo "Set JAVA_HOME to a Java 17 installation before bridge verification" >&2
  exit 1
fi
if [[ ! -d "$WRAPPER_ROOT" ]]; then
  echo "Wrapper checkout not found at $WRAPPER_ROOT" >&2
  exit 1
fi
if [[ "$BRIDGE_VERIFY_DRY_RUN" != "1" && -z "${KAFKA_30_TGZ:-}" && "$ALLOW_PARTIAL" != "1" ]]; then
  echo "Set KAFKA_30_TGZ for complete verification, or ALLOW_PARTIAL=1 for local-only checks" >&2
  exit 2
fi

FINGERPRINT_FILE="$EVIDENCE_DIR/source-fingerprints.json"
if [[ "$BRIDGE_VERIFY_DRY_RUN" != "1" ]]; then
  current_fingerprint="$EVIDENCE_DIR/source-fingerprints.current.json"
  python3 - "$ROOT_DIR" "$WRAPPER_ROOT" "$current_fingerprint" <<'PY'
import hashlib
import json
import pathlib
import subprocess
import sys

def fingerprint(root):
    names = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    paths = sorted(pathlib.Path(name.decode("utf-8")) for name in names.split(b"\0") if name)
    digest = hashlib.sha256()
    for relative in paths:
        digest.update(str(relative).encode("utf-8"))
        digest.update(b"\0")
        digest.update((root / relative).read_bytes())
        digest.update(b"\0")
    return {"sha256": digest.hexdigest(), "file_count": len(paths)}

kafka_root = pathlib.Path(sys.argv[1])
wrapper_root = pathlib.Path(sys.argv[2])
output = pathlib.Path(sys.argv[3])
output.write_text(json.dumps({
    "kafka": fingerprint(kafka_root),
    "wrapper": fingerprint(wrapper_root),
}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  if [[ "$BRIDGE_VERIFY_RESUME" == "1" ]]; then
    if [[ ! -f "$FINGERPRINT_FILE" ]]; then
      echo "Cannot resume because $FINGERPRINT_FILE is missing" >&2
      exit 3
    fi
    if ! cmp -s "$FINGERPRINT_FILE" "$current_fingerprint"; then
      echo "Source content changed since the recorded verification commands; start a new evidence directory" >&2
      exit 3
    fi
    rm "$current_fingerprint"
  else
    mv "$current_fingerprint" "$FINGERPRINT_FILE"
  fi
fi

run() {
  label=$1
  shift
  log="$EVIDENCE_DIR/$label.log"
  echo "===== $label ====="
  if [[ "$BRIDGE_VERIFY_RESUME" == "1" ]] &&
     awk -F '\t' -v command="$label" '$1 == command && $3 == "passed" {found = 1} END {exit !found}' \
       "$TIMINGS_FILE"; then
    echo "Reusing previously passed command $label"
    return 0
  fi
  if [[ "$BRIDGE_VERIFY_DRY_RUN" == "1" ]]; then
    printf 'DRY RUN:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  start=$SECONDS
  if "$@" > >(tee "$log") 2>&1; then
    duration=$((SECONDS - start))
    printf '%s\t%s\tpassed\n' "$label" "$duration" >>"$TIMINGS_FILE"
  else
    status=$?
    duration=$((SECONDS - start))
    printf '%s\t%s\tfailed(%s)\n' "$label" "$duration" "$status" >>"$TIMINGS_FILE"
    return "$status"
  fi
}

run_in_root() {
  cd "$ROOT_DIR"
  "$@"
}

run_in_wrapper() {
  cd "$WRAPPER_ROOT"
  "$@"
}

run_wrapper_tests() {
  cd "$WRAPPER_ROOT"
  args=(:likafka:kafka-impl_2.12:cleanTest :likafka:kafka-impl_2.12:test -x format)
  if [[ "$WRAPPER_GRADLE_OFFLINE" == "1" ]]; then
    args+=(--offline)
  fi
  KAFKA_30_TGZ="$KAFKA_30_TGZ" ./gradlew --no-daemon "${args[@]}"
}

run_evidence_audit() {
  cd "$ROOT_DIR"
  args=(--evidence-dir "$EVIDENCE_DIR" --require-archives \
        --output-json "$EVIDENCE_DIR/evidence-audit.json")
  if [[ "$BRIDGE_VERIFY_FULL" == "1" ]]; then
    args+=(--require-full)
  fi
  if [[ "$SKIP_LOCAL_STAGE" == "1" ]]; then
    args+=(--allow-missing-stage)
  fi
  python3 tests/bin/audit_li_bridge_evidence.py "${args[@]}"
}

run preflight-unit run_in_root python3 -m unittest discover \
  -s tests/unit -p 'li_bridge*_test.py'
run source-whitespace run_in_root git diff --check
run wrapper-whitespace run_in_wrapper git diff --check
run scala-212-compile run_in_root ./gradlew --no-daemon --max-workers=4 -PscalaVersion=2.12 \
  clients:compileTestJava core:compileScala core:compileTestScala
run scala-213-compile run_in_root ./gradlew --no-daemon --max-workers=4 -PscalaVersion=2.13 \
  clients:compileTestJava core:compileScala core:compileTestScala

run clients-bridge-tests run_in_root ./gradlew --no-daemon clients:cleanTest clients:test \
  --tests 'org.apache.kafka.common.message.Bridge*' \
  --tests 'org.apache.kafka.common.protocol.BridgeProtocolConstantsTest' \
  --tests 'org.apache.kafka.common.requests.RequestResponseTest' \
  --tests 'org.apache.kafka.clients.NodeApiVersionsTest' \
  --tests 'org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListenerTest'
run core-bridge-tests run_in_root ./gradlew --no-daemon core:cleanTest core:test --max-workers=4 \
  --tests 'kafka.controller.ControllerChannelManagerTest' \
  --tests 'kafka.server.ApiVersionManagerTest' \
  --tests 'kafka.server.ApiVersionsRequestTest' \
  --tests 'kafka.server.SaslApiVersionsRequestTest' \
  --tests 'kafka.server.KafkaConfigTest.testFromPropsInvalid' \
  --tests 'kafka.server.LiProtocolBridgeConfigTest' \
  --tests 'kafka.server.LiProtocolBridgeMetricsTest' \
  --tests 'kafka.server.ListOffsetsRequestInstrumentationTest' \
  --tests 'kafka.server.ProduceRequestInstrumentationTest' \
  --tests 'kafka.server.RequestQuotaTest'
run storage-bridge-tests run_in_root ./gradlew --no-daemon storage:cleanTest storage:test \
  --tests 'org.apache.kafka.storage.internals.log.LogDirFailureChannelTest' \
  --tests 'org.apache.kafka.storage.log.metrics.BrokerTopicMetricsTest'

if [[ "$BRIDGE_VERIFY_FULL" == "1" ]]; then
  run clients-full run_in_root ./gradlew --no-daemon clients:cleanTest clients:test --max-workers=4
  run server-full run_in_root ./gradlew --no-daemon server:cleanTest server:test --max-workers=4
  run storage-full run_in_root ./gradlew --no-daemon storage:cleanTest storage:test --max-workers=4
fi

if [[ "$SKIP_LOCAL_STAGE" != "1" ]]; then
  run stage-wrapper-artifacts run_in_root tests/bin/stage_li_bridge_ivy.sh
fi

if [[ -n "${KAFKA_30_TGZ:-}" ]]; then
  run wrapper-tests run_wrapper_tests
  run release-39 run_in_root ./gradlew --no-daemon -PscalaVersion=2.12 core:releaseTarGz
  # Release build workers can retain several GiB and starve the three-broker process test.
  run stop-kafka-gradle run_in_root ./gradlew --stop
  run stop-wrapper-gradle run_in_wrapper ./gradlew --stop
  KAFKA_39_TGZ="$ROOT_DIR/core/build/distributions/kafka_2.12-3.9.2.tgz"
  if [[ "$BRIDGE_VERIFY_DRY_RUN" == "1" ]]; then
    run mixed-process env KAFKA_30_TGZ="$KAFKA_30_TGZ" KAFKA_39_TGZ="$KAFKA_39_TGZ" \
      "$ROOT_DIR/tests/bin/li_bridge_mixed_cluster_smoke.sh"
  else
    run mixed-process env KAFKA_30_TGZ="$KAFKA_30_TGZ" KAFKA_39_TGZ="$KAFKA_39_TGZ" \
      EVIDENCE_DIR="$EVIDENCE_DIR/mixed-process" \
      "$ROOT_DIR/tests/bin/li_bridge_mixed_cluster_smoke.sh"
  fi
else
  echo "KAFKA_30_TGZ is unset; wrapper mixed-plugin and mixed-process tests were skipped" | \
    tee "$EVIDENCE_DIR/cross-artifact-skipped.log"
fi

if [[ "$BRIDGE_VERIFY_DRY_RUN" != "1" ]]; then
  python3 - "$EVIDENCE_DIR" "$ROOT_DIR" "$WRAPPER_ROOT" <<'PY'
import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

output, kafka_root, wrapper_root = map(pathlib.Path, sys.argv[1:])
def revision(root):
    return subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
def dirty(root):
    return bool(subprocess.check_output(["git", "-C", str(root), "status", "--porcelain"], text=True).strip())
def fingerprint(root):
    names = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    paths = sorted(pathlib.Path(name.decode("utf-8")) for name in names.split(b"\0") if name)
    digest = hashlib.sha256()
    for relative in paths:
        digest.update(str(relative).encode("utf-8"))
        digest.update(b"\0")
        digest.update((root / relative).read_bytes())
        digest.update(b"\0")
    return {"sha256": digest.hexdigest(), "file_count": len(paths)}
fingerprints = json.loads((output / "source-fingerprints.json").read_text(encoding="utf-8"))
current_fingerprints = {"kafka": fingerprint(kafka_root), "wrapper": fingerprint(wrapper_root)}
if current_fingerprints != fingerprints:
    raise SystemExit("Source content changed during verification; evidence is invalid")
summary = {
    "passed": True,
    "finished_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "kafka": {"commit": revision(kafka_root), "dirty": dirty(kafka_root),
              "source_sha256": fingerprints["kafka"]["sha256"]},
    "wrapper": {"commit": revision(wrapper_root), "dirty": dirty(wrapper_root),
                "source_sha256": fingerprints["wrapper"]["sha256"]},
}
(output / "verification-summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  if [[ -n "${KAFKA_30_TGZ:-}" ]]; then
    run evidence-audit run_evidence_audit
  fi
fi

if [[ "$BRIDGE_VERIFY_DRY_RUN" == "1" ]]; then
  echo "Bridge verification plan rendered; evidence directory: $EVIDENCE_DIR"
elif [[ -z "${KAFKA_30_TGZ:-}" ]]; then
  echo "Partial bridge verification passed; cross-artifact tests were skipped: $EVIDENCE_DIR"
else
  echo "Bridge verification passed; evidence directory: $EVIDENCE_DIR"
fi
