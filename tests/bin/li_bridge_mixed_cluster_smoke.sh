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

# Runs real 3.0-li and 3.9-li broker processes against one ZooKeeper ensemble.
# The test forces each binary to become controller while the other binary is
# present, then verifies topic mutation, reassignment and cancellation, retained
# private APIs, old transactional/group clients, and produce/consume across failover.
#
# Required environment variables:
#   KAFKA_30_TGZ  bridge-capable 3.0-li release archive
#   KAFKA_39_TGZ  bridge-capable 3.9-li release archive
# Optional:
#   KEEP_WORK_DIR=1           retain logs and data after the test
#   SCALE_TOPIC_COUNT=10       metadata-volume topics retained across failovers
#   SCALE_PARTITION_COUNT=5    partitions in each metadata-volume topic
#   RECOVERY_RECORD_COUNT=200  records produced while each follower is offline
#   RECOVERY_RECORD_SIZE=100000 bytes in each follower-recovery record
#   EVIDENCE_DIR=/path         retain JSON preflights, timings, hashes, and protocol logs
#   EVIDENCE_INCLUDE_LOGS=1    include a compressed archive of all process logs

set -euo pipefail

: "${KAFKA_30_TGZ:?Set KAFKA_30_TGZ to the 3.0-li bridge release archive}"
: "${KAFKA_39_TGZ:?Set KAFKA_39_TGZ to the 3.9-li bridge release archive}"

KAFKA_30_TGZ=$(cd "$(dirname "$KAFKA_30_TGZ")" && pwd)/$(basename "$KAFKA_30_TGZ")
KAFKA_39_TGZ=$(cd "$(dirname "$KAFKA_39_TGZ")" && pwd)/$(basename "$KAFKA_39_TGZ")
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/li-kafka-bridge-smoke.XXXXXX")
RUN_STARTED_UTC=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EVIDENCE_DIR=${EVIDENCE_DIR:-}
if [[ -n "$EVIDENCE_DIR" ]]; then
  mkdir -p "$EVIDENCE_DIR"
  EVIDENCE_DIR=$(cd "$EVIDENCE_DIR" && pwd)
fi
TIMINGS_FILE="$WORK_DIR/timings.tsv"
RESOURCES_FILE="$WORK_DIR/broker-resources.tsv"
HEAP_INFO_FILE="$WORK_DIR/broker-heap-info.log"
printf 'operation\tduration_seconds\tresult\n' >"$TIMINGS_FILE"
printf 'timestamp_utc\tphase\tprocess\tpid\trss_kib\n' >"$RESOURCES_FILE"
ZK_PORT=${ZK_PORT:-22181}
BROKER_30_PORT=${BROKER_30_PORT:-29092}
BROKER_39_PORT=${BROKER_39_PORT:-29093}
BROKER_39_EXTRA_PORT=${BROKER_39_EXTRA_PORT:-29094}
SCALE_TOPIC_COUNT=${SCALE_TOPIC_COUNT:-10}
SCALE_PARTITION_COUNT=${SCALE_PARTITION_COUNT:-5}
RECOVERY_RECORD_COUNT=${RECOVERY_RECORD_COUNT:-200}
RECOVERY_RECORD_SIZE=${RECOVERY_RECORD_SIZE:-100000}
ZK_PID=""
BROKER_30_PID=""
BROKER_39_PID=""
BROKER_39_EXTRA_PID=""
CONNECT_30_PID=""

capture_evidence() {
  status=$1
  [[ -n "$EVIDENCE_DIR" ]] || return 0
  cp "$TIMINGS_FILE" "$EVIDENCE_DIR/timings.tsv"
  cp "$RESOURCES_FILE" "$EVIDENCE_DIR/broker-resources.tsv"
  if [[ -s "$HEAP_INFO_FILE" ]]; then
    cp "$HEAP_INFO_FILE" "$EVIDENCE_DIR/broker-heap-info.log"
  fi
  for evidence in bridge-preflight-evidence.json native-preflight-evidence.json; do
    if [[ -f "$WORK_DIR/$evidence" ]]; then
      cp "$WORK_DIR/$evidence" "$EVIDENCE_DIR/$evidence"
    fi
  done
  {
    if [[ -n "${KAFKA_30_HOME:-}" && -f "$KAFKA_30_HOME/logs/controller.log" ]]; then
      grep -E 'LI protocol bridge mode (enabled|disabled)' \
        "$KAFKA_30_HOME/logs/controller.log" || true
    fi
    if [[ -n "${KAFKA_39_HOME:-}" && -f "$KAFKA_39_HOME/logs/controller.log" ]]; then
      grep -E 'LI protocol bridge mode (enabled|disabled)' \
        "$KAFKA_39_HOME/logs/controller.log" || true
    fi
  } >"$EVIDENCE_DIR/protocol-selection.log"

  python3 - "$EVIDENCE_DIR/run-summary.json" "$status" "$RUN_STARTED_UTC" \
    "$KAFKA_30_TGZ" "$KAFKA_39_TGZ" "$SCALE_TOPIC_COUNT" "$SCALE_PARTITION_COUNT" \
    "$RECOVERY_RECORD_COUNT" "$RECOVERY_RECORD_SIZE" <<'PY'
import datetime
import hashlib
import json
import pathlib
import sys

output, status, started, archive_30, archive_39, topics, partitions, records, record_size = sys.argv[1:]
def archive(path):
    value = pathlib.Path(path)
    return {"path": str(value), "sha256": hashlib.sha256(value.read_bytes()).hexdigest()}
summary = {
    "passed": int(status) == 0,
    "exit_status": int(status),
    "started_utc": started,
    "finished_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "archives": {"3.0": archive(archive_30), "3.9": archive(archive_39)},
    "scale": {"topic_count": int(topics), "partitions_per_topic": int(partitions)},
    "recovery": {"record_count": int(records), "record_size": int(record_size)},
}
pathlib.Path(output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

  if [[ "${EVIDENCE_INCLUDE_LOGS:-0}" == "1" ]]; then
    evidence_logs="$WORK_DIR/evidence-logs"
    mkdir -p "$evidence_logs"
    for log in "$WORK_DIR"/*.log; do
      [[ -f "$log" ]] && cp "$log" "$evidence_logs/"
    done
    if [[ -n "${KAFKA_30_HOME:-}" ]]; then
      cp "$KAFKA_30_HOME/logs/controller.log" "$evidence_logs/controller-30.log" 2>/dev/null || true
      cp "$KAFKA_30_HOME/logs/server.log" "$evidence_logs/server-30.log" 2>/dev/null || true
    fi
    if [[ -n "${KAFKA_39_HOME:-}" ]]; then
      cp "$KAFKA_39_HOME/logs/controller.log" "$evidence_logs/controller-39.log" 2>/dev/null || true
      cp "$KAFKA_39_HOME/logs/server.log" "$evidence_logs/server-39.log" 2>/dev/null || true
    fi
    tar -czf "$EVIDENCE_DIR/process-logs.tgz" -C "$evidence_logs" .
  fi
  echo "Verification evidence written to $EVIDENCE_DIR" >&2
}

cleanup() {
  status=$?
  set +e
  if [[ -n "$CONNECT_30_PID" ]] && kill -0 "$CONNECT_30_PID" 2>/dev/null; then
    kill "$CONNECT_30_PID" 2>/dev/null
    wait "$CONNECT_30_PID" 2>/dev/null
  fi
  for pid in "$BROKER_30_PID" "$BROKER_39_PID" "$BROKER_39_EXTRA_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null
    fi
  done
  for pid in "$BROKER_30_PID" "$BROKER_39_PID" "$BROKER_39_EXTRA_PID"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null
    fi
  done
  if [[ -n "$ZK_PID" ]] && kill -0 "$ZK_PID" 2>/dev/null; then
    kill "$ZK_PID" 2>/dev/null
  fi
  if [[ -n "$ZK_PID" ]]; then
    wait "$ZK_PID" 2>/dev/null
  fi
  capture_evidence "$status"
  if [[ $status -ne 0 ]]; then
    echo "Mixed-cluster smoke test failed; recent logs:" >&2
    for log in "$WORK_DIR"/*.log "$WORK_DIR"/extract-*/*/logs/controller.log; do
      [[ -f "$log" ]] || continue
      echo "===== $log =====" >&2
      tail -80 "$log" >&2
    done
  fi
  if [[ "${KEEP_WORK_DIR:-0}" == "1" ]]; then
    echo "Work directory retained at $WORK_DIR" >&2
  else
    rm -rf "$WORK_DIR"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

record_broker_resources() {
  phase=$1
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  for entry in "broker-0:$BROKER_30_PID" "broker-1:$BROKER_39_PID" \
               "broker-2:$BROKER_39_EXTRA_PID"; do
    process_name=${entry%%:*}
    pid=${entry#*:}
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      rss=$(ps -o rss= -p "$pid" | tr -d ' ')
      printf '%s\t%s\t%s\t%s\t%s\n' \
        "$timestamp" "$phase" "$process_name" "$pid" "${rss:-unknown}" >>"$RESOURCES_FILE"
      if command -v jcmd >/dev/null 2>&1; then
        {
          echo "===== $timestamp $phase $process_name pid=$pid ====="
          jcmd "$pid" GC.heap_info
        } >>"$HEAP_INFO_FILE" 2>&1 || true
      fi
    fi
  done
}

wait_until() {
  timeout_seconds=$1
  description=$2
  shift 2
  start_seconds=$SECONDS
  deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@"; then
      duration=$((SECONDS - start_seconds))
      echo "$description completed in $duration seconds"
      printf '%s\t%s\tpassed\n' "$description" "$duration" >>"$TIMINGS_FILE"
      return 0
    fi
    sleep 1
  done
  duration=$((SECONDS - start_seconds))
  printf '%s\t%s\ttimed_out\n' "$description" "$duration" >>"$TIMINGS_FILE"
  echo "Timed out waiting for $description" >&2
  return 1
}

znode_contains() {
  path=$1
  expected=$2
  echo "get $path" | "$KAFKA_39_HOME/bin/zookeeper-shell.sh" "127.0.0.1:$ZK_PORT" 2>/dev/null |
    grep -q "$expected"
}

zk_responding() {
  echo 'ls /' | "$KAFKA_39_HOME/bin/zookeeper-shell.sh" "127.0.0.1:$ZK_PORT" 2>/dev/null |
    grep -q '\['
}

controller_is() {
  znode_contains /controller "\"brokerid\":$1"
}

broker_registered() {
  znode_contains "/brokers/ids/$1" '"version"'
}

cluster_has_broker_count() {
  expected=$1
  actual=$("$KAFKA_39_HOME/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" 2>/dev/null | grep -c 'id:')
  [[ "$actual" -ge "$expected" ]]
}

start_broker_30() {
  "$KAFKA_30_HOME/bin/kafka-server-start.sh" "$WORK_DIR/broker-30.properties" \
    >>"$WORK_DIR/broker-30.log" 2>&1 &
  BROKER_30_PID=$!
  wait_until 90 "3.0 broker registration" broker_registered 0
}

start_broker_39() {
  "$KAFKA_39_HOME/bin/kafka-server-start.sh" "$WORK_DIR/broker-39.properties" \
    >>"$WORK_DIR/broker-39.log" 2>&1 &
  BROKER_39_PID=$!
  wait_until 90 "3.9 broker registration" broker_registered 1
}

start_broker_39_extra() {
  "$KAFKA_39_HOME/bin/kafka-server-start.sh" "$WORK_DIR/broker-39-extra.properties" \
    >>"$WORK_DIR/broker-39-extra.log" 2>&1 &
  BROKER_39_EXTRA_PID=$!
  wait_until 90 "extra 3.9 broker registration" broker_registered 2
}

start_upgraded_broker_30() {
  "$KAFKA_39_HOME/bin/kafka-server-start.sh" "$WORK_DIR/broker-30-upgraded.properties" \
    >>"$WORK_DIR/broker-30-upgraded.log" 2>&1 &
  BROKER_30_PID=$!
  wait_until 90 "upgraded broker 0 registration" broker_registered 0
}

stop_broker_30() {
  kill "$BROKER_30_PID"
  wait "$BROKER_30_PID" || true
  BROKER_30_PID=""
}

stop_broker_39() {
  kill "$BROKER_39_PID"
  wait "$BROKER_39_PID" || true
  BROKER_39_PID=""
}

stop_broker_39_extra() {
  kill "$BROKER_39_EXTRA_PID"
  wait "$BROKER_39_EXTRA_PID" || true
  BROKER_39_EXTRA_PID=""
}

cluster_command() {
  "$KAFKA_39_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:$BROKER_39_PORT" "$@"
}

create_mutation_topic() {
  topic=$1
  if cluster_command --describe --topic "$topic" >/dev/null 2>&1; then
    return 0
  fi
  cluster_command --create --topic "$topic" --partitions 1 --replication-factor 2 \
    >/dev/null 2>&1
}

create_and_delete_topic() {
  suffix=$1
  topic="bridge-mutation-$suffix"
  wait_until 90 "controller readiness for topic $topic" create_mutation_topic "$topic"
  cluster_command --describe --topic "$topic" >/dev/null
  cluster_command --delete --topic "$topic"
}

reassignment_complete() {
  reassignment_file=$1
  "$KAFKA_39_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --reassignment-json-file "$reassignment_file" --verify 2>/dev/null |
    grep -q 'is complete'
}

reassignment_in_progress() {
  reassignment_file=$1
  "$KAFKA_39_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --reassignment-json-file "$reassignment_file" --verify 2>/dev/null |
    grep -q 'is still in progress'
}

topic_has_original_replica_set() {
  # Cancellation preserves the original replica set; leadership changes may reorder it.
  cluster_command --describe --topic bridge-cancel 2>/dev/null |
    grep -Eq 'Partition: 0.*Replicas: (0,1|1,0)'
}

ensure_controller_39() {
  if ! controller_is 1; then
    stop_broker_30
    wait_until 60 "3.9 controller election before cancellation test" controller_is 1
    start_broker_30
  fi
}

exercise_private_apis() {
  suffix=$1
  java -cp "$WORK_DIR/private-api-classes:$KAFKA_39_HOME/libs/*" \
    LiBridgePrivateApiSmoke "127.0.0.1:$BROKER_39_PORT" "$suffix"
}

exercise_old_transactional_and_group_client() {
  cluster_command --create --topic bridge-old-client --partitions 2 --replication-factor 2
  java -cp "$WORK_DIR/old-client-classes:$KAFKA_30_HOME/libs/*" \
    LiBridgeOldClientSmoke "127.0.0.1:$BROKER_39_PORT" bridge-old-client
}

connect_output_complete() {
  [[ -f "$WORK_DIR/connect-output.txt" ]] &&
    [[ $(grep -c '^old-connect-message-' "$WORK_DIR/connect-output.txt") -eq 10 ]]
}

exercise_old_streams() {
  cluster_command --create --topic bridge-old-streams-input --partitions 2 --replication-factor 2
  cluster_command --create --topic bridge-old-streams-output --partitions 2 --replication-factor 2
  java -cp "$WORK_DIR/old-streams-classes:$KAFKA_30_HOME/libs/*" \
    LiBridgeOldStreamsSmoke "127.0.0.1:$BROKER_39_PORT" \
    bridge-old-streams-input bridge-old-streams-output
}

exercise_old_connect() {
  cluster_command --create --topic bridge-old-connect --partitions 2 --replication-factor 2
  i=1
  while (( i <= 10 )); do
    echo "old-connect-message-$i"
    i=$((i + 1))
  done >"$WORK_DIR/connect-input.txt"

  cat >"$WORK_DIR/connect-standalone.properties" <<EOF
bootstrap.servers=127.0.0.1:$BROKER_39_PORT
offset.storage.file.filename=$WORK_DIR/connect.offsets
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.flush.interval.ms=1000
plugin.path=$KAFKA_30_HOME/libs
EOF
  cat >"$WORK_DIR/connect-source.properties" <<EOF
name=bridge-old-file-source
connector.class=org.apache.kafka.connect.file.FileStreamSourceConnector
tasks.max=1
file=$WORK_DIR/connect-input.txt
topic=bridge-old-connect
EOF
  cat >"$WORK_DIR/connect-sink.properties" <<EOF
name=bridge-old-file-sink
connector.class=org.apache.kafka.connect.file.FileStreamSinkConnector
tasks.max=1
file=$WORK_DIR/connect-output.txt
topics=bridge-old-connect
EOF

  "$KAFKA_30_HOME/bin/connect-standalone.sh" "$WORK_DIR/connect-standalone.properties" \
    "$WORK_DIR/connect-source.properties" "$WORK_DIR/connect-sink.properties" \
    >>"$WORK_DIR/connect-30.log" 2>&1 &
  CONNECT_30_PID=$!
  wait_until 90 "old Connect source and sink records" connect_output_complete
  kill "$CONNECT_30_PID"
  wait "$CONNECT_30_PID" || true
  CONNECT_30_PID=""
}

recovery_topic_has_full_isr() {
  cluster_command --describe --topic bridge-recovery 2>/dev/null |
    grep -Eq 'Partition: 0.*Isr: (0,1|1,0)'
}

recovery_topic_leader_is() {
  leader=$1
  cluster_command --describe --topic bridge-recovery 2>/dev/null |
    grep -q "Partition: 0.*Leader: $leader"
}

produce_recovery_data() {
  bootstrap_server=$1
  "$KAFKA_39_HOME/bin/kafka-producer-perf-test.sh" \
    --topic bridge-recovery --num-records "$RECOVERY_RECORD_COUNT" \
    --record-size "$RECOVERY_RECORD_SIZE" --throughput -1 \
    --producer-props "bootstrap.servers=$bootstrap_server" acks=all >/dev/null
}

exercise_replica_recovery_both_directions() {
  cluster_command --create --topic bridge-recovery --replica-assignment '0:1'

  # Qualify the Apache 3.9 fetcher while following a 3.0 leader.
  stop_broker_39
  wait_until 60 "3.0 controller election for 3.9 recovery" controller_is 0
  produce_recovery_data "127.0.0.1:$BROKER_30_PORT"
  recovery_start=$SECONDS
  start_broker_39
  wait_until 120 "3.9 follower recovery from a 3.0 leader" recovery_topic_has_full_isr
  recovered_bytes=$((RECOVERY_RECORD_COUNT * RECOVERY_RECORD_SIZE))
  echo "3.9 follower recovered $recovered_bytes bytes in $((SECONDS - recovery_start)) seconds"

  # Put leadership on 3.9, then qualify the old follower against the new leader.
  reassignment_file="$WORK_DIR/reassignment-recovery-leader.json"
  cat >"$reassignment_file" <<'EOF'
{"version":1,"partitions":[{"topic":"bridge-recovery","partition":0,"replicas":[1,0]}]}
EOF
  "$KAFKA_39_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --reassignment-json-file "$reassignment_file" --execute >/dev/null
  wait_until 60 "recovery-topic replica reorder" reassignment_complete "$reassignment_file"
  "$KAFKA_39_HOME/bin/kafka-leader-election.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" --election-type preferred \
    --topic bridge-recovery --partition 0 >/dev/null
  wait_until 60 "3.9 leadership for old-follower recovery" recovery_topic_leader_is 1

  stop_broker_30
  wait_until 60 "3.9 controller election for old-follower recovery" controller_is 1
  produce_recovery_data "127.0.0.1:$BROKER_39_PORT"
  recovery_start=$SECONDS
  start_broker_30
  wait_until 120 "3.0 follower recovery from a 3.9 leader" recovery_topic_has_full_isr
  recovered_bytes=$((RECOVERY_RECORD_COUNT * RECOVERY_RECORD_SIZE))
  echo "3.0 follower recovered $recovered_bytes bytes in $((SECONDS - recovery_start)) seconds"
}

truncation_topic_leader_is_new_broker() {
  cluster_command --describe --topic bridge-truncation 2>/dev/null |
    grep -q 'Partition: 0.*Leader: 1'
}

truncation_topic_leader_is_old_broker() {
  "$KAFKA_30_HOME/bin/kafka-topics.sh" --bootstrap-server "127.0.0.1:$BROKER_30_PORT" \
    --describe --topic bridge-truncation 2>/dev/null |
    grep -q 'Partition: 0.*Leader: 0'
}

truncation_topic_has_full_isr() {
  cluster_command --describe --topic bridge-truncation 2>/dev/null |
    grep -Eq 'Partition: 0.*Isr: (0,1|1,0)'
}

exercise_39_follower_truncation() {
  cluster_command --create --topic bridge-truncation --replica-assignment '1:0' \
    --config unclean.leader.election.enable=true
  wait_until 60 "3.9 leadership for truncation setup" truncation_topic_leader_is_new_broker

  # Let the 3.9 leader advance alone, then start the stale 3.0 replica as unclean leader.
  # The records produced here are intentionally disposable: this scenario verifies that the
  # returning 3.9 follower truncates its longer log to the elected old leader.
  stop_broker_30
  wait_until 60 "3.9 controller election for truncation setup" controller_is 1
  "$KAFKA_39_HOME/bin/kafka-producer-perf-test.sh" \
    --topic bridge-truncation --num-records 50 --record-size 100000 --throughput -1 \
    --producer-props "bootstrap.servers=127.0.0.1:$BROKER_39_PORT" acks=1 >/dev/null
  stop_broker_39

  start_broker_30
  wait_until 60 "3.0 controller election for unclean recovery" controller_is 0
  wait_until 90 "stale 3.0 replica to become unclean leader" truncation_topic_leader_is_old_broker
  start_broker_39
  wait_until 120 "3.9 follower truncation and ISR recovery" truncation_topic_has_full_isr
  grep -Eq 'Truncating (partition )?bridge-truncation-0' \
    "$KAFKA_39_HOME/logs/server.log"
}

exercise_cancellation_across_failover() {
  ensure_controller_39
  start_broker_39_extra
  cluster_command --create --topic bridge-cancel --replica-assignment '0:1'
  "$KAFKA_39_HOME/bin/kafka-producer-perf-test.sh" \
    --topic bridge-cancel --num-records 100 --record-size 100000 --throughput -1 \
    --producer-props "bootstrap.servers=127.0.0.1:$BROKER_39_PORT" acks=all >/dev/null

  reassignment_file="$WORK_DIR/reassignment-cancel.json"
  cat >"$reassignment_file" <<'EOF'
{"version":1,"partitions":[{"topic":"bridge-cancel","partition":0,"replicas":[1,2]}]}
EOF
  "$KAFKA_39_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --reassignment-json-file "$reassignment_file" --execute --throttle 1024 >/dev/null
  wait_until 30 "throttled reassignment to begin" reassignment_in_progress "$reassignment_file"

  # Remove the 3.9 controller and target broker while copying, then restore the original
  # replica and cancel under the 3.0 controller.
  stop_broker_39
  stop_broker_39_extra
  wait_until 60 "3.0 controller election during reassignment" controller_is 0
  start_broker_39
  "$KAFKA_39_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --reassignment-json-file "$reassignment_file" --cancel --preserve-throttles >/dev/null
  wait_until 60 "reassignment cancellation to restore original replicas" topic_has_original_replica_set
}

reassign_partition() {
  suffix=$1
  replicas=$2
  reassignment_file="$WORK_DIR/reassignment-$suffix.json"
  cat >"$reassignment_file" <<EOF
{"version":1,"partitions":[{"topic":"bridge-smoke","partition":0,"replicas":[$replicas]}]}
EOF
  "$KAFKA_39_HOME/bin/kafka-reassign-partitions.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --reassignment-json-file "$reassignment_file" --execute >/dev/null
  wait_until 60 "partition reassignment under $suffix" reassignment_complete "$reassignment_file"
}

produce_phase() {
  phase=$1
  count=$2
  client_home=$3
  i=1
  while (( i <= count )); do
    echo "$phase-message-$i"
    i=$((i + 1))
  done | "$client_home/bin/kafka-console-producer.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" --topic bridge-smoke
}

consume_count() {
  expected=$1
  client_home=$2
  output="$WORK_DIR/consume-$expected.txt"
  "$client_home/bin/kafka-console-consumer.sh" \
    --bootstrap-server "127.0.0.1:$BROKER_39_PORT" --topic bridge-smoke \
    --from-beginning --max-messages "$expected" --timeout-ms 30000 >"$output"
  actual=$(wc -l <"$output" | tr -d ' ')
  [[ "$actual" == "$expected" ]]
}

assert_healthy_partitions() {
  under_replicated=$(cluster_command --describe --under-replicated-partitions)
  unavailable=$(cluster_command --describe --unavailable-partitions)
  [[ -z "$under_replicated" && -z "$unavailable" ]]
}

assert_no_protocol_errors() {
  ! grep -Eqi \
    'UnsupportedVersionException|Error parsing.*(LeaderAndIsr|UpdateMetadata|StopReplica)|unknown api key' \
    "$WORK_DIR/broker-30.log" "$WORK_DIR/broker-30-upgraded.log" \
    "$WORK_DIR/broker-39.log" "$WORK_DIR/broker-39-extra.log" \
    "$KAFKA_30_HOME/logs/controller.log" "$KAFKA_39_HOME/logs/controller.log"
}

native_mode_configured() {
  "$KAFKA_39_HOME/bin/kafka-configs.sh" --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
    --entity-type brokers --entity-default --describe --all 2>/dev/null |
    grep -q 'li.protocol.bridge.mode.enable=false'
}

controller_logged_native_mode() {
  grep -q 'LI protocol bridge mode disabled' "$KAFKA_39_HOME/logs/controller.log"
}

broker_logs_show_ibp_39() {
  grep -q 'inter.broker.protocol.version = 3.9' "$WORK_DIR/broker-30-upgraded.log" &&
    grep -q 'inter.broker.protocol.version = 3.9' "$WORK_DIR/broker-39.log"
}

mkdir -p "$WORK_DIR/extract-30" "$WORK_DIR/extract-39"
tar -xzf "$KAFKA_30_TGZ" -C "$WORK_DIR/extract-30"
tar -xzf "$KAFKA_39_TGZ" -C "$WORK_DIR/extract-39"
KAFKA_30_HOME=$(find "$WORK_DIR/extract-30" -mindepth 1 -maxdepth 1 -type d | head -1)
KAFKA_39_HOME=$(find "$WORK_DIR/extract-39" -mindepth 1 -maxdepth 1 -type d | head -1)
mkdir -p "$WORK_DIR/private-api-classes" "$WORK_DIR/old-client-classes" \
  "$WORK_DIR/old-streams-classes" "$WORK_DIR/metadata-scale-classes"
javac -cp "$KAFKA_39_HOME/libs/*" -d "$WORK_DIR/private-api-classes" \
  "$SCRIPT_DIR/LiBridgePrivateApiSmoke.java"
javac -cp "$KAFKA_39_HOME/libs/*" -d "$WORK_DIR/metadata-scale-classes" \
  "$SCRIPT_DIR/LiBridgeMetadataScaleSmoke.java"
javac -cp "$KAFKA_30_HOME/libs/*" -d "$WORK_DIR/old-client-classes" \
  "$SCRIPT_DIR/LiBridgeOldClientSmoke.java"
javac -cp "$KAFKA_30_HOME/libs/*" -d "$WORK_DIR/old-streams-classes" \
  "$SCRIPT_DIR/LiBridgeOldStreamsSmoke.java"

cat >"$WORK_DIR/zookeeper.properties" <<EOF
clientPort=$ZK_PORT
dataDir=$WORK_DIR/zookeeper-data
maxClientCnxns=0
admin.enableServer=false
EOF

common_broker_config() {
  broker_id=$1
  port=$2
  log_dir=$3
  cat <<EOF
broker.id=$broker_id
listeners=PLAINTEXT://127.0.0.1:$port
advertised.listeners=PLAINTEXT://127.0.0.1:$port
listener.security.protocol.map=PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
zookeeper.connect=127.0.0.1:$ZK_PORT
log.dirs=$log_dir
num.partitions=2
default.replication.factor=2
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
controlled.shutdown.enable=false
delete.topic.enable=true
inter.broker.protocol.version=3.0
li.protocol.bridge.mode.enable=true
EOF
}

bridge_39_config() {
  cat <<'EOF'
li.protocol.bridge.follower.recovery.enable=true
li.protocol.bridge.recommended.leader.election.enable=true
li.protocol.bridge.metadata.exclude.partitions.enable=true
li.protocol.bridge.move.controller.enable=true
li.protocol.bridge.shutdown.safety.override.enable=true
li.protocol.bridge.federated.topics.enable=true
li.protocol.bridge.reassignment.cancellation.safety.enable=true
li.min.original.alive.replicas=2
EOF
}

common_broker_config 0 "$BROKER_30_PORT" "$WORK_DIR/broker-30-data" >"$WORK_DIR/broker-30.properties"
{
  common_broker_config 0 "$BROKER_30_PORT" "$WORK_DIR/broker-30-data"
  bridge_39_config
} >"$WORK_DIR/broker-30-upgraded.properties"
{
  common_broker_config 1 "$BROKER_39_PORT" "$WORK_DIR/broker-39-data"
  bridge_39_config
} >"$WORK_DIR/broker-39.properties"
{
  common_broker_config 2 "$BROKER_39_EXTRA_PORT" "$WORK_DIR/broker-39-extra-data"
  bridge_39_config
} >"$WORK_DIR/broker-39-extra.properties"

"$KAFKA_39_HOME/bin/zookeeper-server-start.sh" "$WORK_DIR/zookeeper.properties" \
  >"$WORK_DIR/zookeeper.log" 2>&1 &
ZK_PID=$!
wait_until 60 "ZooKeeper startup" zk_responding

# Admit 3.9 only after the 3.0 bridge controller is active.
start_broker_30
wait_until 60 "3.0 controller election" controller_is 0
start_broker_39
wait_until 60 "both broker registrations" broker_registered 1
python3 "$SCRIPT_DIR/li_bridge_preflight.py" --phase mixed \
  --legacy-broker-config "$WORK_DIR/broker-30.properties" \
  --broker-config "$WORK_DIR/broker-39.properties" \
  --kafka-home "$KAFKA_39_HOME" --zk-connect "127.0.0.1:$ZK_PORT" \
  --output-json "$WORK_DIR/bridge-preflight-evidence.json" >/dev/null
echo "Mixed-phase configuration and ZooKeeper preflight passed"

java -cp "$WORK_DIR/metadata-scale-classes:$KAFKA_39_HOME/libs/*" \
  LiBridgeMetadataScaleSmoke "127.0.0.1:$BROKER_39_PORT" \
  "$SCALE_TOPIC_COUNT" "$SCALE_PARTITION_COUNT"
record_broker_resources mixed-metadata-loaded
cluster_command --create --topic bridge-smoke --partitions 2 --replication-factor 2
produce_phase phase-30-controller 20 "$KAFKA_30_HOME"
consume_count 20 "$KAFKA_30_HOME"
create_and_delete_topic controller-30
reassign_partition controller-30 '1,0'
exercise_private_apis controller-30

# A 3.9 controller must send bridge versions to the remaining 3.0 broker when it rejoins.
stop_broker_30
wait_until 60 "3.9 controller election" controller_is 1
start_broker_30
wait_until 90 "3.0 broker rejoin under 3.9 controller" broker_registered 0
produce_phase phase-39-controller 20 "$KAFKA_30_HOME"
consume_count 40 "$KAFKA_30_HOME"
create_and_delete_topic controller-39
reassign_partition controller-39 '0,1'
exercise_private_apis controller-39
exercise_old_transactional_and_group_client
exercise_old_streams
exercise_old_connect
record_broker_resources mixed-old-clients-complete
exercise_replica_recovery_both_directions
exercise_39_follower_truncation

# Cancel a throttled reassignment after failing over from the 3.9 controller to 3.0.
exercise_cancellation_across_failover
wait_until 90 "3.9 broker rejoin under 3.0 controller" broker_registered 1
produce_phase phase-30-controller-again 20 "$KAFKA_39_HOME"
consume_count 60 "$KAFKA_39_HOME"
create_and_delete_topic controller-30-again
wait_until 90 "all final mixed-mode partitions to become healthy" assert_healthy_partitions
record_broker_resources mixed-final

# Both binaries must have logged the common controller protocol while active controller.
grep -q 'LI protocol bridge mode enabled: LeaderAndIsr=v2, UpdateMetadata=v5, StopReplica=v1' \
  "$KAFKA_30_HOME/logs/controller.log"
grep -q 'LI protocol bridge mode enabled: LeaderAndIsr=v2, UpdateMetadata=v5, StopReplica=v1' \
  "$KAFKA_39_HOME/logs/controller.log"
echo "Mixed 3.0-li/3.9-li bridge phase passed"

# Replace the last old broker, bake briefly in all-3.9 bridge mode, and then cross the
# final rollback boundary into native Apache control requests while retaining IBP 3.0.
stop_broker_30
wait_until 60 "3.9 controller election for final binary replacement" controller_is 1
start_upgraded_broker_30
wait_until 120 "all-3.9 bridge-mode partitions to become healthy" assert_healthy_partitions
record_broker_resources all-39-bridge

"$KAFKA_39_HOME/bin/kafka-configs.sh" --bootstrap-server "127.0.0.1:$BROKER_39_PORT" \
  --entity-type brokers --entity-default --alter \
  --add-config li.protocol.bridge.mode.enable=false >/dev/null
wait_until 60 "native-mode dynamic configuration" native_mode_configured
create_and_delete_topic native-before-controller-roll
wait_until 60 "active controller to observe native mode" controller_logged_native_mode

# Restart the active controller so the replacement starts with empty queues in native mode.
stop_broker_39
wait_until 60 "upgraded broker 0 native controller election" controller_is 0
start_broker_39
wait_until 90 "broker 1 native-mode rejoin" broker_registered 1
wait_until 90 "native controller metadata to include both brokers" cluster_has_broker_count 2
create_and_delete_topic native-after-controller-roll
produce_phase phase-native-controller 10 "$KAFKA_39_HOME"
consume_count 70 "$KAFKA_39_HOME"
wait_until 120 "all final native-mode partitions to become healthy" assert_healthy_partitions
wait_until 60 "native mode to remain configured" native_mode_configured
record_broker_resources all-39-native

# Raise IBP only after the native-protocol bake, then roll both all-3.9 brokers separately.
# IBP is static in 3.9, so update the rendered files rather than attempting a dynamic alteration.
python3 - "$WORK_DIR/broker-30-upgraded.properties" "$WORK_DIR/broker-39.properties" <<'PY'
import pathlib
import sys
for name in sys.argv[1:]:
    path = pathlib.Path(name)
    config = path.read_text(encoding="utf-8")
    config = config.replace("inter.broker.protocol.version=3.0", "inter.broker.protocol.version=3.9")
    config = config.replace("li.protocol.bridge.mode.enable=true", "li.protocol.bridge.mode.enable=false")
    path.write_text(config, encoding="utf-8")
PY
stop_broker_39
start_broker_39
stop_broker_30
wait_until 60 "broker 1 controller election at IBP 3.9" controller_is 1
start_upgraded_broker_30
wait_until 90 "IBP 3.9 controller metadata to include both brokers" cluster_has_broker_count 2
create_and_delete_topic ibp-39
produce_phase phase-ibp-39 10 "$KAFKA_39_HOME"
consume_count 80 "$KAFKA_39_HOME"
wait_until 120 "all final IBP 3.9 partitions to become healthy" assert_healthy_partitions
record_broker_resources ibp-39-final
wait_until 60 "both restarted brokers to report IBP 3.9" broker_logs_show_ibp_39
wait_until 60 "native mode to remain configured at IBP 3.9" native_mode_configured
python3 "$SCRIPT_DIR/li_bridge_preflight.py" --phase native \
  --broker-config "$WORK_DIR/broker-30-upgraded.properties" \
  --broker-config "$WORK_DIR/broker-39.properties" \
  --kafka-home "$KAFKA_39_HOME" --zk-connect "127.0.0.1:$ZK_PORT" \
  --output-json "$WORK_DIR/native-preflight-evidence.json" >/dev/null
echo "Native IBP 3.9 configuration and ZooKeeper preflight passed"
assert_no_protocol_errors

echo "Mixed bridge, all-3.9 native protocol, and IBP 3.9 smoke test passed"
