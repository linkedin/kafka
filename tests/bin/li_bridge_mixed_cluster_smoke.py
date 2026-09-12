#!/usr/bin/env python3
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

"""Bounded, independently built ZK broker migration, rollback and old-client qualification.

Only disposable loopback listeners, a fresh ZooKeeper namespace and fresh log directories
are used. This is functional qualification, not production capacity or security approval.
"""

import contextlib
import csv
import base64
import datetime
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

from li_bridge_artifacts import file_sha256, snapshot_archive
from li_bridge_contract import CONFIG_METRICS, CONTRACT_VERSION, MODE, PHASES, TOPIC_CLEANUP, scenario_spec

SCRIPT_DIR = Path(__file__).resolve().parent


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_properties(path, values):
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")


def verify_connect_records(expected, actual):
    """File connectors are at-least-once; duplicates are counted, not confused with loss.

    All acknowledged source records must arrive unchanged and no unexpected records may appear.
    Broker/idempotent/transactional ledgers have the stricter no-duplicate assertions in Java.
    """
    if set(actual) - set(expected):
        raise AssertionError("Unexpected/corrupted Connect output")
    return set(actual) == set(expected)


class Migration:
    def __init__(self):
        self.scenario = scenario_spec()
        source_files = list(SCRIPT_DIR.glob("LiBridge*.java")) + [SCRIPT_DIR / name for name in (
            "li_bridge_mixed_cluster_smoke.py", "li_bridge_contract.py", "li_bridge_preflight.py", "li_bridge_artifacts.py")]
        self.source_hashes = {path.name: file_sha256(path) for path in source_files}
        self.started = utc_now()
        self.deadline = time.monotonic() + self.scenario["BRIDGE_SCENARIO_TIMEOUT_SECONDS"]
        self.work = Path(tempfile.mkdtemp(prefix="li-kafka-bridge-smoke.")).resolve()
        self.evidence = Path(os.environ.get("EVIDENCE_DIR", str(self.work / "evidence"))).resolve()
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.archives = {version: Path(os.environ[f"KAFKA_{version.replace('.', '')}_TGZ"]).resolve()
                         for version in ("3.0", "3.9")}
        self.homes = {}
        self.processes = {}
        self.handles = []
        self.generations = {}
        self.configs = {}
        self.sequence = 0
        self.connect_expected = []
        self.port = {0: int(os.environ.get("BROKER_30_PORT", 29092)),
                     1: int(os.environ.get("BROKER_39_PORT", 29093)),
                     2: int(os.environ.get("BROKER_39_EXTRA_PORT", 29094))}
        self.zk_port = int(os.environ.get("ZK_PORT", 22181))
        self.bootstrap = f"127.0.0.1:{self.port[0]},127.0.0.1:{self.port[1]}"
        self.timings = []
        self.resources = []
        self.commands = self.evidence / "process-commands.log"
        self.client_dir = self.work / "old-clients"
        self.client_dir.mkdir()
        self.churn_dir = self.work / "metadata-churn"
        self.churn_dir.mkdir()

    def remaining(self, requested):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Migration exceeded its scenario deadline")
        return min(requested, remaining)

    def command(self, args, check=True, input_text=None, timeout=None, allow_missing=False):
        limit = self.remaining(timeout or self.scenario["BRIDGE_COMMAND_TIMEOUT_SECONDS"])
        with self.commands.open("a", encoding="utf-8") as log:
            log.write(f"\n{utc_now()} {args!r}\n")
            # A separate process group lets a CLI timeout terminate its JVM too.
            process = subprocess.Popen(list(map(str, args)), stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
            try:
                output, _ = process.communicate(input_text, timeout=limit)
            except BaseException:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                output, _ = process.communicate()
                log.write(output)
                raise
            log.write(output)
        if check and process.returncode:
            raise subprocess.CalledProcessError(process.returncode, args, output=output[-8000:])
        return output if process.returncode == 0 or (allow_missing and "Node does not exist:" in output) else None

    def cli(self, script, *args, generation="3.9", check=True):
        return self.command([self.homes[generation] / "bin" / script, *args], check=check)

    def admin(self, script, *args, generation="3.9", check=True):
        return self.cli(script, "--bootstrap-server", self.bootstrap, *args, generation=generation, check=check)

    def java(self, helper, *args, generation="3.0"):
        return self.command(["java", "-cp", f"{self.work / ('classes-' + generation)}:{self.homes[generation] / 'libs'}/*",
                             helper, *map(str, args)])

    def start(self, name, args, extra_env=None):
        if name in self.processes:
            raise AssertionError(f"Process already running: {name}")
        log = (self.work / f"{name}.log").open("a", encoding="utf-8")
        self.handles.append(log)
        environment = dict(os.environ, **(extra_env or {}))
        environment.setdefault("KAFKA_HEAP_OPTS", "-Xms256m -Xmx1g")
        self.processes[name] = subprocess.Popen(list(map(str, args)), stdout=log, stderr=subprocess.STDOUT,
                                                stdin=subprocess.DEVNULL, env=environment, start_new_session=True)

    def stop(self, name, hard=False, cleanup=False):
        process = self.processes.pop(name, None)
        if process is None:
            return
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL if hard else signal.SIGTERM)
        try:
            process.wait(timeout=45 if not cleanup else 10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
            if not cleanup:
                raise TimeoutError(f"Graceful shutdown timed out: {name}")

    def until(self, label, predicate, timeout=120):
        start = time.monotonic()
        deadline = start + self.remaining(timeout)
        try:
            while time.monotonic() < deadline:
                for name in ("old-clients", "old-connect", "metadata-churn"):
                    clients = self.processes.get(name)
                    if clients is not None and clients.poll() is not None:
                        raise AssertionError(f"{name} exited unexpectedly: {clients.returncode}")
                if predicate():
                    self.timings.append((label, round(time.monotonic() - start, 3), "passed"))
                    print(f"{label}: passed", flush=True)
                    return
                time.sleep(1)
            raise TimeoutError(label)
        except BaseException:
            self.timings.append((label, round(time.monotonic() - start, 3), "failed"))
            raise

    def zk(self, command, timeout=30):
        return self.command([self.homes["3.9"] / "bin/zookeeper-shell.sh", f"127.0.0.1:{self.zk_port}"],
                            input_text=command + "\n", check=False, timeout=timeout, allow_missing=True) or ""

    def zookeeper_ready(self):
        process = self.processes["zookeeper"]
        if process.poll() is not None:
            raise AssertionError(f"ZooKeeper exited during startup: {process.returncode}")
        try:
            return "[zookeeper]" in self.zk("ls /", timeout=5)
        except subprocess.TimeoutExpired:
            # A socket may open before the server has loaded its database. Retry only
            # this startup probe; do not hide timeouts in the migration itself.
            return False

    def registered(self, identifier):
        return '"version"' in self.zk(f"get /brokers/ids/{identifier}")

    def controller(self, identifier):
        return f'"brokerid":{identifier}' in self.zk("get /controller")

    def start_broker(self, identifier, generation, mode=True, ibp="3.0"):
        config = {
            "broker.id": identifier, "listeners": f"PLAINTEXT://127.0.0.1:{self.port[identifier]}",
            "advertised.listeners": f"PLAINTEXT://127.0.0.1:{self.port[identifier]}",
            "listener.security.protocol.map": "PLAINTEXT:PLAINTEXT", "inter.broker.listener.name": "PLAINTEXT",
            "zookeeper.connect": f"127.0.0.1:{self.zk_port}", "log.dirs": self.work / f"data-{identifier}",
            "num.partitions": 2, "default.replication.factor": 2, "min.insync.replicas": 1,
            "offsets.topic.replication.factor": 2, "offsets.topic.num.partitions": 3,
            "transaction.state.log.replication.factor": 2, "transaction.state.log.num.partitions": 3,
            "transaction.state.log.min.isr": 1, "controlled.shutdown.enable": "true",
            "controlled.shutdown.max.retries": 3, "controlled.shutdown.retry.backoff.ms": 1000,
            "delete.topic.enable": "true", "log.segment.delete.delay.ms": 100,
            "inter.broker.protocol.version": ibp, MODE: str(mode).lower(),
            CONFIG_METRICS: "true", TOPIC_CLEANUP: "true",
            "remote.log.storage.system.enable": "false", "li.drop.corrupted.files.enable": "false",
            "li.leader.election.on.corruption.wait.ms": 0,
        }
        if generation == "3.0":
            config.update({"li.combined.control.request.enable": "true", "li.async.fetcher.enable": "true"})
        else:
            from li_bridge_contract import MIXED_REQUIRED_GATES
            config.update({gate: "true" for gate in MIXED_REQUIRED_GATES if gate != MODE})
            config.update({"li.protocol.bridge.reassignment.cancellation.safety.enable": "true",
                           "li.min.original.alive.replicas": 2})
        path = self.work / f"broker-{identifier}.properties"
        write_properties(path, config)
        self.configs[identifier] = config
        self.generations[identifier] = generation
        logs = self.work / f"broker-{identifier}-{generation}-logs"
        logs.mkdir(exist_ok=True)
        environment = {"LOG_DIR": str(logs)}
        if generation == "3.0" and os.environ.get("LI_BRIDGE_JAVA_30_HOME"):
            environment["JAVA_HOME"] = os.environ["LI_BRIDGE_JAVA_30_HOME"]
        self.start(f"broker-{identifier}", [self.homes[generation] / "bin/kafka-server-start.sh", path], environment)
        self.until(f"broker {identifier} {generation} registration", lambda: self.registered(identifier), 90)

    def replace(self, identifier, generation, mode=True, ibp="3.0", hard=False):
        self.stop(f"broker-{identifier}", hard=hard)
        self.until(f"broker {identifier} old registration removed", lambda: not self.registered(identifier), 60)
        self.start_broker(identifier, generation, mode, ibp)
        self.until(f"broker {identifier} replacement ISR recovery", self.healthy)

    def healthy(self):
        under = self.admin("kafka-topics.sh", "--describe", "--under-replicated-partitions", check=False)
        offline = self.admin("kafka-topics.sh", "--describe", "--unavailable-partitions", check=False)
        return under is not None and offline is not None and not under.strip() and not offline.strip()

    def force_controller(self, identifier):
        if not self.controller(identifier):
            other = 1 - identifier
            generation = self.generations[other]
            self.stop(f"broker-{other}")
            self.until(f"{self.generations[identifier]} controller election", lambda: self.controller(identifier), 60)
            self.start_broker(other, generation)
            self.until("controller movement ISR recovery", self.healthy)

    def create(self, topic, assignment="0:1", *extra):
        def attempt():
            described = self.admin("kafka-topics.sh", "--describe", "--topic", topic, check=False)
            if described is not None and f"Topic: {topic}" in described:
                return True
            return self.admin("kafka-topics.sh", "--create", "--topic", topic, "--replica-assignment", assignment,
                              *extra, check=False) is not None
        self.until(f"create {topic}", attempt, 90)

    def mutate(self, suffix):
        topic = "bridge-mutation-" + suffix
        for iteration in range(2):
            self.create(topic)
            self.admin("kafka-topics.sh", "--delete", "--topic", topic)
            def gone():
                names = self.admin("kafka-topics.sh", "--list", check=False)
                if names is None or topic in names.splitlines():
                    return False
                if "Node does not exist" not in self.zk(f"get /brokers/topics/{topic}"):
                    return False
                return not any(path.name.startswith(topic + "-") for path in self.work.glob("data-*/*") if path.is_dir())
            self.until(f"delete/recreate {topic} iteration {iteration}", gone)

    def set_mode(self, enabled):
        value = str(enabled).lower()
        self.admin("kafka-configs.sh", "--entity-type", "brokers", "--entity-default", "--alter",
                   "--add-config", f"{MODE}={value}")
        self.until("native-mode dynamic configuration" if not enabled else "bridge-mode dynamic configuration",
                   lambda: f"{MODE}={value}" in (self.admin("kafka-configs.sh", "--entity-type", "brokers",
                                                           "--entity-default", "--describe", "--all", check=False) or ""))
        for identifier, properties in self.configs.items():
            properties[MODE] = value
            write_properties(self.work / f"broker-{identifier}.properties", properties)

    def preflight(self, phase):
        args = [sys.executable, SCRIPT_DIR / "li_bridge_preflight.py", "--phase", phase,
                "--kafka-home", self.homes["3.9"], "--zk-connect", f"127.0.0.1:{self.zk_port}",
                "--output-json", self.evidence / f"preflight-{phase}.json"]
        for identifier in (0, 1):
            args.extend(["--legacy-broker-config" if self.generations[identifier] == "3.0" else "--broker-config",
                         self.work / f"broker-{identifier}.properties"])
        self.command(args)

    def checkpoint(self, phase):
        self.preflight(phase)
        self.sequence += 1
        churn_progress = self.churn_dir / "progress"
        initial_cycles = int(churn_progress.read_text()) if churn_progress.exists() else 0
        name = f"{self.sequence:03d}-{phase}"
        request = self.client_dir / "request.tmp"
        request.write_text(name)
        request.replace(self.client_dir / "request")
        record = f"connect-record-{name}"
        self.connect_expected.append(record)
        with (self.work / "connect-input.txt").open("a") as source:
            source.write(record + "\n")
        def clients_complete():
            result = self.client_dir / f"{name}.done"
            if not result.exists():
                return False
            report = json.loads(result.read_text())
            if report.get("passed") is not True:
                raise AssertionError(report)
            output = self.work / "connect-output.txt"
            return (output.exists() and verify_connect_records(self.connect_expected, output.read_text().splitlines())
                    and churn_progress.exists() and int(churn_progress.read_text()) > initial_cycles)
        self.until(f"old clients phase {phase}", clients_complete, 180)
        report = json.loads((self.client_dir / f"{name}.done").read_text())
        connect_records = (self.work / "connect-output.txt").read_text().splitlines()
        report.update(connect_pid=self.processes["old-connect"].pid,
                      connect_records=len(set(connect_records)),
                      connect_duplicate_deliveries=len(connect_records) - len(set(connect_records)),
                      metadata_churn_cycles=int(churn_progress.read_text()))
        (self.evidence / f"old-clients-{name}.json").write_text(json.dumps(report, indent=2, sort_keys=True))
        for generation in ("3.0", "3.9"):
            self.java("LiBridgePrivateApiSmoke", self.bootstrap, f"{name}-{generation.replace('.', '')}", generation=generation)
        self.mutate(name)
        self.until(f"{phase} metadata/ISR health", self.healthy)
        self.resources_at(phase)

    def resources_at(self, phase):
        for identifier in (0, 1, 2):
            process = self.processes.get(f"broker-{identifier}")
            if process is not None and process.poll() is None:
                rss = self.command(["ps", "-o", "rss=", "-p", process.pid]).strip()
                self.resources.append((utc_now(), phase, f"broker-{identifier}", process.pid, rss))
                if shutil.which("jcmd"):
                    heap = self.command(["jcmd", process.pid, "GC.heap_info"], check=False, timeout=15)
                    with (self.evidence / "broker-heap-info.log").open("a") as output:
                        output.write(f"\n{phase} broker-{identifier}\n{heap}\n")

    def start_clients(self):
        for topic in ("bridge-live", "bridge-live-tx", "bridge-live-stream-in", "bridge-live-stream-out", "bridge-live-connect"):
            self.create(topic, "0:1,1:0")
        self.start("old-clients", ["java", "-Xmx512m", "-cp",
                   f"{self.work / 'classes-3.0'}:{self.homes['3.0'] / 'libs'}/*", "LiBridgeContinuousClients",
                   self.bootstrap, self.client_dir, self.scenario["old_client_interval_ms"]])
        self.until("persistent old clients startup", lambda: (self.client_dir / "ready").exists())
        self.start("metadata-churn", ["java", "-Xmx256m", "-cp",
                   f"{self.work / 'classes-3.0'}:{self.homes['3.0'] / 'libs'}/*", "LiBridgeMetadataChurn",
                   self.bootstrap, self.churn_dir])
        (self.work / "connect-input.txt").touch()
        write_properties(self.work / "connect.properties", {
            # Do not expose the test worker's unauthenticated REST API on the host's network.
            "listeners": "http://127.0.0.1:0",
            "bootstrap.servers": self.bootstrap, "offset.storage.file.filename": self.work / "connect.offsets",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.storage.StringConverter", "offset.flush.interval.ms": 1000,
            "plugin.path": self.homes["3.0"] / "libs"})
        write_properties(self.work / "connect-source.properties", {
            "name": "bridge-old-file-source", "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
            "tasks.max": 1, "file": self.work / "connect-input.txt", "topic": "bridge-live-connect"})
        write_properties(self.work / "connect-sink.properties", {
            "name": "bridge-old-file-sink", "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max": 1, "file": self.work / "connect-output.txt", "topics": "bridge-live-connect"})
        self.start("old-connect", [self.homes["3.0"] / "bin/connect-standalone.sh", self.work / "connect.properties",
                                  self.work / "connect-source.properties", self.work / "connect-sink.properties"],
                   {"KAFKA_HEAP_OPTS": "-Xms128m -Xmx512m", "LOG_DIR": str(self.work / "connect-logs")})

    def recovery(self):
        count, size = self.scenario["RECOVERY_RECORD_COUNT"], self.scenario["RECOVERY_RECORD_SIZE"]
        self.create("bridge-recovery")
        self.stop("broker-1", hard=True)
        self.until("3.0 controller election for 3.9 recovery", lambda: self.controller(0), 60)
        self.java("LiBridgeRecords", "produce", self.bootstrap, "bridge-recovery", 0, count, size)
        self.start_broker(1, "3.9")
        self.until("3.9 follower recovery from a 3.0 leader", self.healthy)
        self.stop("broker-0", hard=True)
        self.until("3.9 controller election for old-follower recovery", lambda: self.controller(1), 60)
        # Promotion forces reads from the recovered new replica, not the old leader's original log.
        self.java("LiBridgeRecords", "verify", self.bootstrap, "bridge-recovery", 0, count, size)
        self.java("LiBridgeRecords", "produce", self.bootstrap, "bridge-recovery", count, count, size)
        self.start_broker(0, "3.0")
        self.until("3.0 follower recovery from a 3.9 leader", self.healthy)
        self.stop("broker-1", hard=True)
        self.until("old recovered replica promoted", lambda: self.controller(0), 60)
        self.java("LiBridgeRecords", "verify", self.bootstrap, "bridge-recovery", 0, 2 * count, size)
        self.start_broker(1, "3.9")
        self.until("recovery ledger final ISR", self.healthy)

    def execute_reassignment(self, path):
        # Continuous metadata churn may already own a different reassignment.
        self.admin("kafka-reassign-partitions.sh", "--reassignment-json-file", path,
                   "--execute", "--additional")

    def offline_name_reuse(self, offline, assigned_before_return):
        survivor = 1 - offline
        offline_generation = self.generations[offline]
        survivor_generation = self.generations[survivor]
        placement = "assigned-at-create" if assigned_before_return else "reassigned-after-return"
        topic = f"bridge-offline-name-reuse-{offline_generation}-{placement}"
        self.force_controller(survivor)
        self.create(topic, f"{survivor}:{offline}")
        self.java("LiBridgeRecords", "produce", self.bootstrap, topic, 0, 1, 64)
        self.stop(f"broker-{offline}", hard=True)
        self.until(f"{offline_generation} former replica offline for name reuse", lambda: not self.registered(offline), 60)

        def reassign(replicas):
            path = self.work / "offline-name-reuse-reassignment.json"
            path.write_text(json.dumps({"version": 1, "partitions": [
                {"topic": topic, "partition": 0, "replicas": replicas}]}))
            self.execute_reassignment(path)
            def complete():
                description = self.admin("kafka-topics.sh", "--describe", "--topic", topic, check=False) or ""
                match = re.search(r"Replicas:\s+([0-9,]+)\s+Isr:\s+([0-9,]+)", description)
                if not match:
                    return False
                assigned = list(map(int, match[1].split(",")))
                in_sync = set(map(int, match[2].split(",")))
                return assigned == replicas and in_sync == set(replicas)
            self.until(f"offline name-reuse assignment {replicas}", complete)

        reassign([survivor])
        self.admin("kafka-topics.sh", "--delete", "--topic", topic)
        self.until("name deleted while former replica offline", lambda:
                   topic not in (self.admin("kafka-topics.sh", "--list", check=False) or "").splitlines() and
                   "Node does not exist" in self.zk(f"get /brokers/topics/{topic}"))
        assignment = f"{survivor}:{offline}" if assigned_before_return else str(survivor)
        self.create(topic, assignment)
        self.java("LiBridgeRecords", "produce", self.bootstrap, topic, 0, 2, 128)
        self.java("LiBridgeRecords", "verify", self.bootstrap, topic, 0, 2, 128)
        self.start_broker(offline, offline_generation)
        self.until("rejoined broker ISR recovery", self.healthy)
        if not assigned_before_return:
            reassign([survivor, offline])
        self.stop(f"broker-{survivor}", hard=True)
        self.until("rejoined replica promoted", lambda: f"Leader: {offline}" in
                   (self.admin("kafka-topics.sh", "--describe", "--topic", topic, check=False) or ""), 60)
        # Exit on a changed record immediately; retrying could hide old bytes.
        self.until(f"offline name-reuse {offline_generation} {placement} records verified after promotion", lambda:
                   self.java("LiBridgeRecords", "verify", self.bootstrap, topic, 0, 2, 128) is not None)
        self.start_broker(survivor, survivor_generation)
        self.until("offline name-reuse final ISR recovery", self.healthy)

    def configure_cancellation_throttle(self):
        # CLI --throttle also rewrites topic throttles for unrelated active moves.
        # Restrict topic selectors and keep the fixture's 1024-byte/s broker rates.
        self.admin("kafka-configs.sh", "--entity-type", "topics", "--entity-name", "bridge-cancel", "--alter",
                   "--add-config", "leader.replication.throttled.replicas=[0:0,0:1],"
                   "follower.replication.throttled.replicas=[0:2]")
        for identifier in (0, 1, 2):
            self.admin("kafka-configs.sh", "--entity-type", "brokers", "--entity-name", str(identifier), "--alter",
                       "--add-config", "leader.replication.throttled.rate=1024,follower.replication.throttled.rate=1024")

    def cancellation(self):
        self.force_controller(1)
        self.start_broker(2, "3.9")
        self.create("bridge-cancel")
        self.java("LiBridgeRecords", "produce", self.bootstrap, "bridge-cancel", 0, 100, 100000)
        path = self.work / "reassignment.json"
        path.write_text(json.dumps({"version": 1, "partitions": [{"topic": "bridge-cancel", "partition": 0, "replicas": [1, 2]}]}))
        self.configure_cancellation_throttle()
        self.execute_reassignment(path)
        self.until("throttled reassignment to begin", lambda: "is still in progress" in
                   (self.admin("kafka-reassign-partitions.sh", "--reassignment-json-file", path, "--verify", check=False) or ""), 30)
        self.stop("broker-1", hard=True)
        self.stop("broker-2", hard=True)
        self.until("3.0 controller election during reassignment", lambda: self.controller(0), 60)
        self.start_broker(1, "3.9")
        self.admin("kafka-reassign-partitions.sh", "--reassignment-json-file", path, "--cancel", "--preserve-throttles")
        self.until("reassignment cancellation to restore original replicas", lambda: re.search(
            r"Partition: 0.*Replicas: (0,1|1,0)(?:\s|$)",
            self.admin("kafka-topics.sh", "--describe", "--topic", "bridge-cancel", check=False) or ""))
        self.java("LiBridgeRecords", "verify", self.bootstrap, "bridge-cancel", 0, 100, 100000)

    def truncation(self):
        self.create("bridge-truncation", "1:0", "--config", "unclean.leader.election.enable=true")
        self.until("truncation setup ISR", self.healthy)
        self.stop("broker-0", hard=True)
        self.until("truncation leader 3.9", lambda: self.controller(1), 60)
        self.java("LiBridgeRecords", "produce", self.bootstrap, "bridge-truncation", 0, 50, 100000)
        self.stop("broker-1", hard=True)
        self.start_broker(0, "3.0")
        self.until("stale 3.0 unclean leader", lambda: "Leader: 0" in
                   (self.admin("kafka-topics.sh", "--describe", "--topic", "bridge-truncation", check=False) or ""))
        self.start_broker(1, "3.9")
        self.until("3.9 follower truncation and ISR recovery", self.healthy)
        log = "\n".join(path.read_text(errors="replace") for path in self.work.glob("broker-1-3.9-logs/*.log"))
        if not re.search(r"Truncating (partition )?bridge-truncation-0", log):
            raise AssertionError("No evidence of new follower truncation to old stale leader")
        # Only this disposable topic permits unclean loss. The persistent client ledger must still pass.

    def prepare(self):
        for generation, archive in list(self.archives.items()):
            # Retain immutable inputs and their source metadata, including in public CI.
            manifest = snapshot_archive(archive, generation, self.evidence / "archives")
            (self.evidence / f"archive-{generation}.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
            archive = Path(manifest["path"])
            self.archives[generation] = archive
            destination = self.work / f"extract-{generation}"
            destination.mkdir()
            with tarfile.open(archive, "r:gz") as source:
                source.extractall(destination)
            homes = [path for path in destination.iterdir() if path.is_dir()]
            if len(homes) != 1:
                raise ValueError("Expected exactly one Kafka distribution root")
            self.homes[generation] = homes[0]
            classes = self.work / f"classes-{generation}"
            classes.mkdir()
            helpers = ["LiBridgePrivateApiSmoke.java", "LiBridgeRecords.java",
                       "LiBridgeMetadataChurn.java", "LiBridgeMetadataChurnRetryTest.java"]
            if generation == "3.0":
                helpers.append("LiBridgeContinuousClients.java")
            else:
                helpers.extend(["LiBridgeMetadataScaleSmoke.java", "LiBridgeLiveInventory.java", "LiBridgeRuntimeProbe.java"])
            self.command(["javac", "--release", "11", "-cp", f"{homes[0] / 'libs'}/*", "-d", classes,
                          *[SCRIPT_DIR / name for name in helpers]])
            self.java("LiBridgeMetadataChurnRetryTest", generation=generation)
        self.java("LiBridgeContinuousClients", "--self-test")
        self.java("LiBridgeRuntimeProbe", self.evidence / "packaged-runtime-39.json", "false", generation="3.9")
        write_properties(self.work / "zookeeper.properties", {"clientPort": self.zk_port,
                         "dataDir": self.work / "zk-data", "maxClientCnxns": 0, "admin.enableServer": "false"})
        self.start("zookeeper", [self.homes["3.9"] / "bin/zookeeper-server-start.sh", self.work / "zookeeper.properties"])
        self.until("ZooKeeper startup", self.zookeeper_ready, 60)

    def interrupted_deletion_recovery(self, generation):
        # Seed the state left when recursive deletion stopped after removing the
        # partition's leader/ISR znode, but before removing its topic parent.
        topic = f"bridge-interrupted-delete-{generation}"
        for path in ("/brokers", "/brokers/topics", "/admin", "/admin/delete_topics"):
            self.zk(f"create {path}")
        assignment = {"version": 3, "topic_id": base64.urlsafe_b64encode(os.urandom(16)).decode().rstrip("="),
                      "partitions": {"0": [1, 0]}, "adding_replicas": {"0": [1]}, "removing_replicas": {"0": [0]}}
        for path, data in ((f"/brokers/topics/{topic}", json.dumps(assignment, separators=(",", ":"))),
                           (f"/admin/delete_topics/{topic}", "")):
            if f"Created {path}" not in self.zk(f"create {path} {data}"):
                raise AssertionError(f"Could not seed interrupted deletion: {path}")
        self.start_broker(1, generation, mode=False)
        self.start_broker(0, generation, mode=False)
        self.until(f"interrupted deletion {generation} assignment removed", lambda:
                   "Node does not exist" in self.zk(f"get /brokers/topics/{topic}"), 60)
        self.create(topic, "1")
        self.java("LiBridgeRecords", "produce", self.bootstrap, topic, 0, 2, 128)
        self.java("LiBridgeRecords", "verify", self.bootstrap, topic, 0, 2, 128)
        self.timings.append((f"interrupted deletion {generation} records verified", 0, "passed"))
        self.admin("kafka-topics.sh", "--delete", "--topic", topic)
        self.until(f"interrupted deletion {generation} fixture removed", lambda:
                   "Node does not exist" in self.zk(f"get /brokers/topics/{topic}"), 60)
        self.stop("broker-0")
        self.stop("broker-1")

    def native_offline_deletion(self):
        topic = "bridge-native-offline-delete"
        self.create(topic, "0")
        self.java("LiBridgeRecords", "produce", self.bootstrap, topic, 0, 1, 64)
        def cached():
            listing = self.command([self.homes["3.9"] / "bin/kafka-topics.sh", "--bootstrap-server",
                                    f"127.0.0.1:{self.port[1]}", "--list"], timeout=20)
            return topic in listing.splitlines()
        self.until("native offline deletion initial metadata", cached)
        self.stop("broker-0", hard=True)
        self.until("native offline deletion controller failover", lambda:
                   not self.registered(0) and self.controller(1), 60)
        # Queue deletion without waiting on an Admin future for the offline replica.
        self.zk(f"create /admin/delete_topics/{topic}")
        self.until("native offline deletion metadata tombstone", lambda: not cached(), 45)
        if "Node does not exist" in self.zk(f"get /brokers/topics/{topic}"):
            raise AssertionError("Assignment deleted before the offline replica acknowledged removal")
        self.timings.append(("native offline deletion retains assignment until acknowledgement", 0, "passed"))
        self.start_broker(0, "3.0", mode=False)
        self.until("native offline deletion removes assignment after acknowledgement", lambda:
                   "Node does not exist" in self.zk(f"get /brokers/topics/{topic}"), 60)
        self.create(topic, "0")
        self.java("LiBridgeRecords", "produce", self.bootstrap, topic, 0, 2, 128)
        self.java("LiBridgeRecords", "verify", self.bootstrap, topic, 0, 2, 128)
        self.timings.append(("native offline deletion recreated records verified", 0, "passed"))
        self.admin("kafka-topics.sh", "--delete", "--topic", topic)
        self.until("native offline deletion final ISR recovery", self.healthy)

    def run(self):
        self.prepare()
        for generation in ("3.0", "3.9"):
            self.interrupted_deletion_recovery(generation)
        self.start_broker(0, "3.0", mode=False)
        self.until("3.0 controller election", lambda: self.controller(0), 60)
        self.start_broker(1, "3.0", mode=False)
        self.java("LiBridgeMetadataScaleSmoke", self.bootstrap, self.scenario["SCALE_TOPIC_COUNT"],
                  self.scenario["SCALE_PARTITION_COUNT"], generation="3.9")
        self.native_offline_deletion()
        self.start_clients()
        self.checkpoint("dormant")
        self.set_mode(True)
        # Clients continue while both old brokers roll; the active controller necessarily restarts.
        self.replace(1, "3.0")
        self.replace(0, "3.0")
        self.checkpoint("legacy-bridge")
        # Rehearse all-3.0 mode backout too, with metadata churn and old clients still active.
        self.set_mode(False)
        self.replace(1, "3.0", mode=False)
        self.replace(0, "3.0", mode=False)
        self.checkpoint("dormant")
        self.set_mode(True)
        self.replace(1, "3.0")
        self.replace(0, "3.0")
        self.checkpoint("legacy-bridge")
        self.force_controller(0)
        self.replace(1, "3.9")
        self.checkpoint("mixed")
        self.resources_at("mixed-metadata-loaded")
        self.force_controller(1)
        self.checkpoint("mixed")
        self.replace(1, "3.0")
        self.until("canary rollback to 3.0", self.healthy)
        self.checkpoint("legacy-bridge")
        self.replace(1, "3.9")
        self.force_controller(1)
        self.stop("broker-1", hard=True)
        self.until("hard controller recovery", lambda: self.controller(0), 60)
        self.start_broker(1, "3.9")
        self.checkpoint("mixed")
        self.resources_at("mixed-old-clients-complete")
        self.recovery()
        for offline in (0, 1):
            for assigned_before_return in (False, True):
                self.offline_name_reuse(offline, assigned_before_return)
        self.cancellation()
        self.truncation()
        self.checkpoint("mixed")
        self.until("all final mixed-mode partitions to become healthy", self.healthy)
        self.resources_at("mixed-final")
        self.replace(0, "3.9")
        self.checkpoint("all-39-bridge")
        # Downgrade both brokers on the same directories, after 3.9 coordinated groups/transactions.
        self.replace(1, "3.0")
        self.checkpoint("mixed")
        self.replace(0, "3.0")
        self.until("all-3.9 rollback to 3.0", self.healthy)
        self.checkpoint("legacy-bridge")
        self.replace(1, "3.9")
        self.checkpoint("mixed")
        self.replace(0, "3.9")
        self.checkpoint("all-39-bridge")
        self.until("all-3.9 bridge-mode partitions to become healthy", self.healthy)
        self.resources_at("all-39-bridge")
        self.set_mode(False)
        self.mutate("native-before-controller-roll")
        self.replace(0, "3.9", mode=False)
        self.replace(1, "3.9", mode=False)
        self.checkpoint("native")
        self.until("all final native-mode partitions to become healthy", self.healthy)
        self.resources_at("all-39-native")
        self.replace(1, "3.9", mode=False, ibp="3.9")
        self.replace(0, "3.9", mode=False, ibp="3.9")
        self.checkpoint("ibp-39")
        self.until("all final IBP 3.9 partitions to become healthy", self.healthy)
        self.resources_at("ibp-39-final")
        (self.client_dir / "stop").touch()
        (self.churn_dir / "stop").touch()
        self.processes["old-clients"].wait(timeout=90)
        if self.processes["old-clients"].returncode:
            raise AssertionError("Old-client final ledger failed")
        self.processes.pop("old-clients")
        self.verify_protocol_logs()

    def verify_protocol_logs(self):
        selected = []
        for generation in ("3.0", "3.9"):
            # Hourly rotation must not erase a protocol decision or hide an error.
            paths = sorted(self.work.glob(f"broker-*-{generation}-logs/controller.log*"))
            logs = "\n".join(path.read_text(errors="replace") for path in paths)
            enabled = "LI protocol bridge mode enabled: LeaderAndIsr=v2, UpdateMetadata=v5, StopReplica=v1"
            if enabled not in logs:
                raise AssertionError(f"Missing {generation} controller bridge selection")
            selected.extend(line for line in logs.splitlines() if "LI protocol bridge mode" in line)
        (self.evidence / "protocol-selection.log").write_text("\n".join(selected))
        if not any("LI protocol bridge mode disabled" in line for line in selected):
            raise AssertionError("Native control selection missing")
        for path in list(self.work.glob("broker-*.log*")) + list(self.work.glob("broker-*-logs/*.log*")):
            if re.search(r"UnsupportedVersionException|Error parsing.*(?:LeaderAndIsr|UpdateMetadata|StopReplica)|unknown api key",
                         path.read_text(errors="replace"), re.IGNORECASE):
                raise AssertionError(f"Protocol error in {path}")

    def diagnostics(self):
        """Capture live failure state before cleanup destroys the blocked threads/sessions."""
        for name, process in self.processes.items():
            if process.poll() is None and shutil.which("jcmd"):
                with contextlib.suppress(Exception):
                    output = self.command(["jcmd", process.pid, "Thread.print"], check=False, timeout=10)
                    (self.evidence / f"threads-{name}.txt").write_text(output or "jcmd failed")
        with contextlib.suppress(Exception):
            for path in ("/controller", "/admin/delete_topics", "/brokers/topics/bridge-continuous-mutation"):
                output = self.zk(f"get {path}")
                (self.evidence / ("znode-" + path.strip("/").replace("/", "-") + ".txt")).write_text(output)
        for name in ("stage", "progress"):
            path = self.churn_dir / name
            if path.exists():
                shutil.copyfile(path, self.evidence / f"metadata-churn-{name}.txt")

    def finish(self, passed):
        if not passed:
            self.diagnostics()
        source_unchanged = all(path.is_file() and file_sha256(path) == digest
                               for name, digest in self.source_hashes.items() for path in [SCRIPT_DIR / name])
        passed = passed and source_unchanged
        for name in sorted(self.processes, key=lambda value: value == "zookeeper"):
            with contextlib.suppress(Exception):
                self.stop(name, cleanup=True)
        for handle in self.handles:
            handle.close()
        for filename, columns, rows in (
                ("timings.tsv", ("operation", "duration_seconds", "result"), self.timings),
                ("broker-resources.tsv", ("timestamp_utc", "phase", "process", "pid", "rss_kib"), self.resources)):
            with (self.evidence / filename).open("w", newline="") as output:
                writer = csv.writer(output, delimiter="\t")
                writer.writerow(columns)
                writer.writerows(rows)
        suite = ET.Element("testsuite", name="li-bridge-process", tests=str(len(self.timings) + 1),
                           failures=str(sum(result != "passed" for _, _, result in self.timings) + (not passed)))
        for label, seconds, result in self.timings + [("complete-migration", 0, "passed" if passed else "failed")]:
            case = ET.SubElement(suite, "testcase", name=label, classname="LiBridgeMigration", time=str(seconds))
            if result != "passed":
                ET.SubElement(case, "failure", message="See retained process-commands.log and process-logs.tgz")
        ET.ElementTree(suite).write(self.evidence / "TEST-li-bridge-process.xml", encoding="utf-8", xml_declaration=True)
        summary = {"contract_version": CONTRACT_VERSION, "passed": passed, "exit_status": 0 if passed else 1,
                   "started_utc": self.started, "finished_utc": utc_now(), "scenario": self.scenario,
                   "source_sha256": self.source_hashes, "source_unchanged": source_unchanged,
                   "archives": {generation: {"path": str(path), "sha256": file_sha256(path)}
                                for generation, path in self.archives.items()},
                   "scale": {"topic_count": self.scenario["SCALE_TOPIC_COUNT"], "partitions_per_topic": self.scenario["SCALE_PARTITION_COUNT"]},
                   "recovery": {"record_count": self.scenario["RECOVERY_RECORD_COUNT"], "record_size": self.scenario["RECOVERY_RECORD_SIZE"]}}
        (self.evidence / "run-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
        if os.environ.get("EVIDENCE_INCLUDE_LOGS", "1") == "1":
            with tarfile.open(self.evidence / "process-logs.tgz", "w:gz") as archive:
                for path in self.work.rglob("*.log*"):
                    if not path.is_relative_to(self.evidence):
                        archive.add(path, arcname=str(path.relative_to(self.work)))
        # Preserve failures and default in-work evidence. Never silently erase the only proof.
        if passed and os.environ.get("KEEP_WORK_DIR") != "1" and not self.evidence.is_relative_to(self.work):
            shutil.rmtree(self.work)
        print(f"Evidence: {self.evidence}; work: {self.work}", flush=True)
        return passed


def main():
    migration = Migration()
    passed = False
    def interrupted(_signum, _frame):
        raise InterruptedError("Migration interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        migration.run()
        passed = True
    finally:
        passed = migration.finish(passed)
    if not passed:
        raise RuntimeError("Scenario source changed during execution; evidence is invalid")
    print("Bridge activation, mixed binaries, persisted rollback, native/IBP and unchanged old clients passed")


if __name__ == "__main__":
    main()
