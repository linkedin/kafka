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

"""Validate rendered broker configuration and live ZooKeeper state before a bridge transition."""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


BRIDGE_GATES: Tuple[str, ...] = (
    "li.protocol.bridge.mode.enable",
    "li.protocol.bridge.follower.recovery.enable",
    "li.protocol.bridge.recommended.leader.election.enable",
    "li.protocol.bridge.metadata.exclude.partitions.enable",
    "li.protocol.bridge.move.controller.enable",
    "li.protocol.bridge.shutdown.safety.override.enable",
    "li.protocol.bridge.preferred.controller.enable",
    "li.protocol.bridge.federated.topics.enable",
    "li.protocol.bridge.rack.id.mapper.enable",
    "li.protocol.bridge.dynamic.topic.deletion.enable",
    "li.protocol.bridge.produce.request.instrumentation.enable",
    "li.protocol.bridge.request.metric.buckets.enable",
    "li.protocol.bridge.request.channel.watchdog.enable",
    "li.protocol.bridge.minimum.log.roll.enable",
    "li.protocol.bridge.reassignment.cancellation.safety.enable",
    "li.protocol.bridge.list.offsets.instrumentation.enable",
    "li.protocol.bridge.static.default.quotas.enable",
    "li.protocol.bridge.replica.request.timeout.enable",
    "li.protocol.bridge.offsets.topic.config.enable",
    "li.protocol.bridge.leader.transfer.on.isr.shrink.enable",
    "li.protocol.bridge.legacy.request.metrics.enable",
    "li.protocol.bridge.log.truncation.metrics.enable",
)

MIXED_REQUIRED_GATES: Tuple[str, ...] = (
    "li.protocol.bridge.mode.enable",
    "li.protocol.bridge.follower.recovery.enable",
    "li.protocol.bridge.recommended.leader.election.enable",
    "li.protocol.bridge.metadata.exclude.partitions.enable",
    "li.protocol.bridge.move.controller.enable",
    "li.protocol.bridge.shutdown.safety.override.enable",
    "li.protocol.bridge.federated.topics.enable",
)

INSPECTED_ZK_PATHS: Tuple[str, ...] = (
    "/brokers/ids",
    "/brokers/corrupted",
    "/brokers/shutdown",
    "/brokers/preferred_controllers",
    "/federatedTopics",
    "/topic_deletion_flag",
)


def parse_properties(path: Path) -> Dict[str, str]:
    """Parse the simple key/value subset used by rendered Kafka properties."""
    properties: Dict[str, str] = {}
    logical_lines: List[str] = []
    pending = ""
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        pending += line
        if pending.endswith("\\") and not pending.endswith("\\\\"):
            pending = pending[:-1]
            continue
        logical_lines.append(pending)
        pending = ""
    if pending:
        logical_lines.append(pending)

    for line in logical_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "!")):
            continue
        match = re.match(r"([^:=\s]+)\s*(?:=|:)\s*(.*)$", stripped)
        if not match:
            match = re.match(r"([^\s]+)\s+(.*)$", stripped)
        if not match:
            raise ValueError(f"Unable to parse property line in {path}: {line!r}")
        properties[match.group(1)] = match.group(2).strip()
    return properties


def boolean_value(properties: Dict[str, str], key: str) -> Optional[bool]:
    value = properties.get(key)
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"{key} must be true or false, found {value!r}")


def protocol_version(value: str) -> Tuple[int, int]:
    match = re.match(r"^(\d+)\.(\d+)", value.strip())
    if not match:
        raise ValueError(f"Unsupported inter.broker.protocol.version value {value!r}")
    return int(match.group(1)), int(match.group(2))


def inspect_config(path: Path, properties: Dict[str, str], phase: str,
                   require_all_gates: bool, generation: str = "3.9") -> Tuple[List[str], Dict[str, object]]:
    issues: List[str] = []
    details: Dict[str, object] = {
        "path": str(path), "broker_id": properties.get("broker.id"), "generation": generation
    }

    if properties.get("process.roles", "").strip():
        issues.append(f"{path}: process.roles must be empty for the ZooKeeper bridge")

    ibp = properties.get("inter.broker.protocol.version")
    if ibp is None:
        issues.append(f"{path}: inter.broker.protocol.version is missing")
    else:
        try:
            parsed_ibp = protocol_version(ibp)
            details["inter_broker_protocol_version"] = ibp
            if phase == "mixed" and parsed_ibp >= (3, 2):
                issues.append(f"{path}: bridge mode requires IBP older than 3.2, found {ibp}")
        except ValueError as error:
            issues.append(f"{path}: {error}")

    expected_mode = phase == "mixed"
    try:
        actual_mode = boolean_value(properties, BRIDGE_GATES[0])
        details["bridge_mode"] = actual_mode
        if actual_mode is None:
            issues.append(f"{path}: {BRIDGE_GATES[0]} is missing")
        elif actual_mode != expected_mode:
            issues.append(f"{path}: {BRIDGE_GATES[0]} must be {str(expected_mode).lower()} for {phase} phase")
    except ValueError as error:
        issues.append(f"{path}: {error}")

    required_gates: Sequence[str]
    if generation == "3.0":
        required_gates = (BRIDGE_GATES[0],)
    else:
        required_gates = BRIDGE_GATES if require_all_gates else MIXED_REQUIRED_GATES
    if phase == "mixed":
        for gate in required_gates:
            try:
                if boolean_value(properties, gate) is not True:
                    issues.append(f"{path}: required mixed-phase gate {gate} is not true")
            except ValueError as error:
                issues.append(f"{path}: {error}")

    for unsafe_key in ("li.combined.control.request.enable", "li.async.fetcher.enable"):
        try:
            if boolean_value(properties, unsafe_key) is True:
                issues.append(f"{path}: unsupported 3.9 behavior {unsafe_key} is enabled")
        except ValueError as error:
            issues.append(f"{path}: {error}")

    try:
        if boolean_value(properties, "remote.log.storage.system.enable") is True:
            issues.append(f"{path}: remote storage is enabled; a separate tiered-storage plan is required")
        if boolean_value(properties, "li.drop.corrupted.files.enable") is True:
            issues.append(f"{path}: corrupted-file dropping is enabled and requires an explicit migration decision")
    except ValueError as error:
        issues.append(f"{path}: {error}")

    details["configured_bridge_gates"] = {
        gate: boolean_value(properties, gate) for gate in BRIDGE_GATES if gate in properties
    }
    return issues, details


def zk_command(kafka_home: Path, zk_connect: str, command: str) -> str:
    shell = kafka_home / "bin" / "zookeeper-shell.sh"
    if not shell.is_file():
        raise ValueError(f"ZooKeeper shell not found at {shell}")
    completed = subprocess.run(
        [str(shell), zk_connect], input=command + "\n", text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"ZooKeeper command {command!r} failed: {completed.stdout.strip()}")
    return completed.stdout


def parse_children(output: str) -> Optional[List[str]]:
    if "Node does not exist" in output:
        return None
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            body = stripped[1:-1].strip()
            return [] if not body else [item.strip() for item in body.split(",")]
    raise ValueError(f"Unable to find ZooKeeper child list in output: {output!r}")


def inspect_zookeeper(kafka_home: Path, zk_connect: str,
                      configured_broker_ids: Sequence[str]) -> Tuple[List[str], Dict[str, object]]:
    issues: List[str] = []
    state: Dict[str, object] = {}
    for path in INSPECTED_ZK_PATHS:
        command = f"ls {path}" if path != "/topic_deletion_flag" else f"get {path}"
        output = zk_command(kafka_home, zk_connect, command)
        if path == "/topic_deletion_flag":
            state[path] = "missing" if "Node does not exist" in output else "present"
        else:
            state[path] = parse_children(output)

    live_ids = state["/brokers/ids"] or []
    expected_ids = sorted(identifier for identifier in configured_broker_ids if identifier is not None)
    if expected_ids and sorted(live_ids) != expected_ids:
        issues.append(f"Live ZooKeeper broker IDs {sorted(live_ids)} do not match rendered configs {expected_ids}")
    corrupted = state["/brokers/corrupted"] or []
    if corrupted:
        issues.append(f"ZooKeeper contains corrupted-broker state: {corrupted}")
    return issues, state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-config", action="append", default=[], type=Path,
                        help="Rendered effective 3.9 broker properties; repeat for every 3.9 broker")
    parser.add_argument("--legacy-broker-config", action="append", default=[], type=Path,
                        help="Rendered effective 3.0 bridge properties; repeat for every old broker")
    parser.add_argument("--phase", choices=("mixed", "native"), required=True)
    parser.add_argument("--require-all-gates", action="store_true",
                        help="Require all 22 wrapper compatibility gates during the mixed phase")
    parser.add_argument("--zk-connect", help="Live ZooKeeper connection string")
    parser.add_argument("--kafka-home", type=Path,
                        help="Kafka distribution containing bin/zookeeper-shell.sh")
    parser.add_argument("--output-json", type=Path, help="Write machine-readable evidence to this path")
    args = parser.parse_args()

    if bool(args.zk_connect) != bool(args.kafka_home):
        parser.error("--zk-connect and --kafka-home must be supplied together")
    if not args.broker_config and not args.legacy_broker_config:
        parser.error("at least one --broker-config or --legacy-broker-config is required")
    if args.phase == "native" and args.legacy_broker_config:
        parser.error("native phase cannot contain legacy broker configs")

    issues: List[str] = []
    config_details: List[Dict[str, object]] = []
    configured_ids: List[str] = []
    config_inputs = [(path, "3.9") for path in args.broker_config]
    config_inputs.extend((path, "3.0") for path in args.legacy_broker_config)
    for config_path, generation in config_inputs:
        try:
            properties = parse_properties(config_path)
            config_issues, details = inspect_config(
                config_path, properties, args.phase, args.require_all_gates, generation)
            issues.extend(config_issues)
            config_details.append(details)
            if properties.get("broker.id") is not None:
                configured_ids.append(properties["broker.id"])
        except (OSError, ValueError) as error:
            issues.append(str(error))

    zk_state: Optional[Dict[str, object]] = None
    if args.zk_connect and args.kafka_home:
        try:
            zk_issues, zk_state = inspect_zookeeper(args.kafka_home, args.zk_connect, configured_ids)
            issues.extend(zk_issues)
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            issues.append(str(error))

    evidence = {
        "phase": args.phase,
        "passed": not issues,
        "broker_configs": config_details,
        "zookeeper_state": zk_state,
        "issues": issues,
    }
    rendered = json.dumps(evidence, indent=2, sort_keys=True)
    print(rendered)
    if args.output_json:
        args.output_json.write_text(rendered + "\n", encoding="utf-8")
    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())
