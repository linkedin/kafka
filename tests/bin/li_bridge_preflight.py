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
import datetime
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


# Also supports importlib-based unit-test loading without installing a Python package.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from li_bridge_contract import (BRIDGE_GATES, CONTRACT_VERSION, MIXED_REQUIRED_GATES, MODE,
                                PHASES, inventory_issues, required_gates, transition_allowed)

INSPECTED_ZK_PATHS: Tuple[str, ...] = (
    "/brokers/ids",
    "/brokers/corrupted",
    "/brokers/shutdown",
    "/brokers/preferred_controllers",
    "/federatedTopics",
    "/topic_deletion_flag",
)

ALLOWED_DISPOSITIONS: Dict[str, Tuple[str, ...]] = {
    **dict.fromkeys(INSPECTED_ZK_PATHS, ("retained", "unused")),
    "remote-storage": ("unused",),
    "plugin-state": ("unused", "qualified"),
    "client-floor": ("qualified-unchanged",),
    "artifact-admission": ("bridge-artifacts-only",),
}


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
    match = re.fullmatch(r"(\d+)\.(\d+)(?:-IV(\d+))?", value.strip())
    if not match:
        raise ValueError(f"Unsupported inter.broker.protocol.version value {value!r}")
    version = int(match.group(1)), int(match.group(2))
    max_internal_version = {(3, 0): 1, (3, 9): 0}.get(version)
    if match.group(3) is not None and max_internal_version is not None:
        if int(match.group(3)) > max_internal_version:
            raise ValueError(f"Unsupported inter.broker.protocol.version value {value!r}")
    return version


def inspect_config(path: Path, properties: Dict[str, str], phase: str,
                   require_all_gates: bool, generation: str = "3.9") -> Tuple[List[str], Dict[str, object]]:
    issues: List[str] = []
    details: Dict[str, object] = {
        "path": str(path), "broker_id": None, "generation": generation
    }
    expected_ibp, expected_mode, generations = PHASES[phase]
    if generation not in generations:
        issues.append(f"{path}: {phase} does not permit generation {generation}")
    broker_id = properties.get("broker.id", "").strip()
    if not re.fullmatch(r"[0-9]+", broker_id) or int(broker_id) > 2147483647:
        issues.append(f"{path}: broker.id must be an explicit non-negative 32-bit integer")
    else:
        details["broker_id"] = str(int(broker_id))

    if properties.get("process.roles", "").strip():
        issues.append(f"{path}: process.roles must be empty for the ZooKeeper bridge")

    ibp = properties.get("inter.broker.protocol.version")
    if ibp is None:
        issues.append(f"{path}: inter.broker.protocol.version is missing")
    else:
        try:
            parsed_ibp = protocol_version(ibp)
            details["inter_broker_protocol_version"] = ibp
            if parsed_ibp != protocol_version(expected_ibp):
                issues.append(f"{path}: {phase} phase requires IBP {expected_ibp}, found {ibp}")
        except ValueError as error:
            issues.append(f"{path}: {error}")

    try:
        actual_mode = boolean_value(properties, BRIDGE_GATES[0])
        details["bridge_mode"] = actual_mode
        if actual_mode is None:
            issues.append(f"{path}: {BRIDGE_GATES[0]} is missing")
        elif actual_mode != expected_mode:
            issues.append(f"{path}: {BRIDGE_GATES[0]} must be {str(expected_mode).lower()} for {phase} phase")
    except ValueError as error:
        issues.append(f"{path}: {error}")

    for gate in required_gates(phase, generation, require_all_gates):
        try:
            if boolean_value(properties, gate) is not True:
                issues.append(f"{path}: required {phase}-phase gate {gate} is not true")
        except ValueError as error:
            issues.append(f"{path}: {error}")

    if generation == "3.9":
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

    delay = properties.get("li.leader.election.on.corruption.wait.ms", "0")
    if not re.fullmatch(r"[0-9]+", delay) or int(delay) != 0:
        issues.append(f"{path}: li.leader.election.on.corruption.wait.ms must be 0; delayed elections are not ported")
    details["configured_bridge_gates"] = {}
    for gate in BRIDGE_GATES:
        if gate in properties:
            try:
                details["configured_bridge_gates"][gate] = boolean_value(properties, gate)
            except ValueError as error:
                issues.append(f"{path}: {error}")
    details["properties"] = {key: value for key, value in properties.items()
                             if key in BRIDGE_GATES or key in (
                                 "broker.id", "inter.broker.protocol.version", "process.roles",
                                 "li.async.fetcher.enable", "li.combined.control.request.enable",
                                 "remote.log.storage.system.enable", "li.drop.corrupted.files.enable",
                                 "li.leader.election.on.corruption.wait.ms", "li.zookeeper.pagination.enable")}
    return issues, details


def zk_command(kafka_home: Path, zk_connect: str, command: str) -> str:
    shell = kafka_home / "bin" / "zookeeper-shell.sh"
    if not shell.is_file():
        raise ValueError(f"ZooKeeper shell not found at {shell}")
    completed = subprocess.run(
        [str(shell), zk_connect], input=command + "\n", text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30, check=False)
    # ZooKeeper 3.8's shell exits 1 for NONODE. Optional LI roots can legitimately be
    # absent before the first compatible controller/tool creates them. Do not treat
    # authentication/connection errors as equivalent to missing state.
    if completed.returncode != 0 and "Node does not exist:" not in completed.stdout:
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
            values = [line.strip().lower() for line in output.splitlines() if line.strip().lower() in ("true", "false")]
            state["topic_deletion_flag_value"] = values[-1] if values else None
            if state[path] == "present" and not values:
                # LI 3.0 creates this root with null data. Both brokers treat null/empty
                # data as no override, not as disabled deletion. Preserve that distinction.
                null_data = any(line.strip() == "null" for line in output.splitlines())
                if not null_data:
                    stat = zk_command(kafka_home, zk_connect, f"stat {path}")
                    empty_data = re.search(r"(?m)^dataLength = 0\s*$", stat) is not None
                    if not empty_data:
                        issues.append("ZooKeeper topic deletion flag must be true, false, or unset")
        else:
            state[path] = parse_children(output)

    live_ids = state["/brokers/ids"] or []
    expected_ids = sorted(identifier for identifier in configured_broker_ids if identifier is not None)
    if not expected_ids:
        issues.append("No configured broker IDs were supplied for the live inventory check")
    if sorted(live_ids) != expected_ids:
        issues.append(f"Live ZooKeeper broker IDs {sorted(live_ids)} do not match rendered configs {expected_ids}")
    corrupted = state["/brokers/corrupted"] or []
    if corrupted:
        issues.append(f"ZooKeeper contains corrupted-broker state: {corrupted}")
    return issues, state


def inspect_live_inventory(inventory, dispositions, cluster_id, configs, phase, all_gates,
                           max_age_seconds, now=None):
    """Validate collected observations separately from owner-reviewed state dispositions.

    No broker version is inferred from a properties filename. Artifact admission remains a
    deployment responsibility; this gate compares the observed effective config with that input.
    """
    issues = []
    now = now or datetime.datetime.now(datetime.timezone.utc)
    for label, evidence in (("inventory", inventory), ("dispositions", dispositions)):
        if not isinstance(evidence, dict) or evidence.get("contract_version") != CONTRACT_VERSION:
            raise ValueError(f"{label} must use contract version {CONTRACT_VERSION}")
        if evidence.get("cluster_id") != cluster_id:
            issues.append(f"{label} cluster ID does not match {cluster_id}")
        timestamp = evidence.get("collected_at_utc")
        if not isinstance(timestamp, str):
            raise ValueError(f"{label} lacks a collection timestamp")
        collected = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if collected.tzinfo is None or not 0 <= (now - collected).total_seconds() <= max_age_seconds:
            issues.append(f"{label} is stale, future-dated, or lacks a timezone")
    observed = inventory.get("brokers")
    if not isinstance(observed, list) or any(not isinstance(broker, dict) for broker in observed):
        raise ValueError("inventory lacks broker observations")
    if not isinstance(dispositions.get("broker_runtimes", {}), dict) or not isinstance(dispositions.get("decisions"), dict):
        raise ValueError("dispositions requires broker_runtimes and decisions maps")
    expected = {config["broker_id"]: config for config in configs}
    ids = [str(broker["broker_id"]) for broker in observed]
    if len(ids) != len(set(ids)) or set(ids) != set(expected):
        issues.append("Live AdminClient inventory does not match the rendered broker inventory")
    for broker in observed:
        identifier = str(broker["broker_id"])
        if identifier not in expected:
            continue
        config = expected[identifier]
        properties = broker.get("properties")
        if not isinstance(properties, dict) or any(not isinstance(value, str) for value in properties.values()):
            raise ValueError(f"Broker {identifier} lacks string-valued effective properties")
        if broker.get("source") != "AdminClient.describeConfigs(includeSynonyms=true)":
            issues.append(f"Broker {identifier} lacks an effective AdminClient config observation")
        observed_issues, _ = inspect_config(Path(f"live-broker-{identifier}"), properties, phase,
                                             all_gates, config["generation"])
        issues.extend(observed_issues)
        for key, value in config["properties"].items():
            if key in properties and properties[key] != value:
                issues.append(f"Broker {identifier}: rendered/live mismatch for {key}")
        if boolean_value(properties, "li.zookeeper.pagination.enable"):
            runtime = dispositions.get("broker_runtimes", {}).get(identifier, {})
            if not isinstance(runtime, dict):
                raise ValueError(f"Invalid runtime observation for broker {identifier}")
            if runtime.get("pagination_supported") is not True:
                issues.append(f"Broker {identifier}: packaged ZooKeeper pagination capability is unqualified")
            for key in ("zookeeper_sha256", "jute_sha256"):
                if not re.fullmatch(r"[0-9a-f]{64}", runtime.get(key, "")):
                    issues.append(f"Broker {identifier}: missing packaged {key}")
            if not runtime.get("server_version") or not runtime.get("qualification_evidence"):
                issues.append(f"Broker {identifier}: missing deployed ZooKeeper server qualification")
    topics = inventory.get("topics")
    topic_names = inventory.get("topic_names")
    if not isinstance(topics, dict) or not isinstance(topic_names, list) or set(topics) != set(topic_names):
        issues.append("Live topic configuration inventory is incomplete")
    else:
        for topic, properties in topics.items():
            if not isinstance(properties, dict) or any(not isinstance(value, str) for value in properties.values()):
                raise ValueError(f"Invalid topic configuration for {topic}")
            if boolean_value(properties, "remote.storage.enable") or boolean_value(properties, "remote.log.storage.enable"):
                issues.append(f"Topic {topic} enables remote storage; a separate migration plan is required")
    # These are attestations backed by retained evidence, not claims inferred from an empty znode.
    decisions = dispositions.get("decisions", {})
    unknown = set(decisions) - set(ALLOWED_DISPOSITIONS)
    if unknown:
        issues.append(f"Unknown state dispositions: {', '.join(sorted(unknown))}")
    for name, allowed in sorted(ALLOWED_DISPOSITIONS.items()):
        decision = decisions.get(name, {})
        if not isinstance(decision, dict):
            raise ValueError(f"Invalid disposition for {name}")
        if any(not isinstance(decision.get(field), str) or not decision[field].strip()
               for field in ("owner", "evidence", "disposition")):
            issues.append(f"Missing or invalid owner/evidence/disposition for {name}; nonblank strings are required")
            continue
        if decision["disposition"] not in allowed:
            issues.append(f"{name}: unqualified disposition {decision['disposition']!r}; "
                          f"required: {' or '.join(allowed)}")
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-config", action="append", default=[], type=Path,
                        help="Rendered effective 3.9 broker properties; repeat for every 3.9 broker")
    parser.add_argument("--legacy-broker-config", action="append", default=[], type=Path,
                        help="Rendered effective 3.0 bridge properties; repeat for every old broker")
    parser.add_argument("--phase", choices=tuple(PHASES), required=True)
    parser.add_argument("--previous-phase", choices=tuple(PHASES))
    parser.add_argument("--require-all-gates", action="store_true",
                        help="Require every retained wrapper feature in every 3.9 phase (mode is phase-specific)")
    parser.add_argument("--require-live", action="store_true", help="Fail closed without fresh live admission evidence")
    parser.add_argument("--live-inventory", type=Path, help="JSON produced by LiBridgeLiveInventory")
    parser.add_argument("--state-dispositions", type=Path, help="Owner-approved remote/plugin/path disposition JSON")
    parser.add_argument("--cluster-id", help="Expected immutable Kafka cluster ID for live admission")
    parser.add_argument("--max-age-seconds", type=int, default=900)
    parser.add_argument("--zk-connect", help="Live ZooKeeper connection string")
    parser.add_argument("--kafka-home", type=Path,
                        help="Kafka distribution containing bin/zookeeper-shell.sh")
    parser.add_argument("--output-json", type=Path, help="Write machine-readable evidence to this path")
    args = parser.parse_args()

    if bool(args.zk_connect) != bool(args.kafka_home):
        parser.error("--zk-connect and --kafka-home must be supplied together")
    if not args.broker_config and not args.legacy_broker_config:
        parser.error("at least one --broker-config or --legacy-broker-config is required")
    if args.max_age_seconds <= 0:
        parser.error("--max-age-seconds must be positive")
    if args.require_live:
        args.require_all_gates = True  # Production admission is never the reduced smoke profile.

    issues: List[str] = []
    if args.previous_phase and not transition_allowed(args.previous_phase, args.phase):
        issues.append(f"Forbidden transition: {args.previous_phase} -> {args.phase}")
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
            if details["broker_id"] is not None:
                broker_id = str(details["broker_id"])
                if broker_id in configured_ids:
                    issues.append(f"Duplicate broker.id in rendered configs: {broker_id}")
                configured_ids.append(broker_id)
        except (OSError, ValueError) as error:
            issues.append(str(error))

    issues.extend(inventory_issues(args.phase, [details["generation"] for details in config_details]))
    live_evidence = None
    if args.require_live or args.live_inventory:
        if not (args.live_inventory and args.state_dispositions and args.cluster_id and args.zk_connect):
            issues.append("Live admission requires --live-inventory, --state-dispositions, --cluster-id and live ZooKeeper")
        else:
            try:
                live_evidence = json.loads(args.live_inventory.read_text(encoding="utf-8"))
                dispositions = json.loads(args.state_dispositions.read_text(encoding="utf-8"))
                issues.extend(inspect_live_inventory(live_evidence, dispositions, args.cluster_id,
                                                     config_details, args.phase, args.require_all_gates,
                                                     args.max_age_seconds))
            except (OSError, ValueError, TypeError, KeyError) as error:
                issues.append(f"Invalid live evidence: {error}")

    zk_state: Optional[Dict[str, object]] = None
    if args.zk_connect and args.kafka_home:
        try:
            zk_issues, zk_state = inspect_zookeeper(args.kafka_home, args.zk_connect, configured_ids)
            issues.extend(zk_issues)
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            issues.append(str(error))

    evidence = {
        "contract_version": CONTRACT_VERSION,
        "kind": "live-admission" if args.require_live else "configuration-check",
        "collected_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "cluster_id": args.cluster_id,
        "require_all_gates": args.require_all_gates,
        "live_inventory": live_evidence,
        "input_sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                         for path in [item[0] for item in config_inputs] + [args.live_inventory, args.state_dispositions]
                         if path and path.is_file()},
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
