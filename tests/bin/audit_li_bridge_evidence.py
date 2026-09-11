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

"""Audit a li-bridge verification evidence directory for coverage and internal consistency."""

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


sys.path.insert(0, str(Path(__file__).resolve().parent))
from li_bridge_contract import CONTRACT_VERSION, PHASES, inventory_issues, scenario_spec
from li_bridge_preflight import inspect_config


REQUIRED_COMMANDS: Tuple[str, ...] = (
    "archive-input-30",
    "preflight-unit",
    "source-whitespace",
    "wrapper-whitespace",
    "scala-212-compile",
    "scala-213-compile",
    "clients-bridge-tests",
    "core-bridge-tests",
    "vendor-pagination",
    "storage-bridge-tests",
    "stage-wrapper-artifacts",
    "wrapper-artifacts",
    "wrapper-tests",
    "wrapper-artifacts-unchanged",
    "release-39",
    "mixed-process",
)

FULL_COMMANDS: Tuple[str, ...] = ("clients-full", "server-full", "storage-full")

REQUIRED_TIMING_OPERATIONS: Tuple[str, ...] = (
    "3.0 controller election",
    "3.9 controller election",
    "all final mixed-mode partitions to become healthy",
    "all-3.9 bridge-mode partitions to become healthy",
    "native-mode dynamic configuration",
    "all final native-mode partitions to become healthy",
    "all final IBP 3.9 partitions to become healthy",
    "canary rollback to 3.0",
    "all-3.9 rollback to 3.0",
    "hard controller recovery",
    "native offline deletion metadata tombstone",
    "native offline deletion retains assignment until acknowledgement",
    "native offline deletion removes assignment after acknowledgement",
    "native offline deletion recreated records verified",
) + tuple(f"old clients phase {phase}" for phase in PHASES) + tuple(
    f"offline name-reuse {generation} {placement} records verified after promotion"
    for generation in ("3.0", "3.9") for placement in ("assigned-at-create", "reassigned-after-return")) + tuple(
    f"interrupted deletion {generation} {check}"
    for generation in ("3.0", "3.9") for check in ("assignment removed", "records verified"))

REQUIRED_RESOURCE_PHASES: Tuple[str, ...] = (
    "mixed-metadata-loaded",
    "mixed-old-clients-complete",
    "mixed-final",
    "all-39-bridge",
    "all-39-native",
    "ibp-39-final",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_fingerprint(root: Path) -> Dict[str, object]:
    names = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    paths = sorted(Path(name.decode("utf-8")) for name in names.split(b"\0") if name)
    digest = hashlib.sha256()
    for relative in paths:
        digest.update(str(relative).encode("utf-8"))
        digest.update(b"\0")
        digest.update((root / relative).read_bytes())
        digest.update(b"\0")
    return {"sha256": digest.hexdigest(), "file_count": len(paths)}


def verification_fingerprints(kafka_root: Path, wrapper_root: Path,
                              legacy_archive: Optional[Path], skip_local_stage: bool,
                              supplied_archive: Optional[Path] = None,
                              build_version: Optional[str] = None) -> Dict[str, object]:
    def archive(path):
        return None if path is None else {"path": str(path.resolve()), "sha256": file_sha256(path)}
    return {
        "kafka": source_fingerprint(kafka_root),
        "wrapper": source_fingerprint(wrapper_root),
        "inputs": {"legacy_archive": archive(legacy_archive), "broker_archive": archive(supplied_archive),
                   "skip_local_stage": skip_local_stage, "build_version": build_version,
                   "scenario": scenario_spec(),
                   "verification_timeout_seconds": int(os.environ.get("BRIDGE_VERIFY_COMMAND_TIMEOUT_SECONDS", "3600"))},
    }


def load_json(path: Path, issues: List[str]) -> Dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not data:
            issues.append(f"Expected a non-empty JSON object in {path}")
            return {}
        return data
    except FileNotFoundError:
        issues.append(f"Missing evidence file: {path}")
    except json.JSONDecodeError as error:
        issues.append(f"Invalid JSON in {path}: {error}")
    except (OSError, UnicodeError) as error:
        issues.append(f"Cannot read JSON in {path}: {error}")
    return {}


def load_tsv(path: Path, issues: List[str]) -> List[Dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as source:
            return list(csv.DictReader(source, delimiter="\t"))
    except FileNotFoundError:
        issues.append(f"Missing evidence file: {path}")
    except (OSError, UnicodeError, csv.Error) as error:
        issues.append(f"Cannot read TSV in {path}: {error}")
    return []


def audit_commands(root: Path, require_full: bool, require_stage: bool, issues: List[str],
                   supplied_archive: bool = False) -> None:
    rows = load_tsv(root / "commands.tsv", issues)
    results = {row.get("command"): row.get("result") for row in rows}
    base_commands = REQUIRED_COMMANDS if require_stage else tuple(
        command for command in REQUIRED_COMMANDS if command != "stage-wrapper-artifacts")
    if supplied_archive:
        base_commands = tuple(command for command in base_commands
                              if command != "release-39") + ("provided-39",)
    required: Sequence[str] = base_commands + (FULL_COMMANDS if require_full else ())
    for command in required:
        if command not in results:
            issues.append(f"Required verification command was not recorded: {command}")
        elif results[command] != "passed":
            issues.append(f"Verification command did not pass: {command}={results[command]}")


def audit_preflight(path: Path, expected_phase: str, issues: List[str]) -> None:
    evidence = load_json(path, issues)
    if evidence and evidence.get("phase") != expected_phase:
        issues.append(f"{path} has phase {evidence.get('phase')!r}, expected {expected_phase!r}")
    if evidence and evidence.get("passed") is not True:
        issues.append(f"{path} did not pass: {evidence.get('issues')}")
    if not evidence:
        return
    if evidence.get("issues") != []:
        issues.append(f"{path} does not contain an empty issue list")
    configs = evidence.get("broker_configs")
    if not isinstance(configs, list) or not configs:
        issues.append(f"{path} lacks broker configuration evidence")
        return
    broker_ids = [config.get("broker_id") for config in configs if isinstance(config, dict)]
    if len(broker_ids) != len(configs) or any(not isinstance(value, str) or not value.isdecimal()
                                           for value in broker_ids):
        issues.append(f"{path} has incomplete broker IDs")
        return
    if len(set(broker_ids)) != len(broker_ids):
        issues.append(f"{path} has duplicate broker IDs")
    if evidence.get("contract_version") != CONTRACT_VERSION:
        issues.append(f"{path} does not use bridge contract {CONTRACT_VERSION}; regenerate qualification evidence")
    expected_ibp, expected_mode, generations = PHASES[expected_phase]
    issues.extend(inventory_issues(expected_phase, [config.get("generation", "unknown") for config in configs]))
    for config in configs:
        if config.get("bridge_mode") is not expected_mode:
            issues.append(f"{path} records an incorrect bridge mode for broker {config.get('broker_id')}")
        if config.get("inter_broker_protocol_version") not in {expected_ibp, expected_ibp + "-IV0", expected_ibp + "-IV1"}:
            issues.append(f"{path} records an incorrect IBP for broker {config.get('broker_id')}")
        generation = config.get("generation")
        if generation not in generations:
            issues.append(f"{path} records an incorrect binary generation for broker {config.get('broker_id')}")
        properties = config.get("properties")
        if not isinstance(properties, dict):
            issues.append(f"{path} lacks effective configuration values")
        else:
            config_issues, observed = inspect_config(path, properties, expected_phase,
                                                     evidence.get("require_all_gates") is True, generation)
            issues.extend(config_issues)
            for field in ("broker_id", "bridge_mode", "inter_broker_protocol_version"):
                if config.get(field) != observed.get(field):
                    issues.append(f"{path} has inconsistent {field} for broker {config.get('broker_id')}")
    state = evidence.get("zookeeper_state")
    if not isinstance(state, dict) or not isinstance(state.get("/brokers/ids"), list):
        issues.append(f"{path} lacks live ZooKeeper broker inventory")
    elif any(not isinstance(value, str) or not value.isdecimal() for value in state["/brokers/ids"]):
        issues.append(f"{path} has invalid live broker IDs")
    elif set(state["/brokers/ids"]) != set(broker_ids):
        issues.append(f"{path} broker configs do not match live ZooKeeper inventory")


def audit_archive_hashes(summary: Dict[str, object], require_archives: bool,
                         issues: List[str], warnings: List[str]) -> None:
    archives = summary.get("archives", {})
    if not isinstance(archives, dict):
        issues.append("Mixed-process summary has no archive map")
        return
    for generation in ("3.0", "3.9"):
        archive = archives.get(generation)
        if not isinstance(archive, dict) or not archive.get("path") or not archive.get("sha256"):
            issues.append(f"Mixed-process summary lacks {generation} archive path or hash")
            continue
        path = Path(str(archive["path"]))
        if not path.is_file():
            message = f"Cannot re-hash absent {generation} archive: {path}"
            if require_archives:
                issues.append(message)
            else:
                warnings.append(message)
            continue
        try:
            digest = file_sha256(path)
        except OSError as error:
            issues.append(f"Cannot hash archive {path}: {error}")
            continue
        if digest != archive["sha256"]:
            issues.append(f"Archive checksum mismatch for {path}: {digest} != {archive['sha256']}")


def audit_mixed_process(root: Path, require_archives: bool,
                        issues: List[str], warnings: List[str], direct: bool = False) -> Dict[str, object]:
    mixed = root if direct else root / "mixed-process"
    summary = load_json(mixed / "run-summary.json", issues)
    if summary and summary.get("passed") is not True:
        issues.append(f"Mixed-process run did not pass: exit_status={summary.get('exit_status')}")
    if summary:
        audit_archive_hashes(summary, require_archives, issues, warnings)
        if summary.get("source_unchanged") is not True or not summary.get("source_sha256"):
            issues.append("Process scenario lacks unchanged-source evidence")

    for phase in PHASES:
        audit_preflight(mixed / f"preflight-{phase}.json", phase, issues)

    timings = load_tsv(mixed / "timings.tsv", issues)
    timing_results = {row.get("operation"): row.get("result") for row in timings}
    for operation in REQUIRED_TIMING_OPERATIONS:
        if timing_results.get(operation) != "passed":
            issues.append(f"Required process transition was not recorded as passed: {operation}")

    old_client_phases = set()
    old_client_pids = set()
    connect_pids = set()
    for path in sorted(mixed.glob("old-clients-*.json")):
        client = load_json(path, issues)
        phase = str(client.get("phase", "")).split("-", 1)[-1]
        if client.get("passed") is not True or client.get("aborted_records_visible") is not False:
            issues.append(f"Old client checkpoint failed: {path}")
        elif phase not in PHASES or not isinstance(client.get("acknowledged_records"), int) or client["acknowledged_records"] <= 0:
            issues.append(f"Incomplete old client checkpoint: {path}")
        else:
            old_client_phases.add(phase)
        connect_pid = client.get("connect_pid")
        count = client.get("connect_records")
        if type(connect_pid) is not int or connect_pid <= 0 or type(count) is not int or count <= 0:
            issues.append(f"Missing persistent Connect evidence: {path}")
        else:
            connect_pids.add(connect_pid)
        if not isinstance(client.get("metadata_churn_cycles"), int) or client["metadata_churn_cycles"] <= 0:
            issues.append(f"Missing concurrent metadata mutation evidence: {path}")
        pid = client.get("pid")
        if not isinstance(pid, int) or pid <= 0:
            issues.append(f"Missing old client process identity: {path}")
        else:
            old_client_pids.add(pid)
    if old_client_phases != set(PHASES):
        issues.append(f"Old-client evidence does not cover all phases: {sorted(old_client_phases)}")
    if len(old_client_pids) != 1 or len(connect_pids) != 1:
        issues.append("The same unchanged old-client process and Connect worker must survive every phase")

    resources = load_tsv(mixed / "broker-resources.tsv", issues)
    phases = {row.get("phase") for row in resources}
    for phase in REQUIRED_RESOURCE_PHASES:
        if phase not in phases:
            issues.append(f"Required broker resource phase is missing: {phase}")

    protocol_path = mixed / "protocol-selection.log"
    try:
        protocol_log = protocol_path.read_text(encoding="utf-8")
        enabled_count = protocol_log.count(
            "LI protocol bridge mode enabled: LeaderAndIsr=v2, UpdateMetadata=v5, StopReplica=v1")
        if enabled_count < 2:
            issues.append(f"Expected bridge-selection evidence from both generations, found {enabled_count}")
        if "LI protocol bridge mode disabled" not in protocol_log:
            issues.append("Native protocol selection was not recorded")
    except FileNotFoundError:
        issues.append(f"Missing evidence file: {protocol_path}")
    except (OSError, UnicodeError) as error:
        issues.append(f"Cannot read protocol evidence {protocol_path}: {error}")
    return summary


def audit_wrapper_artifacts(root: Path, archives: Dict[str, object], issues: List[str]) -> None:
    before = load_json(root / "wrapper-artifacts.json", issues)
    after = load_json(root / "wrapper-artifacts-after.json", issues)
    broker = load_json(root / "archive-39.json", issues)
    if before.get("passed") is not True or before != after:
        issues.append("Wrapper artifact checks failed or changed during testing")
    used = archives.get("3.9", {}) if isinstance(archives, dict) else {}
    if not isinstance(used, dict) or not used.get("sha256"):
        issues.append("Wrapper artifact check has no mixed-process 3.9 archive")
    elif before.get("archive_sha256") != used["sha256"] or broker.get("sha256") != used["sha256"]:
        issues.append("Wrapper artifacts were checked against a different 3.9 archive")
    artifacts = before.get("artifacts")
    libraries = broker.get("libraries")
    if not isinstance(artifacts, list) or not artifacts or not isinstance(libraries, dict):
        issues.append("Wrapper artifact check lacks the resolved jars or archive library hashes")
        return
    main_modules = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict) or artifact.get("group") != "com.linkedin.kafka" or \
                artifact.get("version") != broker.get("version") or not artifact.get("sha256"):
            issues.append("Wrapper artifact check has an invalid Kafka coordinate or hash")
            continue
        if artifact.get("classifier") is None:
            name = artifact.get("name")
            if not isinstance(name, str) or artifact["sha256"] != libraries.get(f"{name}-{broker.get('version')}.jar"):
                issues.append(f"Wrapper main jar does not match the selected archive: {name}")
            else:
                main_modules.add(name)
    if not {"kafka-clients", "kafka_" + str(broker.get("scala_version"))}.issubset(main_modules):
        issues.append("Wrapper artifact check lacks matching Kafka core and clients jars")


def audit(root: Path, require_full: bool = False, require_clean: bool = False,
          require_archives: bool = False, require_stage: bool = True,
          summary_override: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    issues: List[str] = []
    warnings: List[str] = []
    # The verifier checks an in-memory candidate before publishing a successful summary.
    # Standalone audits always read the final result from disk.
    summary = summary_override if summary_override is not None else load_json(root / "verification-summary.json", issues)
    fingerprints = load_json(root / "source-fingerprints.json", issues)
    if summary and summary.get("passed") is not True:
        issues.append("Top-level verification summary did not pass")
    for checkout in ("kafka", "wrapper"):
        checkout_summary = summary.get(checkout, {}) if summary else {}
        fingerprint_summary = fingerprints.get(checkout, {}) if fingerprints else {}
        if not isinstance(checkout_summary, dict) or not checkout_summary.get("commit"):
            issues.append(f"Top-level summary lacks the {checkout} source revision")
            continue
        if require_clean and checkout_summary.get("dirty") is not False:
            issues.append(f"{checkout} checkout was dirty")
        if not isinstance(fingerprint_summary, dict) or not fingerprint_summary.get("sha256"):
            issues.append(f"Source fingerprint is missing for {checkout}")
        elif checkout_summary.get("source_sha256") != fingerprint_summary.get("sha256"):
            issues.append(f"Top-level summary does not match the {checkout} source fingerprint")

    inputs = fingerprints.get("inputs", {})
    supplied_input = inputs.get("broker_archive") if isinstance(inputs, dict) else None
    if supplied_input is not None and inputs.get("skip_local_stage") is not True:
        issues.append("A supplied 3.9 archive must not use local wrapper staging")
    audit_commands(root, require_full, require_stage, issues, supplied_input is not None)
    mixed = audit_mixed_process(root, require_archives, issues, warnings)
    legacy_input = inputs.get("legacy_archive") if isinstance(inputs, dict) else None
    archives = mixed.get("archives", {})
    legacy_used = archives.get("3.0") if isinstance(archives, dict) else None
    if not isinstance(legacy_input, dict) or not legacy_input.get("sha256"):
        issues.append("The verification input fingerprint lacks the 3.0 archive")
    elif not isinstance(legacy_used, dict) or legacy_input.get("sha256") != legacy_used.get("sha256"):
        issues.append("The mixed-process 3.0 archive does not match the verification input")
    if supplied_input is not None:
        broker_used = archives.get("3.9") if isinstance(archives, dict) else None
        if not isinstance(supplied_input, dict) or not supplied_input.get("sha256") or \
                not isinstance(broker_used, dict) or supplied_input["sha256"] != broker_used.get("sha256"):
            issues.append("The mixed-process 3.9 archive does not match the supplied verification input")
    expected_scenario = inputs.get("scenario") if isinstance(inputs, dict) else None
    if not isinstance(expected_scenario, dict) or expected_scenario.get("contract_version") != CONTRACT_VERSION:
        issues.append("Verification fingerprint lacks the current normalized scenario")
    elif mixed.get("scenario") != expected_scenario:
        issues.append("Mixed-process scenario does not match the requested verification inputs")
    audit_wrapper_artifacts(root, archives, issues)
    return {"passed": not issues, "issues": issues, "warnings": warnings}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--process-only", action="store_true",
                        help="Audit a standalone process evidence directory, not full wrapper/release qualification")
    parser.add_argument("--require-full", action="store_true",
                        help="Require complete clients, server, and storage suite records")
    parser.add_argument("--require-clean", action="store_true",
                        help="Reject evidence produced from dirty source checkouts")
    parser.add_argument("--require-archives", action="store_true",
                        help="Require archive files to remain available and match recorded hashes")
    parser.add_argument("--allow-missing-stage", action="store_true",
                        help="Do not require the local artifact-staging command")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()

    if args.process_only:
        if args.require_full or args.require_clean:
            parser.error("--process-only cannot establish full-suite or clean-checkout qualification")
        issues, warnings = [], []
        summary = audit_mixed_process(args.evidence_dir, args.require_archives, issues, warnings, direct=True)
        if summary.get("scenario") != scenario_spec():
            issues.append("Process evidence does not match the requested environment/scenario")
        result = {"kind": "process-qualification-only", "passed": not issues, "issues": issues, "warnings": warnings}
    else:
        result = audit(args.evidence_dir, args.require_full, args.require_clean,
                       args.require_archives, not args.allow_missing_stage)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.output_json:
        args.output_json.write_text(rendered + "\n", encoding="utf-8")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
