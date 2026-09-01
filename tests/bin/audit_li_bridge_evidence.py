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
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


REQUIRED_COMMANDS: Tuple[str, ...] = (
    "preflight-unit",
    "source-whitespace",
    "wrapper-whitespace",
    "scala-212-compile",
    "scala-213-compile",
    "clients-bridge-tests",
    "core-bridge-tests",
    "storage-bridge-tests",
    "stage-wrapper-artifacts",
    "wrapper-tests",
    "release-39",
    "stop-kafka-gradle",
    "stop-wrapper-gradle",
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
)

REQUIRED_RESOURCE_PHASES: Tuple[str, ...] = (
    "mixed-metadata-loaded",
    "mixed-old-clients-complete",
    "mixed-final",
    "all-39-bridge",
    "all-39-native",
    "ibp-39-final",
)


def load_json(path: Path, issues: List[str]) -> Dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        issues.append(f"Missing evidence file: {path}")
    except json.JSONDecodeError as error:
        issues.append(f"Invalid JSON in {path}: {error}")
    return {}


def load_tsv(path: Path, issues: List[str]) -> List[Dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as source:
            return list(csv.DictReader(source, delimiter="\t"))
    except FileNotFoundError:
        issues.append(f"Missing evidence file: {path}")
        return []


def audit_commands(root: Path, require_full: bool, require_stage: bool, issues: List[str]) -> None:
    rows = load_tsv(root / "commands.tsv", issues)
    results = {row.get("command"): row.get("result") for row in rows}
    base_commands = REQUIRED_COMMANDS if require_stage else tuple(
        command for command in REQUIRED_COMMANDS if command != "stage-wrapper-artifacts")
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
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != archive["sha256"]:
            issues.append(f"Archive checksum mismatch for {path}: {digest} != {archive['sha256']}")


def audit_mixed_process(root: Path, require_archives: bool,
                        issues: List[str], warnings: List[str]) -> None:
    mixed = root / "mixed-process"
    summary = load_json(mixed / "run-summary.json", issues)
    if summary and summary.get("passed") is not True:
        issues.append(f"Mixed-process run did not pass: exit_status={summary.get('exit_status')}")
    if summary:
        audit_archive_hashes(summary, require_archives, issues, warnings)

    audit_preflight(mixed / "bridge-preflight-evidence.json", "mixed", issues)
    audit_preflight(mixed / "native-preflight-evidence.json", "native", issues)

    timings = load_tsv(mixed / "timings.tsv", issues)
    timing_results = {row.get("operation"): row.get("result") for row in timings}
    for operation in REQUIRED_TIMING_OPERATIONS:
        if timing_results.get(operation) != "passed":
            issues.append(f"Required process transition was not recorded as passed: {operation}")

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


def audit(root: Path, require_full: bool = False, require_clean: bool = False,
          require_archives: bool = False, require_stage: bool = True) -> Dict[str, object]:
    issues: List[str] = []
    warnings: List[str] = []
    summary = load_json(root / "verification-summary.json", issues)
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

    audit_commands(root, require_full, require_stage, issues)
    audit_mixed_process(root, require_archives, issues, warnings)
    return {"passed": not issues, "issues": issues, "warnings": warnings}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", required=True, type=Path)
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

    result = audit(args.evidence_dir, args.require_full, args.require_clean,
                   args.require_archives, not args.allow_missing_stage)
    rendered = json.dumps(result, indent=2, sort_keys=True)
    print(rendered)
    if args.output_json:
        args.output_json.write_text(rendered + "\n", encoding="utf-8")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
