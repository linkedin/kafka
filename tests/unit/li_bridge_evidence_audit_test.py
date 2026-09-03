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

import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "bin" / "audit_li_bridge_evidence.py"
SPEC = importlib.util.spec_from_file_location("audit_li_bridge_evidence", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load evidence auditor module from {SCRIPT}")
AUDITOR = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDITOR)


class LiBridgeEvidenceAuditTest(unittest.TestCase):
    def create_evidence(self, root: Path):
        mixed = root / "mixed-process"
        mixed.mkdir(parents=True)
        fingerprints = {
            "kafka": {"sha256": "kafka-source", "file_count": 1},
            "wrapper": {"sha256": "wrapper-source", "file_count": 1},
        }
        self.write_json(root / "source-fingerprints.json", fingerprints)
        self.write_json(root / "verification-summary.json", {
            "passed": True,
            "kafka": {"commit": "abc", "dirty": False, "source_sha256": "kafka-source"},
            "wrapper": {"commit": "def", "dirty": False, "source_sha256": "wrapper-source"},
        })

        commands = AUDITOR.REQUIRED_COMMANDS + AUDITOR.FULL_COMMANDS
        (root / "commands.tsv").write_text(
            "command\tduration_seconds\tresult\n" +
            "".join(f"{command}\t1\tpassed\n" for command in commands), encoding="utf-8")

        archives = {}
        for generation in ("3.0", "3.9"):
            path = root / f"kafka-{generation}.tgz"
            path.write_bytes(generation.encode("utf-8"))
            archives[generation] = {
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        self.write_json(mixed / "run-summary.json", {
            "passed": True, "exit_status": 0, "archives": archives,
        })
        self.write_json(mixed / "bridge-preflight-evidence.json", {
            "phase": "mixed", "passed": True, "issues": [],
        })
        self.write_json(mixed / "native-preflight-evidence.json", {
            "phase": "native", "passed": True, "issues": [],
        })
        (mixed / "timings.tsv").write_text(
            "operation\tduration_seconds\tresult\n" +
            "".join(f"{operation}\t1\tpassed\n" for operation in AUDITOR.REQUIRED_TIMING_OPERATIONS),
            encoding="utf-8")
        (mixed / "broker-resources.tsv").write_text(
            "timestamp_utc\tphase\tprocess\tpid\trss_kib\n" +
            "".join(f"now\t{phase}\tbroker-0\t1\t100\n" for phase in AUDITOR.REQUIRED_RESOURCE_PHASES),
            encoding="utf-8")
        enabled = "LI protocol bridge mode enabled: LeaderAndIsr=v2, UpdateMetadata=v5, StopReplica=v1"
        (mixed / "protocol-selection.log").write_text(
            enabled + "\n" + enabled + "\nLI protocol bridge mode disabled\n", encoding="utf-8")
        return archives

    @staticmethod
    def write_json(path: Path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def test_complete_evidence_passes_strict_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            result = AUDITOR.audit(root, require_full=True, require_clean=True, require_archives=True)
            self.assertTrue(result["passed"], result)
            self.assertEqual([], result["issues"])

    def test_failed_or_missing_command_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            command_file = root / "commands.tsv"
            command_file.write_text(
                command_file.read_text(encoding="utf-8").replace(
                    "core-bridge-tests\t1\tpassed", "core-bridge-tests\t1\tfailed(1)"),
                encoding="utf-8")
            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertTrue(any("core-bridge-tests" in issue for issue in result["issues"]))

    def test_missing_local_stage_can_be_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            command_file = root / "commands.tsv"
            command_file.write_text(
                "".join(line for line in command_file.read_text(encoding="utf-8").splitlines(True)
                        if not line.startswith("stage-wrapper-artifacts\t")),
                encoding="utf-8")
            result = AUDITOR.audit(root, require_stage=False)
            self.assertTrue(result["passed"], result)

    def test_source_fingerprint_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            summary = json.loads((root / "verification-summary.json").read_text(encoding="utf-8"))
            summary["kafka"]["source_sha256"] = "different"
            self.write_json(root / "verification-summary.json", summary)
            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertTrue(any("source fingerprint" in issue for issue in result["issues"]))

    def test_archive_hash_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archives = self.create_evidence(root)
            Path(archives["3.9"]["path"]).write_bytes(b"modified")
            result = AUDITOR.audit(root, require_archives=True)
            self.assertFalse(result["passed"])
            self.assertTrue(any("checksum mismatch" in issue for issue in result["issues"]))

    def test_missing_process_transition_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            timings = root / "mixed-process" / "timings.tsv"
            missing_operation = AUDITOR.REQUIRED_TIMING_OPERATIONS[0]
            timings.write_text(
                "".join(line for line in timings.read_text(encoding="utf-8").splitlines(True)
                        if not line.startswith(f"{missing_operation}\t")),
                encoding="utf-8")

            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertTrue(any(missing_operation in issue for issue in result["issues"]))

    def test_malformed_verification_summary_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            summary = root / "verification-summary.json"
            summary.write_text("{not valid json", encoding="utf-8")

            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertTrue(any("Invalid JSON" in issue and str(summary) in issue
                                for issue in result["issues"]))


if __name__ == "__main__":
    unittest.main()
