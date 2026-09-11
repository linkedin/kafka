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
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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
        fingerprints["inputs"] = {"legacy_archive": archives["3.0"], "skip_local_stage": False,
                                  "scenario": AUDITOR.scenario_spec()}
        self.write_json(root / "source-fingerprints.json", fingerprints)
        self.write_json(mixed / "run-summary.json", {
            "passed": True, "exit_status": 0, "archives": archives, "scenario": AUDITOR.scenario_spec(),
            "source_unchanged": True, "source_sha256": {"script.py": "source-digest"},
        })
        artifacts = []
        libraries = {}
        for name in ("kafka-clients", "kafka_2.12"):
            filename = name + "-3.9.2.17.jar"
            digest = hashlib.sha256(name.encode()).hexdigest()
            libraries[filename] = digest
            artifacts.append({"group": "com.linkedin.kafka", "name": name, "version": "3.9.2.17",
                              "classifier": None, "sha256": digest, "path": str(root / filename)})
        self.write_json(root / "archive-39.json", dict(archives["3.9"], version="3.9.2.17",
                                                      scala_version="2.12", libraries=libraries))
        wrapper_artifacts = {"passed": True, "archive_sha256": archives["3.9"]["sha256"], "artifacts": artifacts}
        self.write_json(root / "wrapper-artifacts.json", wrapper_artifacts)
        self.write_json(root / "wrapper-artifacts-after.json", wrapper_artifacts)
        from li_bridge_contract import BRIDGE_GATES, MODE
        for phase, (ibp, mode, generations) in AUDITOR.PHASES.items():
            self.write_json(mixed / f"old-clients-001-{phase}.json", {
                "phase": f"001-{phase}", "passed": True, "pid": 100, "acknowledged_records": 10,
                "aborted_records_visible": False, "connect_pid": 101, "connect_records": 10,
                "metadata_churn_cycles": 10,
            })
            configs = []
            for identifier, generation in enumerate(generations):
                properties = {gate: "true" for gate in BRIDGE_GATES}
                properties.update({"broker.id": str(identifier), "inter.broker.protocol.version": ibp,
                                   MODE: str(mode).lower()})
                _, config = AUDITOR.inspect_config(Path("broker"), properties, phase, False, generation)
                configs.append(config)
            self.write_json(mixed / f"preflight-{phase}.json", {
                "contract_version": 2, "phase": phase, "passed": True, "issues": [],
                "broker_configs": configs, "zookeeper_state": {"/brokers/ids": [c["broker_id"] for c in configs]},
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

    def test_empty_or_wrong_json_shape_is_rejected(self):
        files = ("verification-summary.json", "source-fingerprints.json", "mixed-process/run-summary.json",
                 "mixed-process/preflight-mixed.json", "mixed-process/preflight-native.json")
        for filename in files:
            for value in ({}, [], ["invalid"], None, False, "passed"):
                with self.subTest(filename=filename, value=value), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    self.create_evidence(root)
                    self.write_json(root / filename, value)
                    result = AUDITOR.audit(root, require_full=True, require_clean=True, require_archives=True)
                    self.assertFalse(result["passed"], result)

    def test_preflight_requires_live_broker_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            path = root / "mixed-process/preflight-mixed.json"
            self.write_json(path, {"phase": "mixed", "passed": True, "issues": []})
            self.assertFalse(AUDITOR.audit(root)["passed"])

    def test_preflight_pass_marker_does_not_override_configuration_evidence(self):
        for key, value in (("bridge_mode", False), ("inter_broker_protocol_version", "3.9"),
                           ("generation", "unknown")):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.create_evidence(root)
                path = root / "mixed-process/preflight-mixed.json"
                data = json.loads(path.read_text())
                data["broker_configs"][0][key] = value
                self.write_json(path, data)
                self.assertFalse(AUDITOR.audit(root)["passed"])

    def test_preflight_summary_must_match_its_effective_properties(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            path = root / "mixed-process/preflight-ibp-39.json"
            data = json.loads(path.read_text())
            data["broker_configs"][0]["inter_broker_protocol_version"] = "3.9-IV1"
            self.write_json(path, data)
            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertIn("inconsistent inter_broker_protocol_version", "\n".join(result["issues"]))

    def test_archive_must_match_verification_input(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            path = root / "source-fingerprints.json"
            fingerprints = json.loads(path.read_text())
            fingerprints["inputs"]["legacy_archive"]["sha256"] = "different"
            self.write_json(path, fingerprints)
            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertIn("does not match the verification input", "\n".join(result["issues"]))

    def test_resume_fingerprint_includes_archive_bytes_and_staging_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("kafka", "wrapper"):
                subprocess.run(["git", "init", "-q", str(root / name)], check=True)
                (root / name / "source.txt").write_text("unchanged source")
            archive = root / "legacy.tgz"
            archive.write_bytes(b"first archive")
            supplied = root / "supplied.tgz"
            supplied.write_bytes(b"first supplied archive")
            first = AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", archive, False, supplied)
            archive.write_bytes(b"second archive")
            second = AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", archive, False, supplied)
            supplied.write_bytes(b"second supplied archive")
            third = AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", archive, False, supplied)
            fourth = AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", archive, True, supplied)
            self.assertEqual(first["kafka"], second["kafka"])
            self.assertEqual(first["wrapper"], second["wrapper"])
            self.assertNotEqual(first, second)
            self.assertNotEqual(second, third)
            self.assertNotEqual(third, fourth)

    def test_resume_fingerprint_includes_effective_scenario(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("kafka", "wrapper"):
                subprocess.run(["git", "init", "-q", str(root / name)], check=True)
            with mock.patch.dict("os.environ", {}, clear=True):
                first = AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", None, False)
            for key in ("SCALE_TOPIC_COUNT", "SCALE_PARTITION_COUNT", "RECOVERY_RECORD_COUNT", "RECOVERY_RECORD_SIZE"):
                with mock.patch.dict("os.environ", {key: "999"}, clear=True):
                    changed = AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", None, False)
                    self.assertNotEqual(first, changed)
            with mock.patch.dict("os.environ", {"SCALE_TOPIC_COUNT": "10"}, clear=True):
                self.assertEqual(first, AUDITOR.verification_fingerprints(root / "kafka", root / "wrapper", None, False))

    def test_process_scenario_must_match_requested_workload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            path = root / "mixed-process/run-summary.json"
            data = json.loads(path.read_text())
            data["scenario"]["SCALE_TOPIC_COUNT"] += 1
            self.write_json(path, data)
            self.assertFalse(AUDITOR.audit(root)["passed"])

    def test_old_scenario_revision_does_not_qualify_deletion_recovery(self):
        for revision in (None, 3, 4, 5):
            with self.subTest(revision=revision), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.create_evidence(root)
                path = root / "mixed-process/run-summary.json"
                data = json.loads(path.read_text())
                if revision is None:
                    data["scenario"].pop("scenario_revision")
                else:
                    data["scenario"]["scenario_revision"] = revision
                self.write_json(path, data)
                self.assertFalse(AUDITOR.audit(root)["passed"])

    def supplied_evidence(self, root):
        archives = self.create_evidence(root)
        path = root / "source-fingerprints.json"
        fingerprints = json.loads(path.read_text())
        fingerprints["inputs"].update(broker_archive=archives["3.9"], skip_local_stage=True)
        self.write_json(path, fingerprints)
        commands = root / "commands.tsv"
        commands.write_text("".join(line for line in commands.read_text().splitlines(True)
                                   if line.split("\t")[0] not in ("release-39", "stage-wrapper-artifacts"))
                            + "provided-39\t1\tpassed\n")

    def test_supplied_archive_replaces_the_local_release_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.supplied_evidence(root)
            result = AUDITOR.audit(root, require_stage=False, require_archives=True)
            self.assertTrue(result["passed"], result)
            path = root / "source-fingerprints.json"
            fingerprints = json.loads(path.read_text())
            fingerprints["inputs"]["broker_archive"]["sha256"] = "different"
            self.write_json(path, fingerprints)
            result = AUDITOR.audit(root, require_stage=False)
            self.assertFalse(result["passed"])
            self.assertIn("3.9 archive does not match", "\n".join(result["issues"]))

    def test_supplied_mode_rejects_local_staging_and_missing_archive_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.supplied_evidence(root)
            commands = root / "commands.tsv"
            commands.write_text(commands.read_text().replace("provided-39\t1\tpassed\n", ""))
            path = root / "source-fingerprints.json"
            fingerprints = json.loads(path.read_text())
            fingerprints["inputs"]["skip_local_stage"] = False
            self.write_json(path, fingerprints)
            result = AUDITOR.audit(root, require_stage=False)
            self.assertFalse(result["passed"])
            self.assertIn("must not use local wrapper staging", "\n".join(result["issues"]))
            self.assertIn("provided-39", "\n".join(result["issues"]))

    def test_wrapper_report_must_match_archive_and_remain_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            path = root / "wrapper-artifacts.json"
            report = json.loads(path.read_text())
            report["artifacts"][0]["sha256"] = "another jar"
            self.write_json(path, report)
            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertIn("changed during testing", "\n".join(result["issues"]))
            self.assertIn("does not match the selected archive", "\n".join(result["issues"]))

    def test_complete_evidence_passes_strict_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            result = AUDITOR.audit(root, require_full=True, require_clean=True, require_archives=True)
            self.assertTrue(result["passed"], result)
            self.assertEqual([], result["issues"])

    def test_phase_labels_cannot_replace_unchanged_client_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.create_evidence(root)
            path = root / "mixed-process/old-clients-001-native.json"
            data = json.loads(path.read_text())
            data["pid"] = 200
            self.write_json(path, data)
            result = AUDITOR.audit(root)
            self.assertFalse(result["passed"])
            self.assertIn("same unchanged old-client process", "\n".join(result["issues"]))
            path.unlink()
            self.assertFalse(AUDITOR.audit(root)["passed"])

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

    def test_native_offline_deletion_requires_tombstone_acknowledgement_and_records(self):
        operations = ("native offline deletion metadata tombstone",
                      "native offline deletion retains assignment until acknowledgement",
                      "native offline deletion removes assignment after acknowledgement",
                      "native offline deletion recreated records verified")
        for operation in operations:
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.create_evidence(root)
                timings = root / "mixed-process/timings.tsv"
                timings.write_text("".join(line for line in timings.read_text().splitlines(True)
                                           if not line.startswith(operation + "\t")))
                result = AUDITOR.audit(root)
                self.assertFalse(result["passed"])
                self.assertTrue(any(operation in issue for issue in result["issues"]))

    def test_interrupted_deletion_requires_both_generations_and_record_checks(self):
        for generation in ("3.0", "3.9"):
            for check in ("assignment removed", "records verified"):
                with self.subTest(generation=generation, check=check), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    self.create_evidence(root)
                    timings = root / "mixed-process/timings.tsv"
                    operation = f"interrupted deletion {generation} {check}"
                    timings.write_text("".join(line for line in timings.read_text().splitlines(True)
                                               if not line.startswith(operation + "\t")))
                    result = AUDITOR.audit(root)
                    self.assertFalse(result["passed"])
                    self.assertTrue(any(operation in issue for issue in result["issues"]))

    def test_offline_name_reuse_requires_all_placement_checks(self):
        for generation in ("3.0", "3.9"):
            for placement in ("assigned-at-create", "reassigned-after-return"):
                with self.subTest(generation=generation, placement=placement), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    self.create_evidence(root)
                    timings = root / "mixed-process/timings.tsv"
                    operation = f"offline name-reuse {generation} {placement} records verified after promotion"
                    timings.write_text("".join(line for line in timings.read_text().splitlines(True)
                                               if not line.startswith(operation + "\t")))
                    result = AUDITOR.audit(root)
                    self.assertFalse(result["passed"])
                    self.assertTrue(any(operation in issue for issue in result["issues"]))

    def test_invalid_utf8_is_an_issue_not_an_auditor_crash(self):
        for filename in ("verification-summary.json", "commands.tsv", "mixed-process/protocol-selection.log"):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.create_evidence(root)
                (root / filename).write_bytes(b"\xff\xfe\xff")
                result = AUDITOR.audit(root)
                self.assertFalse(result["passed"])
                self.assertTrue(any("Cannot read" in issue for issue in result["issues"]))

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
