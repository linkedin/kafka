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

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "bin"))
from li_bridge_commands import Commands
from verify_li_bridge import Verification, selection, write_json


class LiBridgeCommandsTest(unittest.TestCase):
    def test_failed_last_attempt_is_not_reused_and_logs_are_retained(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = Commands(root, os.environ)
            commands.run("probe", [sys.executable, "-c", "print('first pass')"], root)
            with self.assertRaises(subprocess.CalledProcessError):
                commands.run("probe", [sys.executable, "-c", "raise SystemExit(7)"], root)
            resumed = Commands(root, os.environ, resume=True)
            resumed.run("probe", [sys.executable, "-c", "print('fresh pass')"], root)
            self.assertEqual("passed", resumed.results["probe"])
            self.assertIn("fresh pass", (root / "probe.log").read_text())
            self.assertIn("first pass", (root / "probe-attempt-1.log").read_text())
            self.assertTrue((root / "probe-attempt-2.log").is_file())
            resumed.run("probe", ["must-not-run"], root)

    def test_timeout_and_start_failure_are_not_successes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            environment = dict(os.environ, BRIDGE_VERIFY_COMMAND_TIMEOUT_SECONDS="1")
            commands = Commands(root, environment)
            started = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                commands.run("timeout", [sys.executable, "-c", "import time; time.sleep(60)"], root)
            self.assertLess(time.monotonic() - started, 10)
            self.assertNotEqual("passed", commands.results["timeout"])
            with self.assertRaises(FileNotFoundError):
                commands.run("missing", [str(root / "missing-command")], root)
            self.assertNotEqual("passed", commands.results["missing"])

    def test_result_is_published_only_after_audit(self):
        for passed in (False, True):
            with self.subTest(passed=passed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                verification = object.__new__(Verification)
                verification.evidence = root
                verification.dry_run = False
                verification.full = False
                verification.skip_stage = False
                verification.legacy = root / "legacy.tgz"
                verification.fingerprints = {"source": "unchanged"}
                verification.commands = Commands(root, os.environ)
                write_json(root / "verification-summary.json", {"passed": False})

                def check_candidate(*args, **kwargs):
                    self.assertFalse(json.loads((root / "verification-summary.json").read_text())["passed"])
                    self.assertTrue(kwargs["summary_override"]["passed"])
                    return {"passed": passed, "issues": [] if passed else ["audit failed"], "warnings": []}

                with mock.patch.object(verification, "current_fingerprints", return_value=verification.fingerprints), \
                        mock.patch.object(verification, "summary", return_value={"passed": True}), \
                        mock.patch("verify_li_bridge.audit", side_effect=check_candidate):
                    if passed:
                        verification.finish()
                    else:
                        with self.assertRaisesRegex(ValueError, "audit failed"):
                            verification.finish()
                self.assertEqual(passed, json.loads((root / "verification-summary.json").read_text())["passed"])
                self.assertEqual("passed" if passed else "failed(1)", verification.commands.results["evidence-audit"])

    def test_selection_names_tests_and_requires_all_suites(self):
        path = Path(__file__).parents[1] / "bin/li_bridge_test_selection.ini"
        selected = selection(path)
        self.assertIn("kafka.controller.TopicDeletionManagerTest", selected["core"])
        self.assertIn("org.apache.kafka.common.message.Bridge*", selected["clients"])
        with tempfile.TemporaryDirectory() as directory:
            invalid = Path(directory) / "selection.ini"
            invalid.write_text("[core]\nkafka.Foo\n")
            with self.assertRaises(ValueError):
                selection(invalid)


if __name__ == "__main__":
    unittest.main()
