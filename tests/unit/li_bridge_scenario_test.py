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

import re
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "bin"))
from li_bridge_contract import BRIDGE_GATES, FEATURES, PHASES, scenario_spec
from li_bridge_mixed_cluster_smoke import Migration, verify_connect_records
from li_bridge_preflight import parse_properties, zk_command


class LiBridgeScenarioTest(unittest.TestCase):
    def test_scenario_defaults_validation_and_runtime_profile(self):
        default = scenario_spec({})
        self.assertEqual(3, default["scenario_revision"])
        self.assertEqual(default, scenario_spec({"SCALE_TOPIC_COUNT": "10"}))
        for key in ("SCALE_TOPIC_COUNT", "SCALE_PARTITION_COUNT", "RECOVERY_RECORD_COUNT", "RECOVERY_RECORD_SIZE"):
            for value in ("0", "-1", "bad", "2147483648"):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    scenario_spec({key: value})
            self.assertNotEqual(default, scenario_spec({key: "20"}))
        self.assertNotEqual(default, scenario_spec({"KAFKA_HEAP_OPTS": "-Xmx2g"}))
        self.assertNotEqual(default, scenario_spec({"LI_BRIDGE_JAVA_30_HOME": "/jdk11"}))
        self.assertEqual({"dormant", "legacy-bridge", "mixed", "all-39-bridge", "native", "ibp-39"}, set(PHASES))

    def test_zookeeper_startup_retries_a_probe_timeout_but_not_a_dead_server(self):
        runner = object.__new__(Migration)
        process = mock.Mock()
        process.poll.return_value = None
        runner.processes = {"zookeeper": process}
        with mock.patch.object(runner, "zk", side_effect=[subprocess.TimeoutExpired(["zk"], 5), "[zookeeper]\n"]) as probe:
            self.assertFalse(runner.zookeeper_ready())
            self.assertTrue(runner.zookeeper_ready())
            probe.assert_called_with("ls /", timeout=5)
        process.poll.return_value = 1
        process.returncode = 1
        with self.assertRaisesRegex(AssertionError, "exited during startup"):
            runner.zookeeper_ready()

    def test_protocol_selection_and_errors_include_rotated_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = object.__new__(Migration)
            runner.work = Path(directory)
            runner.evidence = runner.work / "evidence"
            runner.evidence.mkdir()
            enabled = "LI protocol bridge mode enabled: LeaderAndIsr=v2, UpdateMetadata=v5, StopReplica=v1"
            for generation in ("3.0", "3.9"):
                logs = runner.work / f"broker-0-{generation}-logs"
                logs.mkdir()
                (logs / "controller.log.2026-09-11-07").write_text(enabled)
                (logs / "controller.log").write_text("LI protocol bridge mode disabled")
            runner.verify_protocol_logs()
            self.assertEqual(2, (runner.evidence / "protocol-selection.log").read_text().count(enabled))
            rotated = logs / "server.log.2026-09-11-07"
            for error in ("UnsupportedVersionException", "Error parsing LeaderAndIsr", "unknown api key"):
                with self.subTest(error=error):
                    rotated.write_text(error)
                    with self.assertRaisesRegex(AssertionError, "Protocol error"):
                        runner.verify_protocol_logs()
            rotated.unlink()
            (logs / "controller.log.2026-09-11-07").unlink()
            with self.assertRaisesRegex(AssertionError, "Missing 3.9 controller bridge selection"):
                runner.verify_protocol_logs()

    def test_failure_archive_retains_rotated_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = object.__new__(Migration)
            runner.work = Path(directory)
            runner.evidence = runner.work / "evidence"
            runner.evidence.mkdir()
            runner.processes, runner.source_hashes, runner.archives = {}, {}, {}
            runner.handles, runner.timings, runner.resources = [], [], []
            runner.started = "2026-09-11T07:00:00+00:00"
            runner.scenario = scenario_spec({})
            rotated = runner.work / "broker-0-3.9-logs/controller.log.2026-09-11-07"
            rotated.parent.mkdir()
            rotated.write_text("retained failure detail")
            with mock.patch.object(runner, "diagnostics"), mock.patch.dict("os.environ", {"EVIDENCE_INCLUDE_LOGS": "1"}):
                self.assertFalse(runner.finish(False))
            with tarfile.open(runner.evidence / "process-logs.tgz") as archive:
                data = archive.extractfile(str(rotated.relative_to(runner.work))).read()
            self.assertEqual(b"retained failure detail", data)
            self.assertTrue(rotated.exists())

    def test_manifest_has_every_gate_and_effective_metric(self):
        root = Path(__file__).parents[2]
        config = (root / "core/src/main/scala/kafka/server/KafkaConfig.scala").read_text()
        metrics = (root / "core/src/main/scala/kafka/server/LiProtocolBridgeMetrics.scala").read_text()
        self.assertEqual(set(BRIDGE_GATES), set(re.findall(r'"(li\.protocol\.bridge\.[^"]+\.enable)"', config)))
        for _, metric, _ in FEATURES:
            self.assertIn(f'"{metric}"', metrics)

    def test_connect_validation_detects_loss_and_changed_values(self):
        self.assertTrue(verify_connect_records(["a", "b"], ["a", "b"]))
        self.assertFalse(verify_connect_records(["a", "b"], ["a", "a"]))
        self.assertTrue(verify_connect_records(["a", "b"], ["a", "b", "b"]))  # documented at-least-once
        with self.assertRaises(AssertionError):
            verify_connect_records(["a", "b"], ["a", "corrupted"])

    def test_zookeeper_nonode_exit_is_not_an_authentication_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "bin").mkdir()
            (home / "bin/zookeeper-shell.sh").touch()
            result = subprocess.CompletedProcess([], 1, "Node does not exist: /federatedTopics\n")
            with mock.patch("li_bridge_preflight.subprocess.run", return_value=result):
                self.assertIn("Node does not exist", zk_command(home, "localhost", "ls /federatedTopics"))
            result.stdout = "KeeperErrorCode = NoAuth for /federatedTopics\n"
            with mock.patch("li_bridge_preflight.subprocess.run", return_value=result), self.assertRaises(RuntimeError):
                zk_command(home, "localhost", "ls /federatedTopics")

    def test_command_deadline_terminates_child_process(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = object.__new__(Migration)
            runner.deadline = time.monotonic() + 10
            runner.scenario = scenario_spec({})
            runner.commands = Path(directory) / "commands.log"
            started = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                runner.command([sys.executable, "-c", "import time; time.sleep(60)"], timeout=0.05)
            self.assertLess(time.monotonic() - started, 5)
            self.assertTrue(runner.commands.exists())
            output = runner.command([sys.executable, "-c", "print('Node does not exist: /x'); exit(1)"],
                                    check=False, allow_missing=True)
            self.assertIn("Node does not exist", output)
            self.assertIsNone(runner.command([sys.executable, "-c", "print('NoAuth'); exit(1)"],
                                            check=False, allow_missing=True))

    def test_connect_rest_listener_is_local_and_does_not_reserve_a_fixed_port(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = object.__new__(Migration)
            runner.work = Path(directory)
            runner.client_dir = runner.work / "old-clients"
            runner.churn_dir = runner.work / "churn"
            runner.scenario = scenario_spec({})
            runner.bootstrap = "127.0.0.1:29092"
            runner.homes = {"3.0": runner.work / "kafka"}
            with mock.patch.object(runner, "create"), mock.patch.object(runner, "start"), mock.patch.object(runner, "until"):
                runner.start_clients()
            config = parse_properties(runner.work / "connect.properties")
            self.assertEqual("http://127.0.0.1:0", config["listeners"])


if __name__ == "__main__":
    unittest.main()
