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

import importlib.util
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "bin" / "li_bridge_preflight.py"
SPEC = importlib.util.spec_from_file_location("li_bridge_preflight", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load preflight module from {SCRIPT}")
PREFLIGHT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PREFLIGHT)


class LiBridgePreflightTest(unittest.TestCase):
    def mixed_properties(self):
        properties = {
            "broker.id": "1",
            "inter.broker.protocol.version": "3.0",
            "li.protocol.bridge.mode.enable": "true",
            "remote.log.storage.system.enable": "false",
            "li.drop.corrupted.files.enable": "false",
        }
        for gate in PREFLIGHT.MIXED_REQUIRED_GATES:
            properties[gate] = "true"
        return properties

    def test_valid_mixed_config(self):
        issues, details = PREFLIGHT.inspect_config(
            Path("broker.properties"), self.mixed_properties(), "mixed", False)
        self.assertEqual([], issues)
        self.assertEqual("3.0", details["inter_broker_protocol_version"])
        self.assertTrue(details["bridge_mode"])

    def test_mixed_config_rejects_new_ibp_and_unsafe_features(self):
        properties = self.mixed_properties()
        properties["inter.broker.protocol.version"] = "3.9"
        properties["remote.log.storage.system.enable"] = "true"
        properties["li.async.fetcher.enable"] = "true"
        issues, _ = PREFLIGHT.inspect_config(Path("broker.properties"), properties, "mixed", False)
        rendered = "\n".join(issues)
        self.assertIn("mixed phase requires IBP 3.0", rendered)
        self.assertIn("remote storage is enabled", rendered)
        self.assertIn("li.async.fetcher.enable is enabled", rendered)

    def test_require_all_gates_reports_missing_operational_gates(self):
        issues, _ = PREFLIGHT.inspect_config(
            Path("broker.properties"), self.mixed_properties(), "mixed", True)
        self.assertEqual(len(PREFLIGHT.BRIDGE_GATES) - len(PREFLIGHT.MIXED_REQUIRED_GATES), len(issues))

    def test_legacy_broker_requires_only_bridge_mode(self):
        properties = {
            "broker.id": "0",
            "inter.broker.protocol.version": "3.0",
            "li.protocol.bridge.mode.enable": "true",
        }
        issues, details = PREFLIGHT.inspect_config(
            Path("legacy.properties"), properties, "mixed", True, "3.0")
        self.assertEqual([], issues)
        self.assertEqual("3.0", details["generation"])

    def test_native_phase_requires_bridge_mode_off(self):
        properties = self.mixed_properties()
        properties["inter.broker.protocol.version"] = "3.9"
        properties["li.protocol.bridge.mode.enable"] = "false"
        issues, details = PREFLIGHT.inspect_config(Path("broker.properties"), properties, "native", False)
        self.assertEqual([], issues)
        self.assertFalse(details["bridge_mode"])

    def test_native_phase_rejects_old_ibp(self):
        properties = self.mixed_properties()
        properties["inter.broker.protocol.version"] = "3.8"
        properties["li.protocol.bridge.mode.enable"] = "false"
        issues, _ = PREFLIGHT.inspect_config(Path("broker.properties"), properties, "native", False)
        self.assertTrue(any("native phase requires IBP 3.9" in issue for issue in issues))

    def test_parse_rendered_properties(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.properties"
            path.write_text(
                "# rendered config\n"
                "broker.id=7\n"
                "listeners=PLAINTEXT://localhost:9092\\\n"
                ",SSL://localhost:9093\n",
                encoding="utf-8")
            self.assertEqual(
                {"broker.id": "7", "listeners": "PLAINTEXT://localhost:9092,SSL://localhost:9093"},
                PREFLIGHT.parse_properties(path))

    def test_gate_manifest_matches_kafka_config(self):
        source = (Path(__file__).parents[2] / "core/src/main/scala/kafka/server/KafkaConfig.scala").read_text(
            encoding="utf-8")
        source_gates = set(re.findall(r'"(li\.protocol\.bridge\.[^"]+\.enable)"', source))
        self.assertEqual(set(PREFLIGHT.BRIDGE_GATES), source_gates)

    def test_parse_zookeeper_children(self):
        self.assertEqual(["0", "1"], PREFLIGHT.parse_children("Connecting...\n[0, 1]\n"))
        self.assertEqual([], PREFLIGHT.parse_children("[]\n"))
        self.assertIsNone(PREFLIGHT.parse_children("Node does not exist: /brokers/corrupted\n"))

    def test_inspect_zookeeper_parses_state(self):
        outputs = {
            "ls /brokers/ids": "[1, 2]\n",
            "ls /brokers/corrupted": "Node does not exist: /brokers/corrupted\n",
            "ls /brokers/shutdown": "[]\n",
            "ls /brokers/preferred_controllers": "[2]\n",
            "ls /federatedTopics": "[west]\n",
            "get /topic_deletion_flag": "true\n",
        }
        with mock.patch.object(PREFLIGHT, "zk_command",
                               side_effect=lambda _home, _connect, command: outputs[command]):
            issues, state = PREFLIGHT.inspect_zookeeper(Path("kafka"), "localhost:2181", ["1", "2"])

        self.assertEqual([], issues)
        self.assertEqual(["1", "2"], state["/brokers/ids"])
        self.assertIsNone(state["/brokers/corrupted"])
        self.assertEqual(["2"], state["/brokers/preferred_controllers"])
        self.assertEqual(["west"], state["/federatedTopics"])
        self.assertEqual("present", state["/topic_deletion_flag"])

    def test_inspect_zookeeper_reports_inventory_and_corruption(self):
        def output(_home, _connect, command):
            if command == "ls /brokers/ids":
                return "[1, 3]\n"
            if command == "ls /brokers/corrupted":
                return "[3]\n"
            if command == "get /topic_deletion_flag":
                return "Node does not exist: /topic_deletion_flag\n"
            return "[]\n"

        with mock.patch.object(PREFLIGHT, "zk_command", side_effect=output):
            issues, state = PREFLIGHT.inspect_zookeeper(Path("kafka"), "localhost:2181", ["1", "2"])

        self.assertEqual("missing", state["/topic_deletion_flag"])
        self.assertEqual(2, len(issues))
        self.assertIn("do not match rendered configs", issues[0])
        self.assertIn("corrupted-broker state", issues[1])


if __name__ == "__main__":
    unittest.main()
