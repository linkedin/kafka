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

import datetime
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

    def test_missing_or_invalid_broker_id_is_rejected(self):
        for value in (None, "", "-1", "broker-1", "2147483648"):
            with self.subTest(value=value):
                properties = self.mixed_properties()
                if value is None:
                    properties.pop("broker.id")
                else:
                    properties["broker.id"] = value
                issues, _ = PREFLIGHT.inspect_config(Path("broker.properties"), properties, "mixed", False)
                self.assertTrue(any("broker.id" in issue for issue in issues))

    def test_ibp_parser_rejects_invalid_suffixes(self):
        for value in ("3.0-invalid", "3.0-IV2", "3.9-IV1", "3.9garbage", "3.0.99"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    PREFLIGHT.protocol_version(value)
        for value in ("3.0", "3.0-IV0", "3.0-IV1"):
            self.assertEqual((3, 0), PREFLIGHT.protocol_version(value))
        for value in ("3.9", "3.9-IV0"):
            self.assertEqual((3, 9), PREFLIGHT.protocol_version(value))

    def test_live_inventory_requires_configured_broker_ids(self):
        def output(_home, _connect, command):
            if command == "ls /brokers/ids":
                return "[0, 1, 2]\n"
            if command == "get /topic_deletion_flag":
                return "true\n"
            return "[]\n"
        with mock.patch.object(PREFLIGHT, "zk_command", side_effect=output):
            issues, _ = PREFLIGHT.inspect_zookeeper(Path("kafka"), "localhost:2181", [])
        self.assertTrue(any("No configured broker IDs" in issue for issue in issues))

    def test_duplicate_broker_ids_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            config = Path(directory) / "broker.properties"
            config.write_text("\n".join(f"{key}={value}" for key, value in self.mixed_properties().items()))
            with mock.patch("sys.argv", ["preflight", "--phase", "mixed", "--broker-config", str(config),
                                         "--broker-config", str(config)]), mock.patch("builtins.print"):
                self.assertEqual(1, PREFLIGHT.main())

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

    def test_legacy_broker_requires_bridge_mode_and_topic_cleanup(self):
        properties = {
            "broker.id": "0",
            "inter.broker.protocol.version": "3.0",
            "li.protocol.bridge.mode.enable": "true",
            "li.protocol.bridge.topic.deletion.state.cleanup.enable": "true",
        }
        issues, details = PREFLIGHT.inspect_config(
            Path("legacy.properties"), properties, "mixed", True, "3.0")
        self.assertEqual([], issues)
        self.assertEqual("3.0", details["generation"])

    def test_native_phase_requires_bridge_mode_off(self):
        properties = self.mixed_properties()
        properties["inter.broker.protocol.version"] = "3.0"
        properties["li.protocol.bridge.mode.enable"] = "false"
        issues, details = PREFLIGHT.inspect_config(Path("broker.properties"), properties, "native", False)
        self.assertEqual([], issues)
        self.assertFalse(details["bridge_mode"])

    def test_native_phase_rejects_premature_ibp_change(self):
        properties = self.mixed_properties()
        properties["inter.broker.protocol.version"] = "3.8"
        properties["li.protocol.bridge.mode.enable"] = "false"
        issues, _ = PREFLIGHT.inspect_config(Path("broker.properties"), properties, "native", False)
        self.assertTrue(any("native phase requires IBP 3.0" in issue for issue in issues))

    def test_legacy_production_features_are_preserved(self):
        properties = self.mixed_properties()
        properties.update({"li.async.fetcher.enable": "true", "li.combined.control.request.enable": "true"})
        issues, _ = PREFLIGHT.inspect_config(Path("legacy.properties"), properties, "mixed", True, "3.0")
        self.assertEqual([], issues)
        issues, _ = PREFLIGHT.inspect_config(Path("new.properties"), properties, "mixed", False, "3.9")
        self.assertEqual(2, len(issues))

    def test_all_phase_and_retained_gate_contracts(self):
        for phase, (ibp, mode, generations) in PREFLIGHT.PHASES.items():
            for generation in generations:
                with self.subTest(phase=phase, generation=generation):
                    properties = {gate: "true" for gate in PREFLIGHT.BRIDGE_GATES}
                    properties.update({"broker.id": "0", "inter.broker.protocol.version": ibp,
                                       PREFLIGHT.MODE: str(mode).lower()})
                    self.assertEqual([], PREFLIGHT.inspect_config(Path("broker"), properties, phase, True, generation)[0])
                    if generation == "3.9":
                        for gate in PREFLIGHT.BRIDGE_GATES[1:]:
                            disabled = dict(properties, **{gate: "false"})
                            issues, _ = PREFLIGHT.inspect_config(Path("broker"), disabled, phase, True, generation)
                            self.assertTrue(any(gate in issue for issue in issues))
        self.assertTrue(PREFLIGHT.transition_allowed("all-39-bridge", "mixed"))
        self.assertFalse(PREFLIGHT.transition_allowed("native", "mixed"))
        self.assertFalse(PREFLIGHT.transition_allowed("mixed", "ibp-39"))

    def test_delayed_election_and_malformed_flags_fail_closed(self):
        for generation in ("3.0", "3.9"):
            for delay in ("1", "1000", "-1", "bad"):
                properties = self.mixed_properties()
                properties["li.leader.election.on.corruption.wait.ms"] = delay
                self.assertTrue(PREFLIGHT.inspect_config(Path("broker"), properties, "mixed", False, generation)[0])
        properties["li.protocol.bridge.mode.enable"] = "broken"
        self.assertTrue(PREFLIGHT.inspect_config(Path("broker"), properties, "mixed", False)[0])

    def live_evidence(self):
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        properties = self.mixed_properties()
        properties["li.zookeeper.pagination.enable"] = "true"
        _, config = PREFLIGHT.inspect_config(Path("new"), properties, "all-39-bridge", False)
        inventory = {"contract_version": 2, "cluster_id": "cluster", "collected_at_utc": timestamp,
                     "brokers": [{"broker_id": "1", "properties": properties,
                                  "source": "AdminClient.describeConfigs(includeSynonyms=true)"}],
                     "topic_names": ["orders"], "topics": {"orders": {"remote.storage.enable": "false"}}}
        decisions = {name: {"owner": "oncall", "evidence": "sha256:retained-review", "disposition": "retained"}
                     for name in PREFLIGHT.INSPECTED_ZK_PATHS + ("remote-storage", "plugin-state", "client-floor", "artifact-admission")}
        decisions["remote-storage"]["disposition"] = "unused"
        decisions["artifact-admission"]["disposition"] = "bridge-artifacts-only"
        dispositions = {"contract_version": 2, "cluster_id": "cluster", "collected_at_utc": timestamp,
                        "decisions": decisions, "broker_runtimes": {"1": {
                            "pagination_supported": True, "zookeeper_sha256": "a" * 64, "jute_sha256": "b" * 64,
                            "server_version": "qualified-version", "qualification_evidence": "retained-runtime-test"}}}
        return inventory, dispositions, [config]

    def test_live_observations_require_complete_fresh_matching_evidence(self):
        inventory, dispositions, configs = self.live_evidence()
        self.assertEqual([], PREFLIGHT.inspect_live_inventory(inventory, dispositions, "cluster", configs,
                                                              "all-39-bridge", False, 900))
        for mutation in (
                lambda data: data.update(cluster_id="other"),
                lambda data: data.update(collected_at_utc="2000-01-01T00:00:00+00:00"),
                lambda data: data.update(brokers=[]),
                lambda data: data.update(topics={}),
                lambda data: data["topics"]["orders"].update({"remote.storage.enable": "true"}),
                lambda data: data["brokers"][0]["properties"].update({PREFLIGHT.MODE: "false"})):
            inventory, dispositions, configs = self.live_evidence()
            # Preserve the separately supplied rendered configuration.
            import copy
            inventory = copy.deepcopy(inventory)
            mutation(inventory)
            self.assertTrue(PREFLIGHT.inspect_live_inventory(inventory, dispositions, "cluster", configs,
                                                             "all-39-bridge", False, 900))
        inventory, dispositions, configs = self.live_evidence()
        dispositions["broker_runtimes"] = {}
        self.assertTrue(PREFLIGHT.inspect_live_inventory(inventory, dispositions, "cluster", configs,
                                                         "all-39-bridge", False, 900))
        dispositions["decisions"].pop("remote-storage")
        self.assertTrue(PREFLIGHT.inspect_live_inventory(inventory, dispositions, "cluster", configs,
                                                         "all-39-bridge", False, 900))

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
        self.assertEqual("true", state["topic_deletion_flag_value"])

    def test_deletion_flag_value_is_retained_and_invalid_data_is_rejected(self):
        for value in ("true", "false", "null", "", "invalid"):
            def output(_home, _connect, command):
                if command == "ls /brokers/ids":
                    return "[1]"
                if command == "stat /topic_deletion_flag":
                    return f"dataLength = {len(value)}\n"
                return value if command == "get /topic_deletion_flag" else "[]"
            with mock.patch.object(PREFLIGHT, "zk_command", side_effect=output):
                issues, state = PREFLIGHT.inspect_zookeeper(Path("kafka"), "localhost", ["1"])
            self.assertEqual(value != "invalid", not issues)
            self.assertEqual(value if value in ("true", "false") else None, state["topic_deletion_flag_value"])

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
