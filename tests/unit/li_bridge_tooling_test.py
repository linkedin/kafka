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
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path


BIN = Path(__file__).parents[1] / "bin"
SPEC = importlib.util.spec_from_file_location("li_bridge_artifacts", BIN / "li_bridge_artifacts.py")
ARTIFACTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ARTIFACTS)


class LiBridgeToolingTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def java_home(self):
        java = self.root / "jdk/bin/java"
        java.parent.mkdir(parents=True, exist_ok=True)
        java.write_text('#!/usr/bin/env python3\nimport sys\nprint(\'openjdk version "17.0.5"\', file=sys.stderr)\n')
        java.chmod(0o755)
        return str(java.parents[1])

    def verifier_plan(self, supplied, skip_stage):
        wrapper = self.root / "wrapper"
        wrapper.mkdir(exist_ok=True)
        environment = dict(os.environ, JAVA_HOME=self.java_home(), WRAPPER_ROOT=str(wrapper),
                           BRIDGE_VERIFY_DRY_RUN="1", BRIDGE_VERIFY_RESUME="0", BRIDGE_VERIFY_FULL="0",
                           KAFKA_30_TGZ=str(self.root / "kafka_2.12-3.0.1.83.tgz"), SKIP_LOCAL_STAGE=skip_stage,
                           LI_BRIDGE_VERSION="3.9.2.19", EVIDENCE_DIR=str(self.root / "plan"))
        environment.pop("KAFKA_39_TGZ", None)
        if supplied:
            environment["KAFKA_39_TGZ"] = str(self.root / "kafka_2.12-3.9.2.17.tgz")
        return subprocess.run(["bash", str(BIN / "verify_li_bridge.sh")], env=environment,
                              capture_output=True, text=True)

    def test_supplied_verifier_plan_does_not_build_or_stage(self):
        result = self.verifier_plan(supplied=True, skip_stage="1")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("provided-39", result.stdout)
        self.assertIn("KAFKA_39_TGZ=" + str((self.root / "kafka_2.12-3.9.2.17.tgz").resolve()), result.stdout)
        self.assertIn("wrapper-artifacts", result.stdout)
        self.assertNotIn("releaseTarGz", result.stdout)
        self.assertNotIn("stage-wrapper-artifacts", result.stdout)

    def test_supplied_archive_requires_staging_disabled(self):
        result = self.verifier_plan(supplied=True, skip_stage="0")
        self.assertEqual(2, result.returncode)
        self.assertIn("SKIP_LOCAL_STAGE=1", result.stderr)

    def test_staging_passes_version_to_gradle_and_limits_the_manifest(self):
        project = self.root / "kafka"
        script = project / "tests/bin/stage_li_bridge_ivy.sh"
        script.parent.mkdir(parents=True)
        shutil.copy2(BIN / script.name, script)
        shutil.copy2(BIN / "stage_li_bridge_ivy.py", script.with_suffix(".py"))
        subprocess.run(["git", "init", "-q", str(project)], check=True)
        gradle = project / "gradlew"
        gradle.write_text('''#!/usr/bin/env python3
import os, sys
from pathlib import Path
version = os.environ["LI_BRIDGE_VERSION"]
assert "-Pversion=" + version in sys.argv
assert "-PscalaVersion=2.12" in sys.argv
projects = {"clients": "kafka-clients", "core": "kafka_2.12", "server": "kafka-server",
            "server-common": "kafka-server-common", "storage": "kafka-storage",
            "storage/api": "kafka-storage-api", "metadata": "kafka-metadata", "raft": "kafka-raft",
            "group-coordinator": "kafka-group-coordinator",
            "group-coordinator/group-coordinator-api": "kafka-group-coordinator-api",
            "transaction-coordinator": "kafka-transaction-coordinator"}
for directory, module in projects.items():
    output = Path(directory) / "build/libs"
    output.mkdir(parents=True, exist_ok=True)
    (output / (module + "-" + version + ".jar")).write_bytes(b"main jar")
    if directory in ("clients", "core", "server-common"):
        (output / (module + "-" + version + "-test.jar")).write_bytes(b"test jar")
''')
        gradle.chmod(0o755)
        repository = self.root / "ivy"
        foreign = repository / "com/linkedin/kafka/kafka-extra/3.9.2.19/foreign.jar"
        foreign.parent.mkdir(parents=True)
        foreign.write_bytes(b"unrelated artifact")
        result = subprocess.run(["bash", str(script)], capture_output=True, text=True,
                                env=dict(os.environ, JAVA_HOME=self.java_home(), LI_IVY_REPO=str(repository),
                                         LI_BRIDGE_VERSION="3.9.2.19"))
        self.assertEqual(0, result.returncode, result.stderr)
        manifest = json.loads((repository / "com/linkedin/kafka/bridge-artifact-manifest.json").read_text())
        self.assertEqual("3.9.2.19", manifest["version"])
        self.assertEqual(25, len(manifest["files"]))
        self.assertTrue(all("/3.9.2.19/" in artifact["path"] for artifact in manifest["files"]))
        self.assertFalse(any("foreign.jar" in artifact["path"] for artifact in manifest["files"]))

    def test_local_verifier_plan_uses_requested_build_version(self):
        result = self.verifier_plan(supplied=False, skip_stage="0")
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("-Pversion=3.9.2.19", result.stdout)
        self.assertIn("kafka_2.12-3.9.2.19.tgz", result.stdout)
        self.assertIn("LI_BRIDGE_VERSION=3.9.2.19", result.stdout)


if __name__ == "__main__":
    unittest.main()
