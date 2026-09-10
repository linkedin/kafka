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


class LiBridgeArtifactsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def archive(self, version="3.9.2.17", scala="2.12", extra=None):
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, "w") as source:
            source.writestr("kafka/kafka-version.properties", f"version={version}\ncommitId=abcdef0123456789\n")
        libraries = {f"kafka-clients-{version}.jar": jar.getvalue(),
                     f"kafka_{scala}-{version}.jar": b"core jar"}
        path = self.root / f"kafka_{scala}-{version}.tgz"
        with tarfile.open(path, "w:gz") as archive:
            for name, data in libraries.items():
                member = tarfile.TarInfo(f"kafka_{scala}-{version}/libs/{name}")
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
            if extra is not None:
                archive.addfile(extra)
        return path, libraries

    def wrapper(self, libraries, version="3.9.2.17", scala="2.12"):
        root = self.root / "wrapper"
        root.mkdir(exist_ok=True)
        spec = {"build": {"versions": {"linkedin-kafka": version, "baseScala": scala}},
                "external": {"kafka": f"com.linkedin.kafka:kafka_{scala}:{version}",
                             "clients-test": f"com.linkedin.kafka:kafka-clients:{version}:test"}}
        (root / "product-spec.json").write_text(json.dumps(spec))
        artifacts = []
        for filename, data in libraries.items():
            path = root / filename
            path.write_bytes(data)
            artifacts.append({"group": "com.linkedin.kafka", "name": filename.removesuffix(f"-{version}.jar"),
                              "version": version, "classifier": None, "path": str(path)})
        return root, artifacts

    def test_snapshots_include_versions_and_survive_build_cleanup(self):
        for version, generation in (("3.0.1.83", "3.0"), ("3.9.2.17", "3.9")):
            with self.subTest(version=version):
                archive, _ = self.archive(version)
                info = ARTIFACTS.snapshot_archive(archive, generation, self.root / "evidence/archives")
                self.assertEqual(version, info["version"])
                self.assertEqual("2.12", info["scala_version"])
                self.assertEqual(archive.read_bytes(), Path(info["path"]).read_bytes())
                archive.unlink()
                self.assertEqual(info["sha256"], ARTIFACTS.file_sha256(info["path"]))
                again = ARTIFACTS.snapshot_archive(info["path"], generation, self.root / "evidence/archives")
                self.assertEqual(info["path"], again["path"])

    def test_generation_comes_from_jar_metadata(self):
        archive, _ = self.archive("3.0.1.83")
        renamed = archive.with_name("kafka_2.12-3.9.2.17.tgz")
        archive.rename(renamed)
        with self.assertRaisesRegex(ValueError, "Expected a Kafka 3.9 archive"):
            ARTIFACTS.snapshot_archive(renamed, "3.9", self.root / "evidence")

    def test_corrupted_retained_archive_is_rejected(self):
        archive, _ = self.archive()
        info = ARTIFACTS.snapshot_archive(archive, "3.9", self.root / "evidence")
        Path(info["path"]).write_bytes(b"corrupted archive")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            ARTIFACTS.snapshot_archive(archive, "3.9", self.root / "evidence")

    def test_unsafe_archive_entries_are_rejected(self):
        path_traversal = tarfile.TarInfo("../outside")
        symlink = tarfile.TarInfo("kafka/escape")
        symlink.type = tarfile.SYMTYPE
        symlink.linkname = "/outside"
        for entry in (path_traversal, symlink):
            with self.subTest(entry=entry.name):
                archive, _ = self.archive(extra=entry)
                with self.assertRaisesRegex(ValueError, "Unsafe archive entry"):
                    ARTIFACTS.inspect_archive(archive, "3.9")

    def test_missing_version_metadata_is_rejected(self):
        archive = self.root / "empty.tgz"
        with tarfile.open(archive, "w:gz"):
            pass
        with self.assertRaisesRegex(ValueError, "no Kafka clients"):
            ARTIFACTS.inspect_archive(archive, "3.9")

    def test_wrapper_versions_and_main_jar_hashes_match_archive(self):
        path, libraries = self.archive()
        archive = ARTIFACTS.snapshot_archive(path, "3.9", self.root / "evidence")
        wrapper, artifacts = self.wrapper(libraries)
        ARTIFACTS.check_wrapper_spec(wrapper, archive)
        report = ARTIFACTS.check_wrapper_classpath(archive, artifacts)
        self.assertTrue(report["passed"])
        self.assertEqual(archive["sha256"], report["archive_sha256"])
        self.assertEqual(["kafka-clients", "kafka_2.12"], report["main_modules"])
        Path(artifacts[0]["path"]).write_bytes(b"another jar under the same version")
        with self.assertRaisesRegex(ValueError, "does not match the selected archive"):
            ARTIFACTS.check_wrapper_classpath(archive, artifacts)

    def test_wrapper_rejects_mixed_versions_and_missing_core(self):
        path, libraries = self.archive()
        archive = ARTIFACTS.snapshot_archive(path, "3.9", self.root / "evidence")
        wrapper, artifacts = self.wrapper(libraries)
        spec_path = wrapper / "product-spec.json"
        valid = json.loads(spec_path.read_text())
        for section, key, value in (("versions", "linkedin-kafka", "3.0.1.83"),
                                    ("versions", "baseScala", "2.13"),
                                    ("external", "clients-test", "com.linkedin.kafka:kafka-clients:3.0.1.83:test")):
            spec = json.loads(json.dumps(valid))
            target = spec["build"]["versions"] if section == "versions" else spec["external"]
            target[key] = value
            spec_path.write_text(json.dumps(spec))
            with self.subTest(key=key), self.assertRaises(ValueError):
                ARTIFACTS.check_wrapper_spec(wrapper, archive)
        with self.assertRaisesRegex(ValueError, "lacks Kafka core"):
            ARTIFACTS.check_wrapper_classpath(archive, artifacts[:1])
        artifacts[0]["version"] = "3.0.1.83"
        with self.assertRaisesRegex(ValueError, "unexpected Kafka coordinate"):
            ARTIFACTS.check_wrapper_classpath(archive, artifacts)

    def test_test_classifier_changes_are_recorded(self):
        path, libraries = self.archive()
        archive = ARTIFACTS.snapshot_archive(path, "3.9", self.root / "evidence")
        wrapper, artifacts = self.wrapper(libraries)
        test_jar = wrapper / "kafka-clients-3.9.2.17-test.jar"
        test_jar.write_bytes(b"test classes")
        artifacts.append({"group": "com.linkedin.kafka", "name": "kafka-clients", "version": "3.9.2.17",
                          "classifier": "test", "path": str(test_jar)})
        before = ARTIFACTS.check_wrapper_classpath(archive, artifacts)
        test_jar.write_bytes(b"changed test classes")
        self.assertNotEqual(before, ARTIFACTS.check_wrapper_classpath(archive, artifacts))

    def test_forged_archive_library_manifest_is_rejected(self):
        path, libraries = self.archive()
        archive = ARTIFACTS.snapshot_archive(path, "3.9", self.root / "evidence")
        wrapper, _ = self.wrapper(libraries)
        archive["libraries"]["kafka-clients-3.9.2.17.jar"] = "wrong"
        manifest = self.root / "archive.json"
        manifest.write_text(json.dumps(archive))
        result = subprocess.run(["python3", str(BIN / "li_bridge_artifacts.py"), "wrapper",
                                 "--archive-json", str(manifest), "--wrapper-root", str(wrapper)],
                                capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("metadata does not match", result.stderr)


if __name__ == "__main__":
    unittest.main()
