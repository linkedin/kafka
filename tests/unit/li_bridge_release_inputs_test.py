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

"""Release-input validation fixtures, not evidence of a qualified Kafka release."""

import io
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parents[1] / "bin"))
import verify_li_bridge_release as RELEASE


class LiBridgeReleaseInputsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.root, kafka_commit = self.checkout("kafka")
        self.wrapper, wrapper_commit = self.checkout("wrapper")
        self.env = {"KAFKA_30_COMMIT": "a" * 40, "KAFKA_39_COMMIT": kafka_commit,
                    "WRAPPER_COMMIT": wrapper_commit, "WRAPPER_ROOT": str(self.wrapper),
                    "EVIDENCE_DIR": str(self.directory / "evidence")}
        for generation in ("30", "39"):
            self.set_archive(generation, self.env[f"KAFKA_{generation}_COMMIT"][:16])

    def checkout(self, name):
        root = self.directory / name
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        (root / "tracked").write_text(name)
        subprocess.run(["git", "-C", str(root), "add", "tracked"], check=True)
        subprocess.run(["git", "-C", str(root), "-c", "user.name=Fixture",
                        "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false",
                        "commit", "-qm", name], check=True)
        commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
        return root, commit

    def set_archive(self, generation, encoded):
        version = "3.0.1.83" if generation == "30" else "3.9.2.17"
        jar = io.BytesIO()
        with zipfile.ZipFile(jar, "w") as zipped:
            zipped.writestr("kafka/kafka-version.properties", f"version={version}\ncommitId={encoded}\n")
        path = self.directory / f"kafka-{generation}.tgz"
        with tarfile.open(path, "w:gz") as archive:
            for name, data in ((f"kafka-clients-{version}.jar", jar.getvalue()),
                               (f"kafka_2.12-{version}.jar", b"fixture core")):
                entry = tarfile.TarInfo(f"kafka/libs/{name}")
                entry.size = len(data)
                archive.addfile(entry, io.BytesIO(data))
        self.env[f"KAFKA_{generation}_TGZ"] = str(path)
        self.env[f"KAFKA_{generation}_SHA256"] = RELEASE.file_sha256(path)

    def test_matching_archives_and_clean_checkouts_pass_identity_validation(self):
        RELEASE.check_inputs(self.root, self.env)

    def test_every_required_input_is_enforced(self):
        for name in RELEASE.REQUIRED_INPUTS:
            with self.subTest(name=name):
                missing = dict(self.env)
                missing.pop(name)
                with self.assertRaisesRegex(ValueError, name):
                    RELEASE.check_inputs(self.root, missing)

    def test_refs_and_non_hex_checksums_are_not_immutable_identifiers(self):
        for name in ("KAFKA_30_COMMIT", "KAFKA_39_COMMIT", "WRAPPER_COMMIT",
                     "KAFKA_30_SHA256", "KAFKA_39_SHA256"):
            for value in ("main", "a" * 12, "G" * 64):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    RELEASE.check_inputs(self.root, dict(self.env, **{name: value}))

    def test_archive_checksum_and_source_must_both_match(self):
        for generation in ("30", "39"):
            for suffix, value in (("SHA256", "0" * 64), ("COMMIT", "0" * 40)):
                with self.subTest(generation=generation, suffix=suffix), self.assertRaises(ValueError):
                    RELEASE.check_inputs(self.root, dict(self.env, **{f"KAFKA_{generation}_{suffix}": value}))

    def test_unknown_or_too_short_embedded_source_is_rejected(self):
        for generation in ("30", "39"):
            for encoded in ("unknown", "", self.env[f"KAFKA_{generation}_COMMIT"][:11]):
                with self.subTest(generation=generation, encoded=encoded):
                    self.set_archive(generation, encoded)
                    with self.assertRaisesRegex(ValueError, "Archive source metadata"):
                        RELEASE.check_inputs(self.root, self.env)
            self.set_archive(generation, self.env[f"KAFKA_{generation}_COMMIT"][:16])

    def test_dirty_checkout_and_wrong_wrapper_head_are_rejected(self):
        for root in (self.root, self.wrapper):
            for name in ("tracked", "untracked"):
                with self.subTest(root=root.name, name=name):
                    path = root / name
                    previous = path.read_bytes() if path.exists() else None
                    path.write_text("modified fixture")
                    with self.assertRaisesRegex(ValueError, "clean selected checkout"):
                        RELEASE.check_inputs(self.root, self.env)
                    if previous is None:
                        path.unlink()
                    else:
                        path.write_bytes(previous)
        with self.assertRaisesRegex(ValueError, "clean selected checkout"):
            RELEASE.check_inputs(self.root, dict(self.env, WRAPPER_COMMIT="0" * 40))

    def test_entry_point_forces_full_checks_and_audits_clean_archives(self):
        supplied = dict(self.env, SKIP_LOCAL_STAGE="0", BRIDGE_VERIFY_FULL="0", ALLOW_PARTIAL="1")
        with mock.patch.dict("os.environ", supplied, clear=True), \
                mock.patch.object(RELEASE, "check_inputs") as check, \
                mock.patch.object(RELEASE.subprocess, "run") as run:
            self.assertEqual(0, RELEASE.main())
        check.assert_called_once()
        self.assertEqual(2, run.call_count)
        for call in run.call_args_list:
            env = call.kwargs["env"]
            self.assertEqual(("1", "1", "0"),
                             (env["SKIP_LOCAL_STAGE"], env["BRIDGE_VERIFY_FULL"], env["ALLOW_PARTIAL"]))
            self.assertTrue(call.kwargs["check"])
        audit = run.call_args_list[1].args[0]
        for flag in ("--require-clean", "--require-full", "--require-archives", "--allow-missing-stage"):
            self.assertIn(flag, audit)

    def test_invalid_input_never_launches_the_verifier(self):
        with mock.patch.dict("os.environ", {}, clear=True), \
                mock.patch.object(RELEASE.subprocess, "run") as run, mock.patch("sys.stderr", new=io.StringIO()):
            self.assertEqual(1, RELEASE.main())
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
