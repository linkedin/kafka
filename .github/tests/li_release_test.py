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

"""Test release steps with a disposable Git remote and local command stubs."""

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("li_release", ROOT / ".github/scripts/li_release.py")
RELEASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RELEASE)


class ReleaseTest(unittest.TestCase):
    line = "3.9"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.old_cwd = Path.cwd()
        os.chdir(self.source)
        self.addCleanup(os.chdir, self.old_cwd)
        self.git("init", "-q")
        self.git("config", "user.name", "release-test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.git("config", "commit.gpgSign", "false")
        self.git("config", "tag.gpgSign", "false")
        self.remote = self.root / "remote.git"
        self.git("init", "-q", "--bare", str(self.remote))
        self.git("remote", "add", "origin", str(self.remote))
        self.prefix = RELEASE.PREFIXES[self.line]
        self.first = 82 if self.line == "3.0" else 1
        self.commits, self.tags = [], []
        for index in range(3):
            self.git("commit", "-q", "--allow-empty", "-m", f"Source {index}")
            self.commits.append(self.git("rev-parse", "HEAD"))
            tag = f"{self.prefix}.{self.first + index}"
            self.git("tag", tag)
            self.tags.append(tag)
        if self.line == "3.9":
            self.git("tag", "3.9.2", self.commits[0])
        self.git("push", "-q", "origin", "HEAD", "--tags")
        binaries = self.root / "bin"
        binaries.mkdir()
        gh = binaries / "gh"
        gh.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with Path(os.environ["GH_CALLS"]).open("a") as out:
    out.write(json.dumps(args) + "\\n")
if args[:2] == ["api", "--paginate"]:
    assert args[2] == "repos/local/release-test/releases?per_page=100"
    assert args[3:] == ["--jq", ".[] | select(.prerelease == false and .draft == false) | .tag_name"]
    print(os.environ["PUBLISHED_TAGS"])
elif args[:2] == ["api", "repos/local/release-test/releases/generate-notes"]:
    assert args[2:4] == ["--method", "POST"]
    fields = dict(args[i + 1].split("=", 1) for i in range(4, len(args) - 2, 2))
    assert fields["tag_name"] == os.environ["RELEASE_VERSION"]
    assert fields["target_commitish"] == os.environ["GITHUB_SHA"]
    assert fields.get("previous_tag_name", "") == os.environ.get("PREVIOUS_VERSION", "")
    assert args[-2:] == ["--jq", ".body"]
    print("Fixture release notes")
elif args[:2] == ["release", "view"]:
    sys.exit(0 if os.environ.get("EXISTING_RELEASE") == "1" else 1)
elif args[:2] not in (["release", "edit"], ["release", "create"], ["release", "upload"]):
    raise AssertionError(args)
''')
        gh.chmod(0o755)
        self.environment = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                                GITHUB_REPOSITORY="local/release-test", GITHUB_SHA=self.commits[1],
                                GITHUB_OUTPUT=str(self.root / "output"), GITHUB_ENV=str(self.root / "env"),
                                PUBLISHED_TAGS="\n".join(self.tags), GH_CALLS=str(self.root / "gh-calls.jsonl"),
                                SCALA_VERSION="2.12", RELEASE_VERSION=self.tags[1])
        self.patch = mock.patch.dict(os.environ, self.environment, clear=True)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def git(self, *args):
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()

    def test_retry_reuses_tag_and_notes_exclude_newer_commits(self):
        result = RELEASE.allocate(self.line, os.environ)
        self.assertEqual({"release_version": self.tags[1], "previous_version": self.tags[0]}, result)
        self.assertEqual(3 + (self.line == "3.9"), len(self.git("tag", "--list").splitlines()))

    def test_new_version_is_pushed_and_retry_does_not_allocate_again(self):
        self.git("commit", "-q", "--allow-empty", "-m", "New source")
        os.environ["GITHUB_SHA"] = self.git("rev-parse", "HEAD")
        self.git("push", "-q", "origin", "HEAD")
        result = RELEASE.allocate(self.line, os.environ)
        expected = f"{self.prefix}.{self.first + 3}"
        self.assertEqual(expected, result["release_version"])
        self.assertEqual(self.tags[2], result["previous_version"])
        self.assertEqual(os.environ["GITHUB_SHA"], self.git("rev-parse", expected + "^{}"))
        self.assertIn(os.environ["GITHUB_SHA"], self.git("ls-remote", "origin", f"refs/tags/{expected}^{{}}"))
        self.assertEqual(result, RELEASE.allocate(self.line, os.environ))

    def test_reserved_versions_are_not_reused(self):
        reserved = f"{self.prefix}.{self.first + 3}"
        self.git("tag", reserved, self.commits[0])
        self.git("push", "-q", "origin", "--tags")
        self.git("commit", "-q", "--allow-empty", "-m", "After reserved version")
        os.environ["GITHUB_SHA"] = self.git("rev-parse", "HEAD")
        self.assertEqual(f"{self.prefix}.{self.first + 4}", RELEASE.allocate(self.line, os.environ)["release_version"])

    def test_archive_checksum_works_after_download_and_notes_use_scala(self):
        gradle = self.source / "gradlew"
        gradle.write_text('''#!/usr/bin/env python3
import os, sys
from pathlib import Path
assert "-Pversion=" + os.environ["RELEASE_VERSION"] in sys.argv
assert "-PscalaVersion=" + os.environ["SCALA_VERSION"] in sys.argv
output = Path("core/build/distributions")
output.mkdir(parents=True, exist_ok=True)
(output / ("kafka_" + os.environ["SCALA_VERSION"] + "-" + os.environ["RELEASE_VERSION"] + ".tgz")).write_bytes(b"archive")
''')
        gradle.chmod(0o755)
        for scala in ("2.12", "2.13"):
            os.environ["SCALA_VERSION"] = scala
            archive = RELEASE.build_archive(os.environ)
            destination = self.root / f"download-{scala}"
            destination.mkdir()
            shutil.copyfile(archive, destination / archive.name)
            shutil.copyfile(str(archive) + ".sha256", destination / (archive.name + ".sha256"))
            digest, filename = (destination / (archive.name + ".sha256")).read_text().split()
            self.assertEqual(archive.name, filename)
            self.assertEqual(digest, hashlib.sha256((destination / filename).read_bytes()).hexdigest())
            checksum_tool = shutil.which("sha256sum")
            if checksum_tool:
                subprocess.run([checksum_tool, "-c", archive.name + ".sha256"], cwd=destination, check=True)
            for previous in (self.tags[0], ""):
                os.environ["PREVIOUS_VERSION"] = previous
                text = RELEASE.notes(os.environ)
                self.assertIn(f"Scala {scala}", text)
                self.assertIn("Fixture release notes", text)
                self.assertIn(os.environ["GITHUB_SHA"], text)

    def test_publication_create_and_retry_paths(self):
        archive = self.source / "archive.tgz"
        archive.write_bytes(b"archive")
        Path(str(archive) + ".sha256").write_text("checksum")
        os.environ["RELEASE_ARCHIVE"] = str(archive)
        for existing in ("0", "1"):
            os.environ["EXISTING_RELEASE"] = existing
            RELEASE.publish(os.environ)
        calls = [json.loads(line) for line in Path(os.environ["GH_CALLS"]).read_text().splitlines()]
        self.assertTrue(any(call[:2] == ["release", "create"] and "--verify-tag" in call for call in calls))
        self.assertTrue(any(call[:2] == ["release", "edit"] for call in calls))
        self.assertTrue(any(call[:2] == ["release", "upload"] and "--clobber" in call for call in calls))

    def test_workflow_calls_the_tested_steps_and_keeps_release_queue(self):
        workflow = (ROOT / ".github/workflows/li-release.yml").read_text()
        for command in (f"allocate --line {self.line}", "archive", "notes", "publish"):
            self.assertIn("run: python3 .github/scripts/li_release.py " + command, workflow)
        self.assertIn("  queue: max\n", workflow)
        self.assertIn("  cancel-in-progress: false\n", workflow)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("line", choices=("3.0", "3.9"))
    arguments, remaining = parser.parse_known_args()
    ReleaseTest.line = arguments.line
    unittest.main(argv=[sys.argv[0], *remaining])
