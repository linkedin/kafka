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

"""Verify both bridge archives, wrapper dependencies and the migration scenario.

The shell entry point preserves existing environment-variable usage. This module
owns command selection, input checks, resume rules and the final result.
"""

import configparser
import datetime
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from audit_li_bridge_evidence import audit, verification_fingerprints
from li_bridge_commands import Commands
from stage_li_bridge_ivy import java_17


def write_json(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def selection(path):
    parser = configparser.ConfigParser(allow_no_value=True, interpolation=None)
    parser.optionxform = str
    with path.open(encoding="utf-8") as source:
        parser.read_file(source)
    if set(parser.sections()) != {"clients", "core", "storage"}:
        raise ValueError("Test selection must define clients, core and storage")
    result = {section: list(parser[section]) for section in parser.sections()}
    if any(not tests for tests in result.values()):
        raise ValueError("Each focused suite must contain at least one test")
    return result


class Verification:
    def __init__(self, root, environment):
        self.root = Path(root).resolve()
        self.bin = self.root / "tests/bin"
        self.env = dict(environment)
        self.wrapper = Path(self.env.get("WRAPPER_ROOT", str(Path.home() / "code/li/kafka-server"))).resolve()
        self.full = self.env.get("BRIDGE_VERIFY_FULL") == "1"
        self.resume = self.env.get("BRIDGE_VERIFY_RESUME") == "1"
        self.dry_run = self.env.get("BRIDGE_VERIFY_DRY_RUN") == "1"
        self.skip_stage = self.env.get("SKIP_LOCAL_STAGE") == "1"
        self.offline = self.env.get("WRAPPER_GRADLE_OFFLINE") == "1"
        self.legacy = Path(self.env["KAFKA_30_TGZ"]).resolve() if self.env.get("KAFKA_30_TGZ") else None
        self.supplied = Path(self.env["KAFKA_39_TGZ"]).resolve() if self.env.get("KAFKA_39_TGZ") else None
        self.broker = self.supplied
        if self.supplied and (not self.skip_stage or not self.legacy):
            raise ValueError("KAFKA_39_TGZ requires KAFKA_30_TGZ and SKIP_LOCAL_STAGE=1")
        if not self.dry_run and not self.legacy and self.env.get("ALLOW_PARTIAL") != "1":
            raise ValueError("Set KAFKA_30_TGZ for complete verification, or ALLOW_PARTIAL=1 for local checks")
        java_17(self.env)
        if not self.wrapper.is_dir():
            raise ValueError(f"Wrapper checkout not found at {self.wrapper}")
        default_version = next(line.split("=", 1)[1] for line in (self.root / "gradle.properties").read_text().splitlines()
                               if line.startswith("version="))
        self.version = self.env.get("LI_BRIDGE_VERSION", default_version)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.evidence = Path(self.env.get("EVIDENCE_DIR", f"/tmp/li-bridge-verification-{stamp}")).resolve()
        if self.evidence.is_relative_to(self.root) or self.evidence.is_relative_to(self.wrapper):
            raise ValueError("EVIDENCE_DIR must be outside both source checkouts")
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.fingerprint_file = self.evidence / "source-fingerprints.json"
        self.fingerprints = None
        self.started = False
        self.commands = None

    def current_fingerprints(self):
        result = verification_fingerprints(self.root, self.wrapper, self.legacy, self.skip_stage,
                                           self.supplied, self.version)
        # Input filenames can disappear during core:clean. Identify archives by their
        # retained, content-addressed path before writing or reusing any stage result.
        for key, generation in (("legacy_archive", "3.0"), ("broker_archive", "3.9")):
            archive = result["inputs"][key]
            if archive is not None:
                archive["path"] = str(self.evidence / "archives" / f"kafka-{generation}-{archive['sha256']}.tgz")
        return result

    def check_inputs(self):
        if self.dry_run:
            return
        current = self.current_fingerprints()
        if self.resume:
            if not self.fingerprint_file.is_file():
                raise ValueError("Cannot resume without source-fingerprints.json")
            recorded = json.loads(self.fingerprint_file.read_text())
            if recorded != current:
                raise ValueError("Source, archive, staging or scenario inputs changed; use a new evidence directory")
        else:
            write_json(self.fingerprint_file, current)
        self.fingerprints = current

    def command(self, label, args, cwd=None, extra_env=None, always=False):
        self.commands.run(label, args, cwd or self.root, extra_env, always)

    def gradle(self, label, tasks, cwd=None):
        directory = cwd or self.root
        self.command(label, [directory / "gradlew", "--no-daemon", *tasks], directory)

    def snapshot(self, path, generation, label):
        manifest = self.evidence / f"archive-{generation.replace('.', '')}.json"
        self.command(label, [sys.executable, self.bin / "li_bridge_artifacts.py", "snapshot", "--archive", path,
                             "--generation", generation, "--output-dir", self.evidence / "archives",
                             "--output-json", manifest], always=True)
        return path if self.dry_run else Path(json.loads(manifest.read_text())["path"])

    def wrapper_artifacts(self, after=False):
        arguments = [sys.executable, self.bin / "li_bridge_artifacts.py", "wrapper",
                     "--archive-json", self.evidence / "archive-39.json", "--wrapper-root", self.wrapper,
                     "--classpath-json", self.evidence / "wrapper-classpath.json"]
        if not after:
            tasks = [self.wrapper / "gradlew", "--no-daemon", "-I", self.bin / "li_bridge_wrapper_artifacts.gradle",
                     "liBridgeArtifactClasspath", "-x", "format"]
            if self.offline:
                tasks.append("--offline")
            self.command("wrapper-classpath", tasks, self.wrapper,
                         {"BRIDGE_WRAPPER_ARTIFACTS_OUTPUT": str(self.evidence / "wrapper-classpath.json")}, always=True)
            arguments += ["--output-json", self.evidence / "wrapper-artifacts.json"]
            self.command("wrapper-artifacts", arguments, always=True)
        else:
            arguments += ["--output-json", self.evidence / "wrapper-artifacts-after.json",
                          "--compare-report", self.evidence / "wrapper-artifacts.json"]
            self.command("wrapper-artifacts-unchanged", arguments, always=True)

    def summary(self, passed, error=None):
        result = {"passed": passed,
                  "finished_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "level": "full-suites" if self.full else "focused"}
        if self.fingerprints:
            for name, directory in (("kafka", self.root), ("wrapper", self.wrapper)):
                commit = subprocess.check_output(["git", "-C", str(directory), "rev-parse", "HEAD"], text=True).strip()
                dirty = bool(subprocess.check_output(["git", "-C", str(directory), "status", "--porcelain"], text=True))
                result[name] = {"commit": commit, "dirty": dirty, "source_sha256": self.fingerprints[name]["sha256"]}
        if error:
            result["error"] = str(error)
        return result

    def finish(self):
        if self.dry_run:
            print(f"Bridge verification plan rendered: {self.evidence}")
            return
        if self.current_fingerprints() != self.fingerprints:
            raise ValueError("Source, archive or scenario inputs changed during verification")
        if not self.legacy:
            result = self.summary(False)
            result["partial_checks_passed"] = True
            write_json(self.evidence / "verification-summary.json", result)
            print(f"Partial checks passed; cross-artifact verification was not run: {self.evidence}")
            return
        started = time.monotonic()
        candidate = self.summary(True)
        # Do not write a successful summary before the final audit. The candidate exists
        # only in memory; failure publishes passed=false and cannot leave a stale green file.
        result = audit(self.evidence, require_full=self.full, require_archives=True,
                       require_stage=not self.skip_stage, summary_override=candidate)
        write_json(self.evidence / "evidence-audit.json", result)
        self.commands.record("evidence-audit", started, "passed" if result["passed"] else "failed(1)")
        candidate["passed"] = result["passed"]
        write_json(self.evidence / "verification-summary.json", candidate)
        if not result["passed"]:
            raise ValueError("Evidence audit failed: " + "; ".join(result["issues"]))
        print(f"Bridge verification passed: {self.evidence}")

    def run(self):
        self.check_inputs()
        self.commands = Commands(self.evidence, self.env, self.resume, self.dry_run)
        self.started = True
        if not self.dry_run:
            write_json(self.evidence / "verification-summary.json", self.summary(False, "Verification is running"))
        if self.legacy:
            self.legacy = self.snapshot(self.legacy, "3.0", "archive-input-30")
        if self.supplied:
            self.supplied = self.snapshot(self.supplied, "3.9", "provided-39")
            self.broker = self.supplied
            self.command("provided-wrapper-version", [sys.executable, self.bin / "li_bridge_artifacts.py", "wrapper",
                         "--archive-json", self.evidence / "archive-39.json", "--wrapper-root", self.wrapper], always=True)
        self.command("preflight-unit", [sys.executable, "-m", "unittest", "discover", "-s", "tests/unit",
                                       "-p", "li_bridge*_test.py"])
        self.command("source-whitespace", ["git", "diff", "--check"])
        self.command("wrapper-whitespace", ["git", "diff", "--check"], self.wrapper)
        for scala in ("2.12", "2.13"):
            self.gradle(f"scala-{scala.replace('.', '')}-compile", ["--max-workers=4", f"-PscalaVersion={scala}",
                        "core:clean", "clients:compileTestJava", "core:compileScala", "core:compileTestScala",
                        "jmh-benchmarks:compileJava"])
        for project, filters in selection(self.bin / "li_bridge_test_selection.ini").items():
            tasks = [f"{project}:cleanTest", f"{project}:test", "--max-workers=4"]
            for pattern in filters:
                tasks += ["--tests", pattern]
            self.gradle(f"{project}-bridge-tests", tasks)
        self.gradle("vendor-pagination", ["--max-workers=3", "-PscalaVersion=2.12", "-I",
                                          "tests/bin/li_bridge_zookeeper_test.gradle", "core:liZookeeperPaginationTest"])
        if self.full:
            for project in ("clients", "server", "storage"):
                self.gradle(f"{project}-full", [f"{project}:cleanTest", f"{project}:test", "--max-workers=4"])
        if not self.skip_stage:
            self.command("stage-wrapper-artifacts", [self.bin / "stage_li_bridge_ivy.sh"],
                         extra_env={"LI_BRIDGE_VERSION": self.version})
        if self.legacy:
            if not self.supplied:
                archive = self.root / f"core/build/distributions/kafka_2.12-{self.version}.tgz"
                manifest = self.evidence / "archive-39.json"
                if self.resume and self.commands.results.get("release-39") == "passed" and manifest.is_file():
                    archive = Path(json.loads(manifest.read_text())["path"])
                self.gradle("release-39", ["-PscalaVersion=2.12", f"-Pversion={self.version}", "core:releaseTarGz"])
                self.broker = self.snapshot(archive, "3.9", "archive-built-39")
            self.wrapper_artifacts()
            tasks = [self.wrapper / "gradlew", "--no-daemon", ":likafka:kafka-impl_2.12:cleanTest",
                     ":likafka:kafka-impl_2.12:test", "-x", "format"]
            if self.offline:
                tasks.append("--offline")
            self.command("wrapper-tests", tasks, self.wrapper, {"KAFKA_30_TGZ": str(self.legacy)})
            self.wrapper_artifacts(after=True)
            self.gradle("stop-kafka-gradle", ["--stop"])
            self.gradle("stop-wrapper-gradle", ["--stop"], self.wrapper)
            self.command("mixed-process", [self.bin / "li_bridge_mixed_cluster_smoke.sh"], extra_env={
                "KAFKA_30_TGZ": str(self.legacy), "KAFKA_39_TGZ": str(self.broker),
                "EVIDENCE_DIR": str(self.evidence / "mixed-process")})
        self.finish()


def main():
    verification = None
    def interrupted(_signum, _frame):
        raise InterruptedError("Verification interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        verification = Verification(Path(__file__).resolve().parents[2], os.environ)
        verification.run()
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        if verification and verification.started and not verification.dry_run:
            write_json(verification.evidence / "verification-summary.json", verification.summary(False, error))
        print(f"Bridge verification failed: {error}", file=sys.stderr)
        return 2 if verification is None else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
