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

"""Require the selected release inputs before running complete qualification.

The caller supplies source commits and checksums from the approved release record.
This verifies their identity; it does not grant human or security approval.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

from li_bridge_artifacts import file_sha256, inspect_archive

REQUIRED_INPUTS = {
    "KAFKA_30_TGZ": "the published 3.0 archive",
    "KAFKA_39_TGZ": "the published 3.9 archive",
    "KAFKA_30_SHA256": "the published 3.0 checksum",
    "KAFKA_39_SHA256": "the published 3.9 checksum",
    "KAFKA_30_COMMIT": "the approved 3.0 source commit",
    "KAFKA_39_COMMIT": "the approved 3.9 source commit",
    "WRAPPER_COMMIT": "the approved wrapper commit",
    "WRAPPER_ROOT": "the wrapper checkout with official dependencies",
    "EVIDENCE_DIR": "an external evidence directory",
}


def check_inputs(root, environment):
    for name, description in REQUIRED_INPUTS.items():
        if not environment.get(name):
            raise ValueError(f"Supply {description}: {name}")
    for generation, prefix in (("3.0", "KAFKA_30"), ("3.9", "KAFKA_39")):
        archive = Path(environment[prefix + "_TGZ"])
        commit, checksum = environment[prefix + "_COMMIT"], environment[prefix + "_SHA256"]
        if not re.fullmatch(r"[0-9a-f]{40}", commit) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ValueError("Release commits and checksums must be explicit immutable hex values")
        if file_sha256(archive) != checksum:
            raise ValueError(f"Published checksum mismatch: {archive}")
        encoded = inspect_archive(archive, generation).get("commit_id") or ""
        if not re.fullmatch(r"[0-9a-f]{12,40}", encoded) or not commit.startswith(encoded):
            raise ValueError(f"Archive source metadata does not match the selected commit: {archive}")
    for checkout, expected in ((root, environment["KAFKA_39_COMMIT"]),
                               (Path(environment["WRAPPER_ROOT"]), environment["WRAPPER_COMMIT"])):
        if not re.fullmatch(r"[0-9a-f]{40}", expected):
            raise ValueError("Wrapper/source revisions must be 40-character hashes")
        actual = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
        dirty = subprocess.check_output(["git", "-C", str(checkout), "status", "--porcelain"], text=True)
        if actual != expected or dirty:
            raise ValueError(f"Release qualification requires the clean selected checkout: {checkout}")


def main():
    root = Path(__file__).resolve().parents[2]
    environment = dict(os.environ)
    try:
        check_inputs(root, environment)
        environment.update(SKIP_LOCAL_STAGE="1", BRIDGE_VERIFY_FULL="1", ALLOW_PARTIAL="0")
        subprocess.run([str(root / "tests/bin/verify_li_bridge.sh")], env=environment, check=True)
        evidence = Path(environment["EVIDENCE_DIR"])
        subprocess.run([sys.executable, str(root / "tests/bin/audit_li_bridge_evidence.py"),
                        "--evidence-dir", str(evidence), "--require-clean", "--require-full", "--require-archives",
                        "--allow-missing-stage", "--output-json", str(evidence / "release-evidence-audit.json")],
                       env=environment, check=True, timeout=300)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Release qualification blocked: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
