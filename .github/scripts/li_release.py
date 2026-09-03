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

"""Release bookkeeping for the independent 3.0 and 3.9 CI workflows.

Allocation pushes a tag. Publication changes GitHub releases. Run these commands
only in the release workflow; tests use a disposable local Git remote and stubs.
"""

import argparse
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

PREFIXES = {"3.0": "3.0.1", "3.9": "3.9.2"}
BOT_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"


def output(args):
    return subprocess.check_output(args, text=True).strip()


def run(args):
    subprocess.run(args, check=True)


def append_environment(path, values):
    with Path(path).open("a", encoding="utf-8") as destination:
        for key, value in values.items():
            if "\n" in str(value) or "\r" in str(value):
                raise ValueError(f"Invalid multiline environment value for {key}")
            destination.write(f"{key}={value}\n")


def tags_for_line(text, prefix):
    pattern = re.compile(re.escape(prefix) + r"\.\d+")
    return sorted({line for line in text.splitlines() if pattern.fullmatch(line)},
                  key=lambda tag: int(tag.rsplit(".", 1)[1]))


def is_ancestor(tag, commit):
    result = subprocess.run(["git", "merge-base", "--is-ancestor", tag, commit], check=False)
    if result.returncode not in (0, 1):
        raise subprocess.CalledProcessError(result.returncode, result.args)
    return result.returncode == 0


def allocate(line, environment):
    prefix = PREFIXES[line]
    commit = environment["GITHUB_SHA"]
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("GITHUB_SHA must be a full source commit hash")
    run(["git", "fetch", "--force", "--tags", "origin"])
    all_tags = tags_for_line(output(["git", "tag", "--list", prefix + ".*"]), prefix)
    existing = tags_for_line(output(["git", "tag", "--points-at", commit, "--list", prefix + ".*"]), prefix)
    if line == "3.0":
        published = output(["gh", "api", "--paginate",
                            f"repos/{environment['GITHUB_REPOSITORY']}/releases?per_page=100",
                            "--jq", ".[] | select(.prerelease == false and .draft == false) | .tag_name"])
        candidates = tags_for_line(published, prefix)
        first = 82  # Existing 3.0 release numbering starts after 3.0.1.82.
        previous = ""
    else:
        candidates = all_tags.copy()
        first = 0
        previous = "3.9.2"
    if existing:
        # Preserve the two release lines' existing retry policies.
        version = existing[0] if line == "3.0" else existing[-1]
    else:
        next_number = int(candidates[-1].rsplit(".", 1)[1]) + 1 if candidates else first + 1
        version = f"{prefix}.{next_number}"
        while version in all_tags:
            next_number += 1
            version = f"{prefix}.{next_number}"
        run(["git", "config", "user.name", "github-actions[bot]"])
        run(["git", "config", "user.email", BOT_EMAIL])
        run(["git", "tag", "--annotate", version, "--message", f"LinkedIn Kafka {version}", commit])
        run(["git", "push", "origin", f"refs/tags/{version}"])
    for tag in candidates:
        if tag != version and is_ancestor(tag, commit + "^"):
            previous = tag
    result = {"release_version": version, "previous_version": previous}
    append_environment(environment["GITHUB_OUTPUT"], result)
    return result


def release_parameters(environment):
    version, scala = environment["RELEASE_VERSION"], environment["SCALA_VERSION"]
    if not re.fullmatch(r"3\.(?:0\.1|9\.2)\.\d+", version) or scala not in ("2.12", "2.13"):
        raise ValueError("Expected an internal 3.0.1.N/3.9.2.N release and Scala 2.12 or 2.13")
    return version, scala


def build_archive(environment):
    version, scala = release_parameters(environment)
    run(["./gradlew", "--no-daemon", f"-Pversion={version}", f"-PscalaVersion={scala}", "core:releaseTarGz"])
    archive = Path("core/build/distributions") / f"kafka_{scala}-{version}.tgz"
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    # The checksum uses only the filename so it works from a download directory.
    Path(str(archive) + ".sha256").write_text(f"{digest.hexdigest()}  {archive.name}\n", encoding="utf-8")
    append_environment(environment["GITHUB_ENV"], {"RELEASE_ARCHIVE": str(archive)})
    return archive


def notes(environment):
    version, scala = release_parameters(environment)
    args = ["gh", "api", f"repos/{environment['GITHUB_REPOSITORY']}/releases/generate-notes", "--method", "POST",
            "-f", f"tag_name={version}", "-f", f"target_commitish={environment['GITHUB_SHA']}"]
    if environment.get("PREVIOUS_VERSION"):
        args += ["-f", "previous_tag_name=" + environment["PREVIOUS_VERSION"]]
    args += ["--jq", ".body"]
    generated = output(args) + "\n"
    Path("generated-notes.md").write_text(generated, encoding="utf-8")
    text = (f"LinkedIn Kafka {version}\n\nSource commit: `{environment['GITHUB_SHA']}`\n\n"
            f"Maven coordinates: `com.linkedin.kafka`, Scala {scala}\n\n{generated}")
    Path("release-notes.md").write_text(text, encoding="utf-8")
    return text


def publish(environment):
    version, _ = release_parameters(environment)
    archive = Path(environment["RELEASE_ARCHIVE"])
    checksum = Path(str(archive) + ".sha256")
    if not archive.is_file() or not checksum.is_file():
        raise ValueError("Release archive and checksum must exist before publication")
    exists = subprocess.run(["gh", "release", "view", version], stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, check=False).returncode == 0
    options = ["--title", f"LinkedIn Kafka {version}", "--notes-file", "release-notes.md",
               "--target", environment["GITHUB_SHA"]]
    if exists:
        run(["gh", "release", "edit", version, *options])
        run(["gh", "release", "upload", version, str(archive), str(checksum), "--clobber"])
    else:
        run(["gh", "release", "create", version, str(archive), str(checksum), *options, "--verify-tag"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("allocate", "archive", "notes", "publish"))
    parser.add_argument("--line", choices=tuple(PREFIXES))
    args = parser.parse_args()
    if args.command == "allocate" and not args.line:
        parser.error("allocate requires --line")
    try:
        if args.command == "allocate":
            allocate(args.line, os.environ)
        else:
            {"archive": build_archive, "notes": notes, "publish": publish}[args.command](os.environ)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f"Release step failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
