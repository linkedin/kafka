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

"""Retain bridge archives and check the wrapper against the selected broker artifact."""

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path, PurePosixPath


def stream_sha256(source):
    digest = hashlib.sha256()
    for chunk in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def file_sha256(path):
    with Path(path).open("rb") as source:
        return stream_sha256(source)


def inspect_archive(path, generation):
    libraries = {}
    properties = None
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            parts = PurePosixPath(member.name).parts
            if member.name.startswith("/") or ".." in parts or not (member.isfile() or member.isdir()):
                raise ValueError(f"Unsafe archive entry: {member.name}")
            if not member.isfile() or len(parts) != 3 or parts[1] != "libs":
                continue
            name = parts[-1]
            if not name.startswith("kafka") or not name.endswith(".jar"):
                continue
            if name in libraries:
                raise ValueError(f"Duplicate Kafka library in archive: {name}")
            with archive.extractfile(member) as source:
                if re.fullmatch(r"kafka-clients-[0-9].*\.jar", name) and not name.endswith("-test.jar"):
                    if properties is not None:
                        raise ValueError("Archive contains multiple Kafka clients jars")
                    data = source.read()
                    with zipfile.ZipFile(io.BytesIO(data)) as jar:
                        text = jar.read("kafka/kafka-version.properties").decode("utf-8")
                    properties = dict(line.split("=", 1) for line in text.splitlines()
                                      if "=" in line and not line.startswith("#"))
                    libraries[name] = hashlib.sha256(data).hexdigest()
                else:
                    libraries[name] = stream_sha256(source)
    if properties is None:
        raise ValueError("Archive has no Kafka clients version metadata")
    version = properties.get("version", "")
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:\.\d+)?(?:-SNAPSHOT)?", version) or not version.startswith(generation + "."):
        raise ValueError(f"Expected a Kafka {generation} archive, found version {version!r}")
    core_names = [name for name in libraries if re.fullmatch(r"kafka_2\.(?:12|13)-" + re.escape(version) + r"\.jar", name)]
    if len(core_names) != 1 or f"kafka-clients-{version}.jar" not in libraries:
        raise ValueError(f"Archive lacks matching Kafka core and clients jars for {version}")
    return {"version": version, "commit_id": properties.get("commitId"),
            "scala_version": core_names[0].split("_", 1)[1].split("-", 1)[0], "libraries": libraries}


def snapshot_archive(source, generation, output_dir):
    source = Path(source).resolve()
    digest = file_sha256(source)
    destination = Path(output_dir).resolve() / f"kafka-{generation}-{digest}.tgz"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
        try:
            shutil.copyfile(source, temporary_path)
            if file_sha256(temporary_path) != digest:
                raise ValueError(f"Archive changed while copying: {source}")
            inspect_archive(temporary_path, generation)
            os.replace(temporary_path, destination)
        finally:
            temporary_path.unlink(missing_ok=True)
    if file_sha256(destination) != digest:
        raise ValueError(f"Retained archive checksum mismatch: {destination}")
    result = inspect_archive(destination, generation)
    result.update({"path": str(destination), "input_path": str(source), "sha256": digest})
    return result


def check_wrapper_spec(wrapper_root, archive):
    spec = json.loads((Path(wrapper_root) / "product-spec.json").read_text(encoding="utf-8"))
    version = archive["version"]
    versions = spec["build"]["versions"]
    if versions.get("linkedin-kafka") != version or versions.get("baseScala") != archive["scala_version"]:
        raise ValueError("Wrapper Kafka/Scala build versions do not match the selected archive")
    dependencies = [coordinate.split(":") for coordinate in spec["external"].values()
                    if isinstance(coordinate, str) and coordinate.startswith("com.linkedin.kafka:")]
    if not dependencies or not any(parts[1] == "kafka_" + archive["scala_version"] for parts in dependencies):
        raise ValueError("Wrapper has no matching LinkedIn Kafka core dependency")
    if any(len(parts) < 3 or parts[2] != version for parts in dependencies):
        raise ValueError(f"Every wrapper Kafka dependency must use version {version}")


def check_wrapper_classpath(archive, artifacts):
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("Wrapper classpath has no LinkedIn Kafka artifacts")
    checked = []
    main_modules = set()
    for artifact in artifacts:
        if not isinstance(artifact, dict) or artifact.get("group") != "com.linkedin.kafka" or artifact.get("version") != archive["version"]:
            raise ValueError(f"Wrapper resolved an unexpected Kafka coordinate: {artifact}")
        path = Path(artifact["path"])
        digest = file_sha256(path)
        classifier = artifact.get("classifier")
        suffix = "" if classifier is None else f"-{classifier}"
        expected_name = f"{artifact['name']}-{archive['version']}{suffix}.jar"
        if path.name != expected_name:
            raise ValueError(f"Wrapper jar filename does not match its coordinate: {path}")
        if classifier is None:
            if archive["libraries"].get(expected_name) != digest:
                raise ValueError(f"Wrapper jar does not match the selected archive: {path}")
            main_modules.add(artifact["name"])
        elif classifier != "test":
            raise ValueError(f"Unexpected Kafka runtime classifier: {classifier}")
        checked.append(dict(artifact, sha256=digest))
    if not {"kafka-clients", "kafka_" + archive["scala_version"]}.issubset(main_modules):
        raise ValueError("Wrapper classpath lacks Kafka core or clients")
    return {"passed": True, "archive_sha256": archive["sha256"], "version": archive["version"],
            "main_modules": sorted(main_modules), "artifacts": checked}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    snapshot = commands.add_parser("snapshot", help="Validate and retain an immutable archive copy")
    snapshot.add_argument("--archive", required=True, type=Path)
    snapshot.add_argument("--generation", required=True, choices=("3.0", "3.9"))
    snapshot.add_argument("--output-dir", required=True, type=Path)
    snapshot.add_argument("--output-json", required=True, type=Path)
    wrapper = commands.add_parser("wrapper", help="Check wrapper version declarations and resolved Kafka jars")
    wrapper.add_argument("--archive-json", required=True, type=Path)
    wrapper.add_argument("--wrapper-root", required=True, type=Path)
    wrapper.add_argument("--classpath-json", type=Path)
    wrapper.add_argument("--output-json", type=Path)
    wrapper.add_argument("--compare-report", type=Path,
                         help="Require the resolved files to match a previous wrapper report")
    args = parser.parse_args()
    try:
        if args.command == "snapshot":
            result = snapshot_archive(args.archive, args.generation, args.output_dir)
        else:
            archive = json.loads(args.archive_json.read_text(encoding="utf-8"))
            if file_sha256(archive["path"]) != archive["sha256"]:
                raise ValueError("Retained broker archive changed")
            actual = inspect_archive(archive["path"], "3.9")
            if any(archive.get(key) != value for key, value in actual.items()):
                raise ValueError("Broker archive metadata does not match its contents")
            check_wrapper_spec(args.wrapper_root, archive)
            result = {"passed": True, "version": archive["version"]}
            if args.classpath_json:
                result = check_wrapper_classpath(archive, json.loads(args.classpath_json.read_text(encoding="utf-8")))
            if args.compare_report:
                previous = json.loads(args.compare_report.read_text(encoding="utf-8"))
                if not args.classpath_json or previous != result:
                    raise ValueError("Wrapper dependencies changed during testing")
        rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output_json:
            args.output_json.write_text(rendered, encoding="utf-8")
        print(rendered, end="")
        return 0
    except (OSError, ValueError, KeyError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"Artifact validation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
