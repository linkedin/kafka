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

"""Build and stage local Scala 2.12 artifacts. This does not publish a release."""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ORGANISATION = "com.linkedin.kafka"
MAVEN_NAMESPACE = "http://ant.apache.org/ivy/maven"
# Build output, Gradle project, published module, test classifier required by the wrapper.
MODULES = (
    ("clients", "clients", "kafka-clients", True),
    ("server", "server", "kafka-server", False),
    ("server-common", "server-common", "kafka-server-common", True),
    ("storage", "storage", "kafka-storage", False),
    ("storage/api", "storage:storage-api", "kafka-storage-api", False),
    ("metadata", "metadata", "kafka-metadata", False),
    ("raft", "raft", "kafka-raft", False),
    ("group-coordinator", "group-coordinator", "kafka-group-coordinator", False),
    ("group-coordinator/group-coordinator-api", "group-coordinator:group-coordinator-api",
     "kafka-group-coordinator-api", False),
    ("transaction-coordinator", "transaction-coordinator", "kafka-transaction-coordinator", False),
    ("core", "core", "kafka_2.12", True),
)


def java_17(environment):
    executable = str(Path(environment["JAVA_HOME"]) / "bin/java") if environment.get("JAVA_HOME") else "java"
    result = subprocess.run([executable, "-version"], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, check=True, timeout=30)
    if not re.search(r'version "17(?:[.\-"])', result.stdout):
        raise ValueError("Set JAVA_HOME to a Java 17 installation before staging bridge artifacts")


def write_ivy(path, module, version, test_classifier):
    ET.register_namespace("m", MAVEN_NAMESPACE)
    root = ET.Element("ivy-module", version="2.0")
    ET.SubElement(root, "info", organisation=ORGANISATION, module=module, revision=version)
    configurations = ET.SubElement(root, "configurations")
    ET.SubElement(configurations, "conf", name="default", visibility="public")
    publications = ET.SubElement(root, "publications")
    ET.SubElement(publications, "artifact", name=module, type="jar", ext="jar", conf="default")
    if test_classifier:
        ET.SubElement(publications, "artifact", name=module, type="jar", ext="jar", conf="default",
                      attrib={f"{{{MAVEN_NAMESPACE}}}classifier": "test"})
    if module == "kafka_2.12":
        dependencies = ET.SubElement(root, "dependencies")
        for name, revision in (("scala-collection-compat_2.12", "2.10.0"),
                               ("scala-java8-compat_2.12", "1.0.2")):
            ET.SubElement(dependencies, "dependency", org="org.scala-lang.modules", name=name,
                          rev=revision, conf="default->default")
        for _, _, dependency, _ in MODULES:
            if dependency != module:
                ET.SubElement(dependencies, "dependency", org=ORGANISATION, name=dependency,
                              rev=version, conf="default->default")
    ET.indent(root)
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


def stage(root, repository, version, environment):
    java_17(environment)
    if not re.fullmatch(r"3\.9\.\d+(?:\.\d+)?(?:-SNAPSHOT)?", version):
        raise ValueError(f"Invalid local bridge version: {version!r}")
    tasks = []
    for _, project, module, tests in MODULES:
        tasks.append(f"{project}:{'shadowJar' if module == 'kafka-clients' else 'jar'}")
        if tests:
            tasks.append(f"{project}:testJar")
    subprocess.run([str(root / "gradlew"), "--no-daemon", "--max-workers=4", "-PscalaVersion=2.12",
                    f"-Pversion={version}", *tasks], cwd=root, env=environment, check=True, timeout=3600)
    artifact_root = repository / Path(*ORGANISATION.split("."))
    installed = []
    for output, _, module, tests in MODULES:
        destination = artifact_root / module / version
        destination.mkdir(parents=True, exist_ok=True)
        for classifier in (("", "-test") if tests else ("",)):
            name = f"{module}-{version}{classifier}.jar"
            shutil.copyfile(root / output / "build/libs" / name, destination / name)
            installed.append(destination / name)
        ivy = destination / f"{module}-{version}.ivy"
        write_ivy(ivy, module, version, tests)
        installed.append(ivy)
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True)
    status = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True)
    manifest = {
        "version": version, "scala_binary_version": "2.12",
        "source_commit": revision.stdout.strip() if revision.returncode == 0 else "unknown",
        "source_dirty": status.returncode != 0 or bool(status.stdout.strip()),
        # Do not scan the repository: unrelated artifacts from older runs must stay out.
        "files": [{"path": str(path.relative_to(artifact_root)),
                   "sha256": hashlib.sha256(path.read_bytes()).hexdigest()} for path in sorted(installed)],
    }
    path = artifact_root / "bridge-artifact-manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Staged local bridge artifacts under {artifact_root}")
    print(f"Wrote artifact checksums to {path}")
    return manifest


def main():
    root = Path(__file__).resolve().parents[2]
    environment = dict(os.environ)
    repository = Path(environment.get("LI_IVY_REPO", str(Path.home() / "local-repo"))).expanduser()
    version = environment.get("LI_BRIDGE_VERSION", "3.9.2")
    try:
        stage(root, repository, version, environment)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Local artifact staging failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
