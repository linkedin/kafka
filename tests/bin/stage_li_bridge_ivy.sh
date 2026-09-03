#!/usr/bin/env bash
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

# Stages the bridge artifacts in the Ivy layout used by the LinkedIn wrapper's
# local repository. This is for local verification only; it does not replace
# publishing signed artifacts and regenerating the official dependency spec.

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
IVY_REPO=${LI_IVY_REPO:-$HOME/local-repo}
VERSION=${LI_BRIDGE_VERSION:-3.9.2}
ORGANISATION=com.linkedin.kafka
ORGANISATION_PATH=${ORGANISATION//./\/}

cd "$ROOT_DIR"
java_command=${JAVA_HOME:+$JAVA_HOME/bin/}java
java_version=$("$java_command" -version 2>&1 | awk -F '[".]' '/version/ {print $2; exit}')
if [[ "$java_version" != "17" ]]; then
  echo "Set JAVA_HOME to a Java 17 installation before staging bridge artifacts" >&2
  exit 1
fi
./gradlew --no-daemon --max-workers=4 -PscalaVersion=2.12 \
  clients:shadowJar clients:testJar \
  core:jar core:testJar \
  server:jar server-common:jar server-common:testJar \
  storage:jar storage:storage-api:jar \
  metadata:jar raft:jar \
  group-coordinator:jar group-coordinator:group-coordinator-api:jar \
  transaction-coordinator:jar

install_module() {
  module=$1
  main_jar=$2
  test_jar=${3:-}
  module_dir="$IVY_REPO/$ORGANISATION_PATH/$module/$VERSION"
  mkdir -p "$module_dir"
  cp "$main_jar" "$module_dir/$module-$VERSION.jar"

  test_publication=""
  if [[ -n "$test_jar" ]]; then
    cp "$test_jar" "$module_dir/$module-$VERSION-test.jar"
    test_publication="<artifact name=\"$module\" type=\"jar\" ext=\"jar\" conf=\"default\" m:classifier=\"test\"/>"
  fi

  cat >"$module_dir/$module-$VERSION.ivy" <<EOF
<ivy-module version="2.0" xmlns:m="http://ant.apache.org/ivy/maven">
  <info organisation="$ORGANISATION" module="$module" revision="$VERSION"/>
  <configurations><conf name="default" visibility="public"/></configurations>
  <publications>
    <artifact name="$module" type="jar" ext="jar" conf="default"/>
    $test_publication
  </publications>
</ivy-module>
EOF
}

install_module kafka-clients \
  "clients/build/libs/kafka-clients-$VERSION.jar" \
  "clients/build/libs/kafka-clients-$VERSION-test.jar"
install_module kafka-server "server/build/libs/kafka-server-$VERSION.jar"
install_module kafka-server-common \
  "server-common/build/libs/kafka-server-common-$VERSION.jar" \
  "server-common/build/libs/kafka-server-common-$VERSION-test.jar"
install_module kafka-storage "storage/build/libs/kafka-storage-$VERSION.jar"
install_module kafka-storage-api "storage/api/build/libs/kafka-storage-api-$VERSION.jar"
install_module kafka-metadata "metadata/build/libs/kafka-metadata-$VERSION.jar"
install_module kafka-raft "raft/build/libs/kafka-raft-$VERSION.jar"
install_module kafka-group-coordinator \
  "group-coordinator/build/libs/kafka-group-coordinator-$VERSION.jar"
install_module kafka-group-coordinator-api \
  "group-coordinator/group-coordinator-api/build/libs/kafka-group-coordinator-api-$VERSION.jar"
install_module kafka-transaction-coordinator \
  "transaction-coordinator/build/libs/kafka-transaction-coordinator-$VERSION.jar"

core_module=kafka_2.12
core_dir="$IVY_REPO/$ORGANISATION_PATH/$core_module/$VERSION"
mkdir -p "$core_dir"
cp "core/build/libs/$core_module-$VERSION.jar" "$core_dir/$core_module-$VERSION.jar"
cp "core/build/libs/$core_module-$VERSION-test.jar" "$core_dir/$core_module-$VERSION-test.jar"
cat >"$core_dir/$core_module-$VERSION.ivy" <<EOF
<ivy-module version="2.0" xmlns:m="http://ant.apache.org/ivy/maven">
  <info organisation="$ORGANISATION" module="$core_module" revision="$VERSION"/>
  <configurations><conf name="default" visibility="public"/></configurations>
  <publications>
    <artifact name="$core_module" type="jar" ext="jar" conf="default"/>
    <artifact name="$core_module" type="jar" ext="jar" conf="default" m:classifier="test"/>
  </publications>
  <dependencies>
    <dependency org="org.scala-lang.modules" name="scala-collection-compat_2.12"
                rev="2.10.0" conf="default-&gt;default"/>
    <dependency org="org.scala-lang.modules" name="scala-java8-compat_2.12" rev="1.0.2" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-clients" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-server" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-server-common" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-storage" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-storage-api" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-metadata" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-raft" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-group-coordinator" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-group-coordinator-api" rev="$VERSION" conf="default-&gt;default"/>
    <dependency org="$ORGANISATION" name="kafka-transaction-coordinator" rev="$VERSION" conf="default-&gt;default"/>
  </dependencies>
</ivy-module>
EOF

manifest="$IVY_REPO/$ORGANISATION_PATH/bridge-artifact-manifest.json"
source_commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)
if [[ -n $(git status --porcelain 2>/dev/null) ]]; then
  source_dirty=true
else
  source_dirty=false
fi
python3 - "$IVY_REPO/$ORGANISATION_PATH" "$manifest" "$VERSION" "$source_commit" "$source_dirty" <<'PY'
import hashlib
import json
import pathlib
import sys

artifact_root = pathlib.Path(sys.argv[1])
manifest_path = pathlib.Path(sys.argv[2])
files = []
for path in sorted(artifact_root.rglob("*")):
    if path.is_file() and path != manifest_path:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        files.append({"path": str(path.relative_to(artifact_root)), "sha256": digest})
manifest = {
    "version": sys.argv[3],
    "scala_binary_version": "2.12",
    "source_commit": sys.argv[4],
    "source_dirty": sys.argv[5].lower() == "true",
    "files": files,
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

echo "Staged LinkedIn bridge artifacts under $IVY_REPO/$ORGANISATION_PATH"
echo "Wrote artifact checksums to $manifest"
