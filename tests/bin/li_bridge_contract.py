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

"""Shared, versioned expectations for bridge preflight, scenarios and evidence.

This describes qualification requirements, not automatic permission to deploy.
Retiring an individual compatibility feature requires an owner-approved contract change.
"""

import hashlib
import json
import os

CONTRACT_VERSION = 2
SCENARIO_REVISION = 5
MODE = "li.protocol.bridge.mode.enable"
TOPIC_CLEANUP = "li.protocol.bridge.topic.deletion.state.cleanup.enable"
CONFIG_METRICS = "li.protocol.bridge.config.metrics.enable"
# The Kafka registry and metric-name set are checked against this table by tests.
# (config suffix, effective metric, retained in the minimum compatibility profile)
FEATURES = (
    ("mode", "ModeEnabled", True),
    ("config.metrics", "ConfigMetricsEnabled", True),
    ("topic.deletion.state.cleanup", "TopicDeletionStateCleanupEnabled", True),
    ("follower.recovery", "FollowerRecoveryEnabled", True),
    ("recommended.leader.election", "RecommendedLeaderElectionEnabled", True),
    ("metadata.exclude.partitions", "ExcludePartitionsEnabled", True),
    ("move.controller", "MoveControllerEnabled", True),
    ("shutdown.safety.override", "ShutdownSafetyOverrideEnabled", True),
    ("preferred.controller", "PreferredControllerEnabled", False),
    ("federated.topics", "FederatedTopicsEnabled", True),
    ("rack.id.mapper", "RackIdMapperEnabled", False),
    ("dynamic.topic.deletion", "DynamicTopicDeletionEnabled", False),
    ("produce.request.instrumentation", "ProduceRequestInstrumentationEnabled", False),
    ("request.metric.buckets", "RequestMetricBucketsEnabled", False),
    ("request.channel.watchdog", "RequestChannelWatchdogEnabled", False),
    ("minimum.log.roll", "MinimumLogRollEnabled", False),
    ("reassignment.cancellation.safety", "ReassignmentCancellationSafetyEnabled", False),
    ("list.offsets.instrumentation", "ListOffsetsInstrumentationEnabled", False),
    ("static.default.quotas", "StaticDefaultQuotasEnabled", False),
    ("replica.request.timeout", "ReplicaRequestTimeoutEnabled", False),
    ("offsets.topic.config", "OffsetsTopicConfigEnabled", False),
    ("leader.transfer.on.isr.shrink", "LeaderTransferEnabled", False),
    ("legacy.request.metrics", "LegacyRequestMetricsEnabled", False),
    ("log.truncation.metrics", "LogTruncationMetricsEnabled", False),
)
BRIDGE_GATES = tuple(f"li.protocol.bridge.{suffix}.enable" for suffix, _, _ in FEATURES)
MIXED_REQUIRED_GATES = tuple(f"li.protocol.bridge.{suffix}.enable"
                             for suffix, _, required in FEATURES if required)
# (IBP, bridge mode, permitted/required generations)
PHASES = {
    "dormant": ("3.0", False, ("3.0",)),
    "legacy-bridge": ("3.0", True, ("3.0",)),
    "mixed": ("3.0", True, ("3.0", "3.9")),
    "all-39-bridge": ("3.0", True, ("3.9",)),
    "native": ("3.0", False, ("3.9",)),
    "ibp-39": ("3.9", False, ("3.9",)),
}
TRANSITIONS = {
    "dormant": {"legacy-bridge"},
    "legacy-bridge": {"dormant", "mixed"},
    "mixed": {"legacy-bridge", "all-39-bridge"},
    "all-39-bridge": {"mixed", "native"},
    "native": {"ibp-39"},
    "ibp-39": set(),
}
SCENARIO_DEFAULTS = {
    "SCALE_TOPIC_COUNT": 10,
    "SCALE_PARTITION_COUNT": 5,
    "RECOVERY_RECORD_COUNT": 200,
    "RECOVERY_RECORD_SIZE": 100000,
    "BRIDGE_COMMAND_TIMEOUT_SECONDS": 180,
    "BRIDGE_SCENARIO_TIMEOUT_SECONDS": 2400,
}


def required_gates(phase, generation, all_gates=False):
    if generation == "3.0":
        # The dormant binary may still have every new flag off. Enable cleanup before
        # entering the common protocol, then keep it enabled through the native bake.
        return (CONFIG_METRICS,) if phase == "dormant" else (CONFIG_METRICS, TOPIC_CLEANUP)
    gates = BRIDGE_GATES if all_gates else MIXED_REQUIRED_GATES
    return tuple(gate for gate in gates if gate != MODE)


def inventory_issues(phase, generations):
    expected = set(PHASES[phase][2])
    return [] if set(generations) == expected else [
        f"{phase} requires broker generations {sorted(expected)}, found {sorted(set(generations), key=str)}"]


def transition_allowed(previous, phase):
    return previous == phase or phase in TRANSITIONS[previous]


def scenario_spec(environment=None):
    environment = os.environ if environment is None else environment
    result = {"contract_version": CONTRACT_VERSION, "scenario_revision": SCENARIO_REVISION,
              "profile": "legacy-async-rollback-old-clients",
              "old_client_interval_ms": 100}
    for name, default in SCENARIO_DEFAULTS.items():
        raw = str(environment.get(name, default))
        if not raw.isdecimal() or not 0 < int(raw) <= 2147483647:
            raise ValueError(f"{name} must be a positive 32-bit integer, found {raw!r}")
        result[name] = int(raw)
    # Store a digest rather than potentially sensitive JVM options. A changed runtime profile
    # must invalidate resume even when source/archive files have not changed.
    runtime = {name: environment.get(name, "") for name in (
        "JAVA_HOME", "LI_BRIDGE_JAVA_30_HOME", "KAFKA_HEAP_OPTS", "KAFKA_OPTS", "KAFKA_JVM_PERFORMANCE_OPTS")}
    result["runtime_profile_sha256"] = hashlib.sha256(json.dumps(runtime, sort_keys=True).encode()).hexdigest()
    return result


if __name__ == "__main__":
    print(json.dumps(scenario_spec(), indent=2, sort_keys=True))
