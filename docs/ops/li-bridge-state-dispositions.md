<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# State and client qualification decisions

Production preflight requires owner-reviewed decisions, not a nonempty status
string. The contract-2 JSON shape is unchanged, but its disposition values are now
enforced. Old free-form values such as `pending`, `blocked`, `unknown` or a bare
`retained` client floor do not grant admission. Unknown decision keys also fail.
Each `owner`, `evidence` and `disposition` must be a nonblank string.

| Decision | Accepted values | What the owner must establish |
|---|---|---|
| Each listed ZooKeeper path | `retained`, `unused` | Retained state is compatible with the planned phases and rollback. Unused state has been checked, not inferred from an empty parent alone. |
| `remote-storage` | `unused` | No unsupported remote data, metadata or objects remain. Otherwise use a separate migration plan. |
| `plugin-state` | `unused`, `qualified` | Retained plugin/internal state has passed the planned binary transitions and supported recovery paths. |
| `client-floor` | `qualified-unchanged` | The exact client/tool artifacts and configuration pass the planned phases and approved SLOs without changing that profile during the roll. |
| `artifact-admission` | `bridge-artifacts-only` | Deployment automation cannot restart an unapproved binary or image. |

The checker validates these declarations and their input identity/freshness. It
does not authenticate an approver or prove the linked tests. Release owners must
review the evidence and protect the approved input files. Never fill the template
just to obtain a green report. Regenerate and review old decisions that do not
satisfy the stricter reader.

## Client floor and the inherited bootstrap limitation

F28 is an existing LI 3.0 client-bootstrap limitation, not a new broker protocol
change. Its default expiry path can choose a disconnected bootstrap node while
another node is ready, then wait through the metadata deadline. The deterministic
unchanged-jar probe demonstrates this even though a complete migration run passed.

The client-floor evidence must identify the deployed client/tool versions,
configuration and owners. It must cover cold startup with unavailable bootstrap
entries, steady traffic, failover, and the application's handling of startup
errors against approved SLOs. Do not use one successful migration run to dismiss
the known failure. If the unchanged profile cannot meet those requirements, stop
before the canary.

The existing `li.client.cluster.metadata.expire.time.ms=0` option passed a separate
experiment; it is not required or silently enabled by this plan. Any client change
needs its own owner decision and qualification **before** the migration baseline.
After that baseline, retain the same client artifacts and settings through every
phase. This review does not choose or approve a deployed client configuration.

## Template

Copy this JSON to protected release evidence and fill it from reviewed observations.
The empty fields deliberately fail validation. Use the actual cluster ID and a
fresh timezone-qualified collection timestamp. The runbook defines the required
per-broker runtime observations when pagination is enabled.

```json
{
  "contract_version": 2,
  "cluster_id": "",
  "collected_at_utc": "",
  "broker_runtimes": {},
  "decisions": {
    "/brokers/ids": {"owner": "", "evidence": "", "disposition": ""},
    "/brokers/corrupted": {"owner": "", "evidence": "", "disposition": ""},
    "/brokers/shutdown": {"owner": "", "evidence": "", "disposition": ""},
    "/brokers/preferred_controllers": {"owner": "", "evidence": "", "disposition": ""},
    "/federatedTopics": {"owner": "", "evidence": "", "disposition": ""},
    "/topic_deletion_flag": {"owner": "", "evidence": "", "disposition": ""},
    "remote-storage": {"owner": "", "evidence": "", "disposition": ""},
    "plugin-state": {"owner": "", "evidence": "", "disposition": ""},
    "client-floor": {"owner": "", "evidence": "", "disposition": ""},
    "artifact-admission": {"owner": "", "evidence": "", "disposition": ""}
  }
}
```

See the [rolling-upgrade runbook](li-bridge-upgrade.md) for collection, live
preflight, phase transitions and the independent artifact/runtime release gates.
