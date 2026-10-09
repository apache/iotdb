<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Complete numeric table compression (research)

`ClusterTableSession` adds the complete column-selection + ACluster/KCluster + ALP
pipeline using ordinary IoTDB BLOB storage. The Session module depends on the companion
TsFile `research/cluster-compress` branch's `2.1.0-SNAPSHOT`; install that branch's Java
common and tsfile modules locally before building this module. This BLOB API itself
does not require a modified server. The research branch now also uses that TsFile
version on the server for a separate native aligned/scalar numeric SQL path.
`ClusterAlignedSqlIT` tests that path without calling this BLOB API.

```java
ClusterTableSession storage = new ClusterTableSession(
    session, "root.research.numeric_table", ClusterTableOptions.aCluster());
storage.createSchema();
int blocks = storage.writeTable(firstUnusedBlockId, numericTable);
storage.readAll(table -> { /* consume complete records */ });
storage.readTimeRange(startInclusive, endInclusive, table -> { /* original timestamps */ });
```

Choose `ClusterTableOptions.kCluster()` for KCluster; both use the same selector and ALP.
See `ClusterTable` for the typed INT32/INT64/FLOAT/DOUBLE plus null record API.

Use one dedicated device per table stream, and supply unique increasing block IDs across
writers/restarts. The outer IoTDB timestamp is the block ID, not the original timestamp.
Ordinary SQL sees payload, min_time, max_time and row_count; original numeric fields require
this decoder. Original-time queries prune with min_time/max_time and then filter decoded rows.
Do not apply original-time filters or retention policies directly to the outer block ID.
The caller owns the Session and must close it.

Every independent block preserves complete records and duplicate multiplicities, including
timestamps and exact numeric bits, while permitting physical row reordering.
The writer is single-threaded and refuses further writes after an uncertain insert failure.
Reconcile that block's outcome before restarting; do not blindly retry at a new block ID.

Tests:
- `ClusterTableSessionTest`: RPC payloads, complete-record filtering, invalid paths and failures.
- `ClusterTableSessionIT host port dedicatedDevice write|read`: opt-in local service check.
  Write mode writes 1,200 records for each method, flushes, and verifies full/range queries.
  Read mode performs the same verification without writing, for persistence testing after restart.
  Run only against a dedicated test device. It uses the local test credentials root/root.

This is an opt-in client/block integration, not a new SQL encoding option or a server storage-page
rewrite. Its correctness checks do not establish a performance advantage.
