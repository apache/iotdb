<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Node Commons Guidelines

These instructions apply to `iotdb-core/node-commons/` and supplement the
[repository instructions](../../AGENTS.md).

## Responsibilities and code placement

- This module provides models, contracts, and infrastructure shared across nodes, under
  `org.apache.iotdb.commons`: paths, schema, partitions, authentication/authorization,
  configuration, RPC clients, and common service abstractions.
- Shared AST/PlanNode definitions and serialization contracts also live here. Changes must
  account for all consuming nodes, including their serialization/deserialization counterparts.
  Keep concrete node startup, storage operations, and execution orchestration in the node module.
- Reusable execution operators, column transformations, and query computation generally belong
  in [calc-commons](../calc-commons/AGENTS.md). Existing grouping/hash and UDF primitives here mean
  that the current boundary has overlap; inspect consumers and dependencies before deciding
  placement. Do not move code merely because it is query-related or called a common utility.

## Dependency boundary

- The dependency direction is **`calc-commons` -> `node-commons`**. This module must not depend
  on `calc-commons` or concrete DataNode/ConfigNode implementations through Maven or Java imports.
  Shared APIs, protocol types, and lower-level libraries remain appropriate dependencies.
- When common code needs behavior supplied by a node, define the smallest necessary interface
  here and implement it in the consuming module, as with `IPlanNodeDeserializerProvider`.
  Do not pull a concrete node implementation into this module to satisfy one caller.
- Check all consumers before changing shared types or interfaces. Keep existing cross-node
  contracts intact unless changing those contracts is explicitly part of the task.
