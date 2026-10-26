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

# Calc Commons Guidelines

These instructions apply to `iotdb-core/calc-commons/` and supplement the
[repository instructions](../../AGENTS.md).

## Responsibilities and code placement

- This module provides reusable query computation and execution for server and stream execution
  environments. Place reusable operators, column transformations, aggregation, joins, sorting,
  windows, and their execution/memory utilities here, under `org.apache.iotdb.calc`.
- Shared node models and infrastructure belong in [node-commons](../node-commons/AGENTS.md).
  Keep concrete storage/DataRegion integration, fragment lifecycle, and node scheduler wiring
  in the owning node module; separate reusable computation from that integration.
- Existing SQL constants are also consumed by ConfigNode and CLI. Such usage does not make this
  module a destination for arbitrary shared utilities. Choose by responsibility and actual
  dependencies; do not relocate existing code solely because of its package or class name.

## Dependency boundary

- The dependency direction is **`calc-commons` -> `node-commons`**, never the reverse. Do not
  introduce Maven dependencies or Java imports on concrete DataNode/ConfigNode implementations.
- Obtain host services through common contexts or small provider interfaces. For example,
  `CommonOperatorContext` supplies the execution context, while file-service providers abstract
  temporary/object-file access. Keep their node-specific implementations in the consuming module.
  Do not downcast to DataNode's `OperatorContext` or access a concrete node singleton from shared
  computation code.
- When changing a shared interface, inspect its implementations and callers across consuming
  modules. Preserve use from standalone/test contexts without requiring a running DataNode.
