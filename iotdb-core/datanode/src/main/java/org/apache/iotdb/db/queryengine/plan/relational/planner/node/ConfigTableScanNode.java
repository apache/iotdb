/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ConfigTableScanNode extends TableScanNode {

  // We use config table scan node to tell it from the common data query.
  // Not all the parameters are used in config table scan node.
  // Here we simply extend the table scan node for better maintainability.
  public ConfigTableScanNode(
      final PlanNodeId id,
      final QualifiedObjectName qualifiedObjectName,
      final List<Symbol> outputSymbols,
      final Map<Symbol, ColumnSchema> assignments,
      final Map<Symbol, Integer> idAndAttributeIndexMap) {
    super(id, qualifiedObjectName, outputSymbols, assignments, idAndAttributeIndexMap);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    throw new UnsupportedOperationException(
        "Config table scan shall only be executed on local, and shall neither be persisted nor sent to remote.");
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    throw new UnsupportedOperationException(
        "Config table scan shall only be executed on local, and shall neither be persisted nor sent to remote.");
  }
}
