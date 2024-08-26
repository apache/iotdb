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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice.COUNT_DEVICE_HEADER_STRING;

public class TableDeviceQueryCountNode extends AbstractTableDeviceQueryNode {

  public TableDeviceQueryCountNode(
      final PlanNodeId planNodeId,
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final Expression idFuzzyPredicate,
      final List<ColumnHeader> columnHeaderList,
      final TRegionReplicaSet schemaRegionReplicaSet) {
    super(
        planNodeId,
        database,
        tableName,
        idDeterminedPredicateList,
        idFuzzyPredicate,
        columnHeaderList,
        schemaRegionReplicaSet);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return Collections.singletonList(COUNT_DEVICE_HEADER_STRING);
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitTableDeviceQueryCount(this, context);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_QUERY_COUNT;
  }

  @Override
  public PlanNode clone() {
    return new TableDeviceQueryCountNode(
        getPlanNodeId(),
        database,
        tableName,
        idDeterminedPredicateList,
        idFuzzyPredicate,
        columnHeaderList,
        schemaRegionReplicaSet);
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    return AbstractTableDeviceQueryNode.deserialize(buffer, false);
  }

  @Override
  public String toString() {
    return "TableDeviceQueryCountNode" + toStringMessage();
  }
}
