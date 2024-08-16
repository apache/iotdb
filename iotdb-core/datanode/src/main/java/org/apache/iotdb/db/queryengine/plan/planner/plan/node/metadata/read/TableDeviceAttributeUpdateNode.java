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
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TableDeviceAttributeUpdateNode extends AbstractTableDeviceTraverseNode {

  private final List<UpdateAssignment> assignments;

  protected TableDeviceAttributeUpdateNode(
      final PlanNodeId planNodeId,
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final Expression idFuzzyPredicate,
      final List<ColumnHeader> columnHeaderList,
      final TRegionReplicaSet schemaRegionReplicaSet,
      final List<UpdateAssignment> assignments) {
    super(
        planNodeId,
        database,
        tableName,
        idDeterminedPredicateList,
        idFuzzyPredicate,
        columnHeaderList,
        schemaRegionReplicaSet);
    this.assignments = assignments;
  }

  public List<UpdateAssignment> getAssignments() {
    return assignments;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    super.serializeAttributes(byteBuffer);
    ReadWriteIOUtils.write(assignments.size(), byteBuffer);
    for (final UpdateAssignment assignment : assignments) {
      assignment.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    super.serializeAttributes(stream);
    ReadWriteIOUtils.write(assignments.size(), stream);
    for (final UpdateAssignment assignment : assignments) {
      assignment.serialize(stream);
    }
  }

  @Override
  public PlanNode clone() {
    return new TableDeviceAttributeUpdateNode(
        getPlanNodeId(),
        database,
        tableName,
        idDeterminedPredicateList,
        idFuzzyPredicate,
        columnHeaderList,
        schemaRegionReplicaSet,
        assignments);
  }

  @Override
  public String toString() {
    return "TableDeviceAttributeUpdateNode{assignments="
        + assignments
        + ", "
        + toStringMessage()
        + "}";
  }
}
