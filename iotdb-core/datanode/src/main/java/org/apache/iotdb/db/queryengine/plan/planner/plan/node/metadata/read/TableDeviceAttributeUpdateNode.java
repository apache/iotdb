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
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TableDeviceAttributeUpdateNode extends WritePlanNode {

  protected String database;

  protected String tableName;

  protected List<ColumnHeader> columnHeaderList;

  protected TRegionReplicaSet schemaRegionReplicaSet;
  protected final List<List<SchemaFilter>> idDeterminedPredicateList;

  /** filters/conditions involving non-id columns and concat by OR to id column filters */
  protected final Expression idFuzzyPredicate;

  private final List<UpdateAssignment> assignments;

  public TableDeviceAttributeUpdateNode(
      final PlanNodeId planNodeId,
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final Expression idFuzzyPredicate,
      final List<ColumnHeader> columnHeaderList,
      final TRegionReplicaSet schemaRegionReplicaSet,
      final List<UpdateAssignment> assignments) {
    super(planNodeId);
    this.database = database;
    this.tableName = tableName;
    this.columnHeaderList = columnHeaderList;
    this.schemaRegionReplicaSet = schemaRegionReplicaSet;
    this.idDeterminedPredicateList = idDeterminedPredicateList;
    this.idFuzzyPredicate = idFuzzyPredicate;
    this.assignments = assignments;
  }

  public List<UpdateAssignment> getAssignments() {
    return assignments;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);

    ReadWriteIOUtils.write(idDeterminedPredicateList.size(), byteBuffer);
    for (final List<SchemaFilter> filterList : idDeterminedPredicateList) {
      ReadWriteIOUtils.write(filterList.size(), byteBuffer);
      for (final SchemaFilter filter : filterList) {
        SchemaFilter.serialize(filter, byteBuffer);
      }
    }

    ReadWriteIOUtils.write(idFuzzyPredicate == null ? (byte) 0 : (byte) 1, byteBuffer);
    if (idFuzzyPredicate != null) {
      Expression.serialize(idFuzzyPredicate, byteBuffer);
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), byteBuffer);
    for (final ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(byteBuffer);
    }

    ReadWriteIOUtils.write(assignments.size(), byteBuffer);
    for (final UpdateAssignment assignment : assignments) {
      assignment.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    getType().serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);

    ReadWriteIOUtils.write(idDeterminedPredicateList.size(), stream);
    for (final List<SchemaFilter> filterList : idDeterminedPredicateList) {
      ReadWriteIOUtils.write(filterList.size(), stream);
      for (final SchemaFilter filter : filterList) {
        SchemaFilter.serialize(filter, stream);
      }
    }

    ReadWriteIOUtils.write(idFuzzyPredicate == null ? (byte) 0 : (byte) 1, stream);
    if (idFuzzyPredicate != null) {
      Expression.serialize(idFuzzyPredicate, stream);
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), stream);
    for (final ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(stream);
    }

    ReadWriteIOUtils.write(assignments.size(), stream);
    for (final UpdateAssignment assignment : assignments) {
      assignment.serialize(stream);
    }
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_ATTRIBUTE_UPDATE;
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
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  public String toString() {
    return "TableDeviceAttributeUpdateNode{assignments="
        + assignments
        + ", database='"
        + database
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", idDeterminedFilterList="
        + idDeterminedPredicateList
        + ", idFuzzyFilter="
        + idFuzzyPredicate
        + ", columnHeaderList="
        + columnHeaderList
        + ", schemaRegionReplicaSet="
        + schemaRegionReplicaSet
        + "}";
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    return null;
  }
}
