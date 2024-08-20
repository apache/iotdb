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
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class TableDeviceAttributeUpdateNode extends WritePlanNode implements ISchemaRegionPlan {

  protected String database;

  protected String tableName;

  protected List<ColumnHeader> columnHeaderList;

  protected TRegionReplicaSet schemaRegionReplicaSet;
  protected final List<List<SchemaFilter>> idDeterminedPredicateList;

  /** filters/conditions involving non-id columns and concat by OR to id column filters */
  protected final Expression idFuzzyPredicate;

  private final List<UpdateAssignment> assignments;
  private final SessionInfo sessionInfo;

  public static final TableDeviceAttributeUpdateNode MOCK_INSTANCE =
      new TableDeviceAttributeUpdateNode(
          new PlanNodeId(""), null, null, null, null, null, null, null, null);

  @SuppressWarnings("squid:S107")
  public TableDeviceAttributeUpdateNode(
      final PlanNodeId planNodeId,
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final Expression idFuzzyPredicate,
      final List<ColumnHeader> columnHeaderList,
      final TRegionReplicaSet schemaRegionReplicaSet,
      final List<UpdateAssignment> assignments,
      final SessionInfo sessionInfo) {
    super(planNodeId);
    this.database = database;
    this.tableName = tableName;
    this.columnHeaderList = columnHeaderList;
    this.schemaRegionReplicaSet = schemaRegionReplicaSet;
    this.idDeterminedPredicateList = idDeterminedPredicateList;
    this.idFuzzyPredicate = idFuzzyPredicate;
    this.assignments = assignments;
    this.sessionInfo = sessionInfo;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<ColumnHeader> getColumnHeaderList() {
    return columnHeaderList;
  }

  public List<List<SchemaFilter>> getIdDeterminedFilterList() {
    return idDeterminedPredicateList;
  }

  public Expression getIdFuzzyPredicate() {
    return idFuzzyPredicate;
  }

  public List<UpdateAssignment> getAssignments() {
    return assignments;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, final C context) {
    return visitor.visitTableDeviceAttributeUpdate(this, context);
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

    ReadWriteIOUtils.write(Objects.nonNull(sessionInfo), byteBuffer);
    if (Objects.nonNull(sessionInfo)) {
      sessionInfo.serialize(byteBuffer);
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

    ReadWriteIOUtils.write(Objects.nonNull(sessionInfo), stream);
    if (Objects.nonNull(sessionInfo)) {
      sessionInfo.serialize(stream);
    }
  }

  public static PlanNode deserialize(final ByteBuffer buffer) {
    final String database = ReadWriteIOUtils.readString(buffer);
    final String tableName = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    final List<List<SchemaFilter>> idDeterminedFilterList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int singleSize = ReadWriteIOUtils.readInt(buffer);
      idDeterminedFilterList.add(new ArrayList<>(singleSize));
      for (int k = 0; k < singleSize; k++) {
        idDeterminedFilterList.get(i).add(SchemaFilter.deserialize(buffer));
      }
    }

    Expression idFuzzyFilter = null;
    if (buffer.get() == 1) {
      idFuzzyFilter = Expression.deserialize(buffer);
    }

    size = ReadWriteIOUtils.readInt(buffer);
    final List<ColumnHeader> columnHeaderList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnHeaderList.add(ColumnHeader.deserialize(buffer));
    }

    size = ReadWriteIOUtils.readInt(buffer);
    final List<UpdateAssignment> assignments = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      assignments.add(UpdateAssignment.deserialize(buffer));
    }

    SessionInfo sessionInfo = null;
    if (ReadWriteIOUtils.readBool(buffer)) {
      sessionInfo = SessionInfo.deserializeFrom(buffer);
    }

    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);

    return new TableDeviceAttributeUpdateNode(
        planNodeId,
        database,
        tableName,
        idDeterminedFilterList,
        idFuzzyFilter,
        columnHeaderList,
        null,
        assignments,
        sessionInfo);
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(final PlanNode child) {
    // Do nothing
  }

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
        assignments,
        sessionInfo);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnHeaderList.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Override
  public List<Symbol> getOutputSymbols() {
    return columnHeaderList.stream()
        .map(columnHeader -> new Symbol(columnHeader.getColumnName()))
        .collect(Collectors.toList());
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
    return schemaRegionReplicaSet;
  }

  @Override
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    return new HashSet<>(
            analysis
                .getSchemaPartitionInfo()
                .getSchemaPartitionMap()
                .get(PATH_ROOT + PATH_SEPARATOR + database)
                .values())
        .stream()
            .map(
                replicaSet ->
                    new TableDeviceAttributeUpdateNode(
                        getPlanNodeId(),
                        database,
                        tableName,
                        idDeterminedPredicateList,
                        idFuzzyPredicate,
                        columnHeaderList,
                        replicaSet,
                        assignments,
                        sessionInfo))
            .collect(Collectors.toList());
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.UPDATE_TABLE_DEVICE_ATTRIBUTE;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitUpdateTableDeviceAttribute(this, context);
  }
}
