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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.read;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableDeviceQueryNode extends TableDeviceSourceNode {

  /**
   * The outer list represents the OR relation between different expression lists.
   *
   * <p>The inner list represents the AND between different expression.
   *
   * <p>Each inner list represents a device pattern and each expression of it represents one
   * condition on some id column.
   */
  private final List<List<SchemaFilter>> idDeterminedPredicateList;

  /** filters/conditions involving non-id columns and concat by OR to id column filters */
  private final Expression idFuzzyPredicate;

  public TableDeviceQueryNode(
      PlanNodeId planNodeId,
      String database,
      String tableName,
      List<List<SchemaFilter>> idDeterminedPredicateList,
      Expression idFuzzyPredicate,
      List<ColumnHeader> columnHeaderList,
      TRegionReplicaSet schemaRegionReplicaSet) {
    super(planNodeId, database, tableName, columnHeaderList, schemaRegionReplicaSet);
    this.idDeterminedPredicateList = idDeterminedPredicateList;
    this.idFuzzyPredicate = idFuzzyPredicate;
  }

  public List<List<SchemaFilter>> getIdDeterminedPredicateList() {
    return idDeterminedPredicateList;
  }

  public Expression getIdFuzzyPredicate() {
    return idFuzzyPredicate;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableDeviceQuery(this, context);
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_QUERY;
  }

  @Override
  public PlanNode clone() {
    return new TableDeviceQueryNode(
        getPlanNodeId(),
        database,
        tableName,
        idDeterminedPredicateList,
        idFuzzyPredicate,
        columnHeaderList,
        schemaRegionReplicaSet);
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_DEVICE_QUERY.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);

    ReadWriteIOUtils.write(idDeterminedPredicateList.size(), byteBuffer);
    for (List<SchemaFilter> filterList : idDeterminedPredicateList) {
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
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_DEVICE_QUERY.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);

    ReadWriteIOUtils.write(idDeterminedPredicateList.size(), stream);
    for (List<SchemaFilter> filterList : idDeterminedPredicateList) {
      ReadWriteIOUtils.write(filterList.size(), stream);
      for (SchemaFilter filter : filterList) {
        SchemaFilter.serialize(filter, stream);
      }
    }

    ReadWriteIOUtils.write(idFuzzyPredicate == null ? (byte) 0 : (byte) 1, stream);
    if (idFuzzyPredicate != null) {
      Expression.serialize(idFuzzyPredicate, stream);
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), stream);
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(stream);
    }
  }

  public static PlanNode deserialize(ByteBuffer buffer) {
    String database = ReadWriteIOUtils.readString(buffer);
    String tableName = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    List<List<SchemaFilter>> idDeterminedFilterList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int singleSize = ReadWriteIOUtils.readInt(buffer);
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
    List<ColumnHeader> columnHeaderList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnHeaderList.add(ColumnHeader.deserialize(buffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceQueryNode(
        planNodeId,
        database,
        tableName,
        idDeterminedFilterList,
        idFuzzyFilter,
        columnHeaderList,
        null);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final TableDeviceQueryNode that = (TableDeviceQueryNode) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(idDeterminedPredicateList, that.idDeterminedPredicateList)
        && Objects.equals(idFuzzyPredicate, that.idFuzzyPredicate)
        && Objects.equals(columnHeaderList, that.columnHeaderList)
        && Objects.equals(schemaRegionReplicaSet, that.schemaRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        database,
        tableName,
        idDeterminedPredicateList,
        idFuzzyPredicate,
        columnHeaderList,
        schemaRegionReplicaSet);
  }

  @Override
  public String toString() {
    return "TableDeviceQueryNode{"
        + "database='"
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
        + '}';
  }
}
