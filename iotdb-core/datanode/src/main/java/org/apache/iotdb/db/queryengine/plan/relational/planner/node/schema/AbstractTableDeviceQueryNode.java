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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.DeviceAttributeCacheUpdater;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractTableDeviceQueryNode extends TableDeviceSourceNode {

  /**
   * The outer list represents the OR relation between different expression lists.
   *
   * <p>The inner list represents the AND between different expression.
   *
   * <p>Each inner list represents a device pattern and each expression of it represents one
   * condition on some id column.
   */
  protected final List<List<SchemaFilter>> idDeterminedPredicateList;

  /** filters/conditions involving non-id columns and concat by OR to id column filters */
  protected final Expression idFuzzyPredicate;

  protected AbstractTableDeviceQueryNode(
      final PlanNodeId planNodeId,
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedPredicateList,
      final Expression idFuzzyPredicate,
      final List<ColumnHeader> columnHeaderList,
      final TDataNodeLocation senderLocation) {
    super(planNodeId, database, tableName, columnHeaderList, senderLocation);
    this.idDeterminedPredicateList = idDeterminedPredicateList;
    this.idFuzzyPredicate = idFuzzyPredicate;
  }

  public List<List<SchemaFilter>> getIdDeterminedFilterList() {
    return idDeterminedPredicateList;
  }

  public Expression getIdFuzzyPredicate() {
    return idFuzzyPredicate;
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

    if (Objects.nonNull(senderLocation)) {
      ReadWriteIOUtils.write(true, byteBuffer);
      DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(senderLocation, byteBuffer);
    } else {
      ReadWriteIOUtils.write(false, byteBuffer);
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

    if (Objects.nonNull(senderLocation)) {
      ReadWriteIOUtils.write(true, stream);
      DeviceAttributeCacheUpdater.serializeNodeLocation4AttributeUpdate(senderLocation, stream);
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  protected static PlanNode deserialize(final ByteBuffer buffer, final boolean isScan) {
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

    TDataNodeLocation senderLocation = null;
    if (ReadWriteIOUtils.readBool(buffer)) {
      senderLocation =
          DeviceAttributeCacheUpdater.deserializeNodeLocationForAttributeUpdate(buffer);
    }

    final long limit = isScan ? ReadWriteIOUtils.readLong(buffer) : 0;

    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return isScan
        ? new TableDeviceQueryScanNode(
            planNodeId,
            database,
            tableName,
            idDeterminedFilterList,
            idFuzzyFilter,
            columnHeaderList,
            senderLocation,
            limit)
        : new TableDeviceQueryCountNode(
            planNodeId,
            database,
            tableName,
            idDeterminedFilterList,
            idFuzzyFilter,
            columnHeaderList);
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
    final AbstractTableDeviceQueryNode that = (AbstractTableDeviceQueryNode) o;
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

  protected String toStringMessage() {
    return "{database='"
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
