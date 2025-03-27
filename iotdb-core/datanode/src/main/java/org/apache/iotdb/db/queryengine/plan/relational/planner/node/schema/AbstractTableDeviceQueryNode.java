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
   * condition on some tag column.
   */
  protected final List<List<SchemaFilter>> tagDeterminedPredicateList;

  /** filters/conditions involving non-tag columns and concat by OR to tag column filters */
  protected final Expression tagFuzzyPredicate;

  protected AbstractTableDeviceQueryNode(
      final PlanNodeId planNodeId,
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> tagDeterminedPredicateList,
      final Expression tagFuzzyPredicate,
      final List<ColumnHeader> columnHeaderList,
      final TDataNodeLocation senderLocation) {
    super(planNodeId, database, tableName, columnHeaderList, senderLocation);
    this.tagDeterminedPredicateList = tagDeterminedPredicateList;
    this.tagFuzzyPredicate = tagFuzzyPredicate;
  }

  public List<List<SchemaFilter>> getTagDeterminedFilterList() {
    return tagDeterminedPredicateList;
  }

  public Expression getTagFuzzyPredicate() {
    return tagFuzzyPredicate;
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    getType().serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);

    ReadWriteIOUtils.write(tagDeterminedPredicateList.size(), byteBuffer);
    for (final List<SchemaFilter> filterList : tagDeterminedPredicateList) {
      ReadWriteIOUtils.write(filterList.size(), byteBuffer);
      for (final SchemaFilter filter : filterList) {
        SchemaFilter.serialize(filter, byteBuffer);
      }
    }

    ReadWriteIOUtils.write(tagFuzzyPredicate == null ? (byte) 0 : (byte) 1, byteBuffer);
    if (tagFuzzyPredicate != null) {
      Expression.serialize(tagFuzzyPredicate, byteBuffer);
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

    ReadWriteIOUtils.write(tagDeterminedPredicateList.size(), stream);
    for (final List<SchemaFilter> filterList : tagDeterminedPredicateList) {
      ReadWriteIOUtils.write(filterList.size(), stream);
      for (final SchemaFilter filter : filterList) {
        SchemaFilter.serialize(filter, stream);
      }
    }

    ReadWriteIOUtils.write(tagFuzzyPredicate == null ? (byte) 0 : (byte) 1, stream);
    if (tagFuzzyPredicate != null) {
      Expression.serialize(tagFuzzyPredicate, stream);
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
    final List<List<SchemaFilter>> tagDeterminedFilterList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int singleSize = ReadWriteIOUtils.readInt(buffer);
      tagDeterminedFilterList.add(new ArrayList<>(singleSize));
      for (int k = 0; k < singleSize; k++) {
        tagDeterminedFilterList.get(i).add(SchemaFilter.deserialize(buffer));
      }
    }

    Expression tagFuzzyFilter = null;
    if (buffer.get() == 1) {
      tagFuzzyFilter = Expression.deserialize(buffer);
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
    final boolean needAligned = isScan ? ReadWriteIOUtils.readBool(buffer) : false;

    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return isScan
        ? new TableDeviceQueryScanNode(
            planNodeId,
            database,
            tableName,
            tagDeterminedFilterList,
            tagFuzzyFilter,
            columnHeaderList,
            senderLocation,
            limit,
            needAligned)
        : new TableDeviceQueryCountNode(
            planNodeId,
            database,
            tableName,
            tagDeterminedFilterList,
            tagFuzzyFilter,
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
        && Objects.equals(tagDeterminedPredicateList, that.tagDeterminedPredicateList)
        && Objects.equals(tagFuzzyPredicate, that.tagFuzzyPredicate)
        && Objects.equals(columnHeaderList, that.columnHeaderList)
        && Objects.equals(schemaRegionReplicaSet, that.schemaRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        database,
        tableName,
        tagDeterminedPredicateList,
        tagFuzzyPredicate,
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
        + ", tagDeterminedFilterList="
        + tagDeterminedPredicateList
        + ", tagFuzzyFilter="
        + tagFuzzyPredicate
        + ", columnHeaderList="
        + columnHeaderList
        + ", schemaRegionReplicaSet="
        + schemaRegionReplicaSet
        + '}';
  }
}
