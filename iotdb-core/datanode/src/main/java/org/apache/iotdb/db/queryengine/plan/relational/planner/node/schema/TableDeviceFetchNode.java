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
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.update.DeviceAttributeCacheUpdater;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TableDeviceFetchNode extends TableDeviceSourceNode {

  private final List<Object[]> deviceIdList;

  // transient
  private final List<IDeviceID> partitionKeyList;

  public TableDeviceFetchNode(
      final PlanNodeId id,
      final String database,
      final String tableName,
      final List<Object[]> deviceIdList,
      final List<IDeviceID> partitionKeyList,
      final List<ColumnHeader> columnHeaderList,
      final TDataNodeLocation senderLocation) {
    super(id, database, tableName, columnHeaderList, senderLocation);
    this.deviceIdList = deviceIdList;
    this.partitionKeyList = partitionKeyList;
  }

  public void addDeviceId(final Object[] deviceId) {
    deviceIdList.add(deviceId);
  }

  public List<Object[]> getDeviceIdList() {
    return deviceIdList;
  }

  public List<IDeviceID> getPartitionKeyList() {
    return partitionKeyList;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_FETCH;
  }

  @Override
  public PlanNode clone() {
    throw new UnsupportedOperationException(
        "The TableDeviceFetchNode's clone() method shall not be called.");
  }

  public PlanNode cloneForDistribution() {
    // The id is not used, only for requireNonNull
    return new TableDeviceFetchNode(
        id, database, tableName, new ArrayList<>(), null, columnHeaderList, senderLocation);
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_DEVICE_FETCH.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);

    ReadWriteIOUtils.write(deviceIdList.size(), byteBuffer);
    for (final Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, byteBuffer);
      for (final Object idValue : deviceId) {
        ReadWriteIOUtils.writeObject(idValue, byteBuffer);
      }
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
    PlanNodeType.TABLE_DEVICE_FETCH.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);

    ReadWriteIOUtils.write(deviceIdList.size(), stream);
    for (final Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, stream);
      for (final Object idValue : deviceId) {
        ReadWriteIOUtils.writeObject(idValue, stream);
      }
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

  public static TableDeviceFetchNode deserialize(final ByteBuffer buffer) {
    final String database = ReadWriteIOUtils.readString(buffer);
    final String tableName = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    final List<Object[]> deviceIdList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      final int length = ReadWriteIOUtils.readInt(buffer);
      final Object[] nodes = new String[length];
      for (int j = 0; j < length; j++) {
        nodes[j] = ReadWriteIOUtils.readObject(buffer);
      }
      deviceIdList.add(nodes);
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

    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceFetchNode(
        planNodeId, database, tableName, deviceIdList, null, columnHeaderList, senderLocation);
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableDeviceFetch(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableDeviceFetchNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final TableDeviceFetchNode that = (TableDeviceFetchNode) o;
    return Objects.equals(database, that.database)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(deviceIdList, that.deviceIdList)
        && Objects.equals(columnHeaderList, that.columnHeaderList)
        && Objects.equals(schemaRegionReplicaSet, that.schemaRegionReplicaSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        database,
        tableName,
        deviceIdList,
        columnHeaderList,
        schemaRegionReplicaSet);
  }
}
