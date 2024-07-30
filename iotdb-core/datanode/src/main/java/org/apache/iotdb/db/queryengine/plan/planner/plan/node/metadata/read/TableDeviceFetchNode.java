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
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TableDeviceFetchNode extends TableDeviceSourceNode {

  private final List<Object[]> deviceIdList;

  public TableDeviceFetchNode(
      PlanNodeId id,
      String database,
      String tableName,
      List<Object[]> deviceIdList,
      List<ColumnHeader> columnHeaderList,
      TRegionReplicaSet regionReplicaSet) {
    super(id, database, tableName, columnHeaderList, regionReplicaSet);
    this.deviceIdList = deviceIdList;
  }

  public void addDeviceId(Object[] deviceId) {
    deviceIdList.add(deviceId);
  }

  public List<Object[]> getDeviceIdList() {
    return deviceIdList;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_DEVICE_FETCH;
  }

  @Override
  public PlanNode clone() {
    return new TableDeviceFetchNode(
        getPlanNodeId(),
        database,
        tableName,
        deviceIdList,
        columnHeaderList,
        schemaRegionReplicaSet);
  }

  @Override
  public List<String> getOutputColumnNames() {
    return columnHeaderList.stream().map(ColumnHeader::getColumnName).collect(Collectors.toList());
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.TABLE_DEVICE_FETCH.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);

    ReadWriteIOUtils.write(deviceIdList.size(), byteBuffer);
    for (Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, byteBuffer);
      for (Object idValue : deviceId) {
        ReadWriteIOUtils.writeObject(idValue, byteBuffer);
      }
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), byteBuffer);
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(byteBuffer);
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.TABLE_DEVICE_FETCH.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);

    ReadWriteIOUtils.write(deviceIdList.size(), stream);
    for (Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, stream);
      for (Object idValue : deviceId) {
        ReadWriteIOUtils.writeObject(idValue, stream);
      }
    }

    ReadWriteIOUtils.write(columnHeaderList.size(), stream);
    for (ColumnHeader columnHeader : columnHeaderList) {
      columnHeader.serialize(stream);
    }
  }

  public static TableDeviceFetchNode deserialize(ByteBuffer buffer) {
    String database = ReadWriteIOUtils.readString(buffer);
    String tableName = ReadWriteIOUtils.readString(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    List<Object[]> deviceIdList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      int length = ReadWriteIOUtils.readInt(buffer);
      Object[] nodes = new String[length];
      for (int j = 0; j < length; j++) {
        nodes[j] = ReadWriteIOUtils.readObject(buffer);
      }
      deviceIdList.add(nodes);
    }

    size = ReadWriteIOUtils.readInt(buffer);
    List<ColumnHeader> columnHeaderList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnHeaderList.add(ColumnHeader.deserialize(buffer));
    }

    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new TableDeviceFetchNode(
        planNodeId, database, tableName, deviceIdList, columnHeaderList, null);
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitTableDeviceFetch(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableDeviceFetchNode)) return false;
    if (!super.equals(o)) return false;
    TableDeviceFetchNode that = (TableDeviceFetchNode) o;
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
