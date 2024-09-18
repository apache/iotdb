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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegionPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanType;
import org.apache.iotdb.db.schemaengine.schemaregion.SchemaRegionPlanVisitor;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.storageengine.dataregion.memtable.DeviceIDFactory.convertRawDeviceIDs2PartitionKeys;

public class CreateOrUpdateTableDeviceNode extends WritePlanNode implements ISchemaRegionPlan {

  private final String database;

  private final String tableName;

  private final List<Object[]> deviceIdList;

  private final List<String> attributeNameList;

  private final List<Object[]> attributeValueList;

  private TRegionReplicaSet regionReplicaSet;

  private transient List<IDeviceID> partitionKeyList;

  public static final CreateOrUpdateTableDeviceNode MOCK_INSTANCE =
      new CreateOrUpdateTableDeviceNode(new PlanNodeId(""), null, null, null, null, null);

  public CreateOrUpdateTableDeviceNode(
      final PlanNodeId id,
      final String database,
      final String tableName,
      final List<Object[]> deviceIdList,
      final List<String> attributeNameList,
      final List<Object[]> attributeValueList) {
    super(id);
    this.database = database;
    this.tableName = tableName;
    this.deviceIdList = deviceIdList;
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
  }

  // In this constructor, we don't need to truncate tailing nulls for deviceIdList, because this
  // constructor can only be generated from another CreateTableDeviceNode
  public CreateOrUpdateTableDeviceNode(
      final PlanNodeId id,
      final TRegionReplicaSet regionReplicaSet,
      final String database,
      final String tableName,
      final List<Object[]> deviceIdList,
      final List<String> attributeNameList,
      final List<Object[]> attributeValueList) {
    super(id);
    this.database = database;
    this.tableName = tableName;
    this.deviceIdList = deviceIdList;
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
    this.regionReplicaSet = regionReplicaSet;
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.CREATE_OR_UPDATE_TABLE_DEVICE;
  }

  public String getDatabase() {
    return database;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Object[]> getDeviceIdList() {
    return deviceIdList;
  }

  public List<String> getAttributeNameList() {
    return attributeNameList;
  }

  public List<Object[]> getAttributeValueList() {
    return attributeValueList;
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return regionReplicaSet;
  }

  public List<IDeviceID> getPartitionKeyList() {
    if (partitionKeyList == null) {
      this.partitionKeyList = convertRawDeviceIDs2PartitionKeys(tableName, deviceIdList);
    }
    return partitionKeyList;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(final PlanNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlanNode clone() {
    return new CreateOrUpdateTableDeviceNode(
        getPlanNodeId(),
        regionReplicaSet,
        database,
        tableName,
        deviceIdList,
        attributeNameList,
        attributeValueList);
  }

  @Override
  public int allowedChildCount() {
    return 0;
  }

  @Override
  public List<String> getOutputColumnNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void serializeAttributes(final ByteBuffer byteBuffer) {
    PlanNodeType.CREATE_OR_UPDATE_TABLE_DEVICE.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);
    ReadWriteIOUtils.write(deviceIdList.size(), byteBuffer);
    for (final Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, byteBuffer);
      for (final Object idSeg : deviceId) {
        ReadWriteIOUtils.writeObject(idSeg, byteBuffer);
      }
    }
    ReadWriteIOUtils.write(attributeNameList.size(), byteBuffer);
    for (final String attributeName : attributeNameList) {
      ReadWriteIOUtils.write(attributeName, byteBuffer);
    }
    ReadWriteIOUtils.write(attributeValueList.size(), byteBuffer);
    for (final Object[] deviceValueList : attributeValueList) {
      for (final Object value : deviceValueList) {
        ReadWriteIOUtils.writeObject(value, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(final DataOutputStream stream) throws IOException {
    PlanNodeType.CREATE_OR_UPDATE_TABLE_DEVICE.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(deviceIdList.size(), stream);
    for (final Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, stream);
      for (final Object idSeg : deviceId) {
        ReadWriteIOUtils.writeObject(idSeg, stream);
      }
    }
    ReadWriteIOUtils.write(attributeNameList.size(), stream);
    for (final String attributeName : attributeNameList) {
      ReadWriteIOUtils.write(attributeName, stream);
    }
    for (final Object[] deviceValueList : attributeValueList) {
      for (final Object value : deviceValueList) {
        ReadWriteIOUtils.writeObject(value, stream);
      }
    }
  }

  public static CreateOrUpdateTableDeviceNode deserialize(final ByteBuffer buffer) {
    final String database = ReadWriteIOUtils.readString(buffer);
    final String tableName = ReadWriteIOUtils.readString(buffer);
    final int deviceNum = ReadWriteIOUtils.readInt(buffer);
    final List<Object[]> deviceIdList = new ArrayList<>(deviceNum);
    int length;
    Object[] deviceId;
    for (int i = 0; i < deviceNum; i++) {
      length = ReadWriteIOUtils.readInt(buffer);
      deviceId = new Object[length];
      for (int j = 0; j < length; j++) {
        deviceId[j] = ReadWriteIOUtils.readObject(buffer);
      }
      deviceIdList.add(deviceId);
    }
    final int attributeNameNum = ReadWriteIOUtils.readInt(buffer);
    final List<String> attributeNameList = new ArrayList<>(attributeNameNum);
    for (int i = 0; i < attributeNameNum; i++) {
      attributeNameList.add(ReadWriteIOUtils.readString(buffer));
    }
    final List<Object[]> attributeValueList = new ArrayList<>(deviceNum);
    Object[] deviceAttributeValues;
    for (int i = 0; i < deviceNum; i++) {
      deviceAttributeValues = new Object[attributeNameNum];
      for (int j = 0; j < attributeNameNum; j++) {
        deviceAttributeValues[j] = ReadWriteIOUtils.readObject(buffer);
      }
      attributeValueList.add(deviceAttributeValues);
    }
    final PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new CreateOrUpdateTableDeviceNode(
        planNodeId, database, tableName, deviceIdList, attributeNameList, attributeValueList);
  }

  @Override
  public List<WritePlanNode> splitByPartition(final IAnalysis analysis) {
    final String dbNameForInvoke = PATH_ROOT + PATH_SEPARATOR + database;
    final Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    final List<IDeviceID> partitionKeyList = getPartitionKeyList();
    for (int i = 0; i < partitionKeyList.size(); i++) {
      // Use the string literal of deviceId as the partition key
      final TRegionReplicaSet regionReplicaSet =
          analysis
              .getSchemaPartitionInfo()
              .getSchemaRegionReplicaSet(dbNameForInvoke, partitionKeyList.get(i));
      splitMap.computeIfAbsent(regionReplicaSet, k -> new ArrayList<>()).add(i);
    }
    final List<WritePlanNode> result = new ArrayList<>(splitMap.size());
    for (final Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      final List<Object[]> subDeviceIdList = new ArrayList<>(entry.getValue().size());
      final List<Object[]> subAttributeValueList = new ArrayList<>(entry.getValue().size());
      for (final Integer index : entry.getValue()) {
        subDeviceIdList.add(deviceIdList.get(index));
        subAttributeValueList.add(attributeValueList.get(index));
      }
      result.add(
          new CreateOrUpdateTableDeviceNode(
              getPlanNodeId(),
              entry.getKey(),
              database,
              tableName,
              subDeviceIdList,
              attributeNameList,
              subAttributeValueList));
    }
    return result;
  }

  @Override
  public <R, C> R accept(final PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTableDevice(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final CreateOrUpdateTableDeviceNode node = (CreateOrUpdateTableDeviceNode) o;
    return Objects.equals(database, node.database)
        && Objects.equals(tableName, node.tableName)
        && Objects.equals(deviceIdList, node.deviceIdList)
        && Objects.equals(attributeNameList, node.attributeNameList)
        && Objects.equals(attributeValueList, node.attributeValueList)
        && Objects.equals(regionReplicaSet, node.regionReplicaSet)
        && Objects.equals(partitionKeyList, node.partitionKeyList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        database,
        tableName,
        deviceIdList,
        attributeNameList,
        attributeValueList,
        regionReplicaSet,
        partitionKeyList);
  }

  @Override
  public SchemaRegionPlanType getPlanType() {
    return SchemaRegionPlanType.CREATE_TABLE_DEVICE;
  }

  @Override
  public <R, C> R accept(final SchemaRegionPlanVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateOrUpdateTableDevice(this, context);
  }
}
