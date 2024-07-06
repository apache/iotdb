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

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

public class CreateTableDeviceNode extends WritePlanNode {

  private final String database;

  private final String tableName;

  private final List<Object[]> deviceIdList;

  private final List<String> attributeNameList;

  private final List<Object[]> attributeValueList;

  private TRegionReplicaSet regionReplicaSet;

  private transient List<IDeviceID> partitionKeyList;

  public CreateTableDeviceNode(
      PlanNodeId id,
      String database,
      String tableName,
      List<Object[]> deviceIdList,
      List<String> attributeNameList,
      List<Object[]> attributeValueList) {
    super(id);
    this.database = database;
    this.tableName = tableName;
    // truncate the tailing null
    this.deviceIdList = truncateTailingNull(deviceIdList);
    this.attributeNameList = attributeNameList;
    this.attributeValueList = attributeValueList;
  }

  private static List<Object[]> truncateTailingNull(List<Object[]> deviceIdList) {
    List<Object[]> res = new ArrayList<>(deviceIdList.size());
    for (Object[] device : deviceIdList) {
      if (device == null || device.length == 0) {
        throw new IllegalArgumentException("DeviceID's length should be larger than 0.");
      }
      int lastNonNullIndex = -1;
      for (int i = device.length - 1; i >= 0; i--) {
        if (device[i] != null) {
          lastNonNullIndex = i;
          break;
        }
      }
      if (lastNonNullIndex == -1) {
        throw new IllegalArgumentException("DeviceID shouldn't be all nulls.");
      }
      res.add(
          lastNonNullIndex == device.length - 1
              ? device
              : Arrays.copyOf(device, lastNonNullIndex + 1));
    }
    return res;
  }

  // in this constructor, we don't need to truncate tailing nulls for deviceIdList, because this
  // constructor can only be generated from another CreateTableDeviceNode
  public CreateTableDeviceNode(
      PlanNodeId id,
      TRegionReplicaSet regionReplicaSet,
      String database,
      String tableName,
      List<Object[]> deviceIdList,
      List<String> attributeNameList,
      List<Object[]> attributeValueList) {
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
    return PlanNodeType.CREATE_TABLE_DEVICE;
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
      List<IDeviceID> tmpPartitionKeyList = new ArrayList<>();
      for (Object[] rawId : deviceIdList) {
        String[] partitionKey = new String[rawId.length + 1];
        partitionKey[0] = tableName;
        for (int i = 0; i < rawId.length; i++) {
          partitionKey[i + 1] = (String) rawId[i];
        }
        tmpPartitionKeyList.add(IDeviceID.Factory.DEFAULT_FACTORY.create(partitionKey));
      }
      this.partitionKeyList = tmpPartitionKeyList;
    }
    return partitionKeyList;
  }

  @Override
  public List<PlanNode> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public void addChild(PlanNode child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PlanNode clone() {
    return new CreateTableDeviceNode(
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
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.CREATE_TABLE_DEVICE.serialize(byteBuffer);
    ReadWriteIOUtils.write(database, byteBuffer);
    ReadWriteIOUtils.write(tableName, byteBuffer);
    ReadWriteIOUtils.write(deviceIdList.size(), byteBuffer);
    for (Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, byteBuffer);
      for (Object idSeg : deviceId) {
        ReadWriteIOUtils.writeObject(idSeg, byteBuffer);
      }
    }
    ReadWriteIOUtils.write(attributeNameList.size(), byteBuffer);
    for (String attributeName : attributeNameList) {
      ReadWriteIOUtils.write(attributeName, byteBuffer);
    }
    ReadWriteIOUtils.write(attributeValueList.size(), byteBuffer);
    for (Object[] deviceValueList : attributeValueList) {
      for (Object value : deviceValueList) {
        ReadWriteIOUtils.writeObject(value, byteBuffer);
      }
    }
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.CREATE_TABLE_DEVICE.serialize(stream);
    ReadWriteIOUtils.write(database, stream);
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(deviceIdList.size(), stream);
    for (Object[] deviceId : deviceIdList) {
      ReadWriteIOUtils.write(deviceId.length, stream);
      for (Object idSeg : deviceId) {
        ReadWriteIOUtils.writeObject(idSeg, stream);
      }
    }
    ReadWriteIOUtils.write(attributeNameList.size(), stream);
    for (String attributeName : attributeNameList) {
      ReadWriteIOUtils.write(attributeName, stream);
    }
    for (Object[] deviceValueList : attributeValueList) {
      for (Object value : deviceValueList) {
        ReadWriteIOUtils.writeObject(value, stream);
      }
    }
  }

  public static CreateTableDeviceNode deserialize(ByteBuffer buffer) {
    String database = ReadWriteIOUtils.readString(buffer);
    String tableName = ReadWriteIOUtils.readString(buffer);
    int deviceNum = ReadWriteIOUtils.readInt(buffer);
    List<Object[]> deviceIdList = new ArrayList<>(deviceNum);
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
    int attributeNameNum = ReadWriteIOUtils.readInt(buffer);
    List<String> attributeNameList = new ArrayList<>(attributeNameNum);
    for (int i = 0; i < attributeNameNum; i++) {
      attributeNameList.add(ReadWriteIOUtils.readString(buffer));
    }
    List<Object[]> attributeValueList = new ArrayList<>(deviceNum);
    Object[] deviceAttributeValues;
    for (int i = 0; i < deviceNum; i++) {
      deviceAttributeValues = new Object[attributeNameNum];
      for (int j = 0; j < attributeNameNum; j++) {
        deviceAttributeValues[j] = ReadWriteIOUtils.readObject(buffer);
      }
      attributeValueList.add(deviceAttributeValues);
    }
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new CreateTableDeviceNode(
        planNodeId, database, tableName, deviceIdList, attributeNameList, attributeValueList);
  }

  @Override
  public List<WritePlanNode> splitByPartition(IAnalysis analysis) {
    String dbNameForInvoke = PATH_ROOT + PATH_SEPARATOR + database;
    Map<TRegionReplicaSet, List<Integer>> splitMap = new HashMap<>();
    List<IDeviceID> partitionKeyList = getPartitionKeyList();
    for (int i = 0; i < partitionKeyList.size(); i++) {
      // use the string literal of deviceId as the partition key
      TRegionReplicaSet regionReplicaSet =
          analysis
              .getSchemaPartitionInfo()
              .getSchemaRegionReplicaSet(dbNameForInvoke, partitionKeyList.get(i));
      splitMap.computeIfAbsent(regionReplicaSet, k -> new ArrayList<>()).add(i);
    }
    List<WritePlanNode> result = new ArrayList<>(splitMap.size());
    for (Map.Entry<TRegionReplicaSet, List<Integer>> entry : splitMap.entrySet()) {
      List<Object[]> subDeviceIdList = new ArrayList<>(entry.getValue().size());
      List<Object[]> subAttributeValueList = new ArrayList<>(entry.getValue().size());
      for (Integer index : entry.getValue()) {
        subDeviceIdList.add(deviceIdList.get(index));
        subAttributeValueList.add(attributeValueList.get(index));
      }
      result.add(
          new CreateTableDeviceNode(
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
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTableDevice(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    CreateTableDeviceNode node = (CreateTableDeviceNode) o;
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
}
