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
package org.apache.iotdb.confignode.physical.crud;

import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Query or apply DataPartition by the specific storageGroup and the deviceGroupStartTimeMap.
 */
public class DataPartitionPlan extends PhysicalPlan {
  private String storageGroup;
  private Map<Integer, List<Long>> deviceGroupStartTimeMap;

  public DataPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public DataPartitionPlan(
      PhysicalPlanType physicalPlanType,
      String storageGroup,
      Map<Integer, List<Long>> deviceGroupStartTimeMap) {
    this(physicalPlanType);
    this.storageGroup = storageGroup;
    this.deviceGroupStartTimeMap = deviceGroupStartTimeMap;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(String storageGroup) {
    this.storageGroup = storageGroup;
  }

  public Map<Integer, List<Long>> getDeviceGroupIDs() {
    return deviceGroupStartTimeMap;
  }

  public void setDeviceGroupIDs(Map<Integer, List<Long>> deviceGroupIDs) {
    this.deviceGroupStartTimeMap = deviceGroupIDs;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.QueryDataPartition.ordinal());
    SerializeDeserializeUtil.write(storageGroup, buffer);

    buffer.putInt(deviceGroupStartTimeMap.size());
    for (Map.Entry<Integer, List<Long>> entry : deviceGroupStartTimeMap.entrySet()) {
      buffer.putInt(entry.getKey());
      buffer.putInt(entry.getValue().size());
      for (Long startTime : entry.getValue()) {
        buffer.putLong(startTime);
      }
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    storageGroup = SerializeDeserializeUtil.readString(buffer);

    int mapLength = buffer.getInt();
    deviceGroupStartTimeMap = new HashMap<>(mapLength);
    for (int i = 0; i < mapLength; i++) {
      int deviceGroupId = buffer.getInt();
      int listLength = buffer.getInt();
      List<Long> startTimeList = new ArrayList<>(listLength);
      for (int j = 0; j < listLength; j++) {
        startTimeList.set(j, buffer.getLong());
      }
      deviceGroupStartTimeMap.put(deviceGroupId, startTimeList);
    }
  }
}
