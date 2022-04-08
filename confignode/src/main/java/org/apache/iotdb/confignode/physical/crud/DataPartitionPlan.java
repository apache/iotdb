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

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Query or apply DataPartition by the specific storageGroup and the deviceGroupStartTimeMap. */
public class DataPartitionPlan extends PhysicalPlan {
  private String storageGroup;
  private Map<Integer, List<Long>> seriesPartitionTimePartitionSlots;
  private Map<Integer, Map<Long, List<RegionReplicaSet>>> dataPartitionReplicaSets;

  public DataPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public DataPartitionPlan(
      PhysicalPlanType physicalPlanType,
      String storageGroup,
      Map<Integer, List<Long>> seriesPartitionTimePartitionSlots) {
    this(physicalPlanType);
    this.storageGroup = storageGroup;
    this.seriesPartitionTimePartitionSlots = seriesPartitionTimePartitionSlots;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(String storageGroup) {
    this.storageGroup = storageGroup;
  }

  public Map<Integer, List<Long>> getSeriesPartitionTimePartitionSlots() {
    return seriesPartitionTimePartitionSlots;
  }

  public void setSeriesPartitionTimePartitionSlots(Map<Integer, List<Long>> deviceGroupIDs) {
    this.seriesPartitionTimePartitionSlots = deviceGroupIDs;
  }

  public Map<Integer, Map<Long, List<RegionReplicaSet>>> getDataPartitionReplicaSets() {
    return dataPartitionReplicaSets;
  }

  public void setDataPartitionReplicaSets(
      Map<Integer, Map<Long, List<RegionReplicaSet>>> dataPartitionReplicaSets) {
    this.dataPartitionReplicaSets = dataPartitionReplicaSets;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.QueryDataPartition.ordinal());
    SerializeDeserializeUtil.write(storageGroup, buffer);

    buffer.putInt(seriesPartitionTimePartitionSlots.size());
    for (Map.Entry<Integer, List<Long>> entry : seriesPartitionTimePartitionSlots.entrySet()) {
      buffer.putInt(entry.getKey());
      buffer.putInt(entry.getValue().size());
      for (Long startTime : entry.getValue()) {
        buffer.putLong(startTime);
      }
    }

    buffer.putInt(dataPartitionReplicaSets.size());
    for (Map.Entry<Integer, Map<Long, List<RegionReplicaSet>>> seriesPartitionSlotEntry :
        dataPartitionReplicaSets.entrySet()) {
      buffer.putInt(seriesPartitionSlotEntry.getKey());
      buffer.putInt(seriesPartitionSlotEntry.getValue().size());
      for (Map.Entry<Long, List<RegionReplicaSet>> timePartitionSlotEntry :
          seriesPartitionSlotEntry.getValue().entrySet()) {
        buffer.putLong(timePartitionSlotEntry.getKey());
        buffer.putInt(timePartitionSlotEntry.getValue().size());
        for (RegionReplicaSet regionReplicaSet : timePartitionSlotEntry.getValue()) {
          regionReplicaSet.deserializeImpl(buffer);
        }
      }
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    storageGroup = SerializeDeserializeUtil.readString(buffer);

    int mapLength = buffer.getInt();
    seriesPartitionTimePartitionSlots = new HashMap<>(mapLength);
    for (int i = 0; i < mapLength; i++) {
      int deviceGroupId = buffer.getInt();
      int listLength = buffer.getInt();
      List<Long> startTimeList = new ArrayList<>(listLength);
      for (int j = 0; j < listLength; j++) {
        startTimeList.set(j, buffer.getLong());
      }
      seriesPartitionTimePartitionSlots.put(deviceGroupId, startTimeList);
    }

    mapLength = buffer.getInt();
    dataPartitionReplicaSets = new HashMap<>(mapLength);
    for (int i = 0; i < mapLength; i++) {
      int seriesPartitionSlot = buffer.getInt();
      int subMapLength = buffer.getInt();
      dataPartitionReplicaSets.put(seriesPartitionSlot, new HashMap<>());
      for (int j = 0; j < subMapLength; j++) {
        long timePartitionSlot = buffer.getLong();
        int listLength = buffer.getInt();
        dataPartitionReplicaSets.get(seriesPartitionSlot).put(timePartitionSlot, new ArrayList<>());
        for (int k = 0; k < listLength; k++) {
          RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
          regionReplicaSet.deserializeImpl(buffer);
          dataPartitionReplicaSets
              .get(seriesPartitionSlot)
              .get(timePartitionSlot)
              .add(regionReplicaSet);
        }
      }
    }
  }
}
