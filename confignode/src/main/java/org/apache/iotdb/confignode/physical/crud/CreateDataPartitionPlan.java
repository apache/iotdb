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
import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateDataPartitionPlan extends PhysicalPlan {

  private Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      assignedDataPartition;

  public CreateDataPartitionPlan(PhysicalPlanType type) {
    super(type);
  }

  public Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
      getAssignedDataPartition() {
    return assignedDataPartition;
  }

  public void setAssignedDataPartition(
      Map<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
          assignedDataPartition) {
    this.assignedDataPartition = assignedDataPartition;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.CreateDataPartition.ordinal());

    buffer.putInt(assignedDataPartition.size());
    for (Map.Entry<String, Map<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>>
        seriesPartitionTimePartitionEntry : assignedDataPartition.entrySet()) {
      SerializeDeserializeUtil.write(seriesPartitionTimePartitionEntry.getKey(), buffer);
      buffer.putInt(seriesPartitionTimePartitionEntry.getValue().size());
      for (Map.Entry<SeriesPartitionSlot, Map<TimePartitionSlot, List<RegionReplicaSet>>>
          timePartitionEntry : seriesPartitionTimePartitionEntry.getValue().entrySet()) {
        timePartitionEntry.getKey().serializeImpl(buffer);
        buffer.putInt(timePartitionEntry.getValue().size());
        for (Map.Entry<TimePartitionSlot, List<RegionReplicaSet>> regionReplicaSetEntry :
            timePartitionEntry.getValue().entrySet()) {
          regionReplicaSetEntry.getKey().serializeImpl(buffer);
          buffer.putInt(regionReplicaSetEntry.getValue().size());
          for (RegionReplicaSet regionReplicaSet : regionReplicaSetEntry.getValue()) {
            regionReplicaSet.serializeImpl(buffer);
          }
        }
      }
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedDataPartition = new HashMap<>();
    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroupName = SerializeDeserializeUtil.readString(buffer);
      assignedDataPartition.put(storageGroupName, new HashMap<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot();
        seriesPartitionSlot.deserializeImpl(buffer);
        assignedDataPartition.get(storageGroupName).put(seriesPartitionSlot, new HashMap<>());
        int timePartitionSlotNum = buffer.getInt();
        for (int k = 0; k < timePartitionSlotNum; k++) {
          TimePartitionSlot timePartitionSlot = new TimePartitionSlot();
          timePartitionSlot.deserializeImpl(buffer);
          assignedDataPartition
              .get(storageGroupName)
              .get(seriesPartitionSlot)
              .put(timePartitionSlot, new ArrayList<>());
          int regionReplicaSetNum = buffer.getInt();
          for (int l = 0; l < regionReplicaSetNum; l++) {
            RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
            regionReplicaSet.deserializeImpl(buffer);
            assignedDataPartition
                .get(storageGroupName)
                .get(seriesPartitionSlot)
                .get(timePartitionSlot)
                .add(regionReplicaSet);
          }
        }
      }
    }
  }
}
