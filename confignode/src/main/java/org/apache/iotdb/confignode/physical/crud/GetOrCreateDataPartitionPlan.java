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

import org.apache.iotdb.commons.partition.SeriesPartitionSlot;
import org.apache.iotdb.commons.partition.TimePartitionSlot;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Get or create DataPartition by the specific partitionSlotsMap. */
public class GetOrCreateDataPartitionPlan extends PhysicalPlan {

  private Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap;

  public GetOrCreateDataPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  @TestOnly
  public void setPartitionSlotsMap(
      Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap) {
    this.partitionSlotsMap = partitionSlotsMap;
  }

  /**
   * Convert TDataPartitionReq to DataPartitionPlan
   *
   * @param req TDataPartitionReq
   */
  public void convertFromRpcTDataPartitionReq(TDataPartitionReq req) {
    partitionSlotsMap = new HashMap<>();
    req.getPartitionSlotsMap()
        .forEach(
            ((storageGroup, tSeriesPartitionTimePartitionSlots) -> {
              // Extract StorageGroupName
              partitionSlotsMap.putIfAbsent(storageGroup, new HashMap<>());

              tSeriesPartitionTimePartitionSlots.forEach(
                  ((tSeriesPartitionSlot, tTimePartitionSlots) -> {
                    // Extract SeriesPartitionSlot
                    SeriesPartitionSlot seriesPartitionSlot =
                        new SeriesPartitionSlot(tSeriesPartitionSlot.getSlotId());
                    partitionSlotsMap
                        .get(storageGroup)
                        .putIfAbsent(seriesPartitionSlot, new ArrayList<>());

                    // Extract TimePartitionSlots
                    tTimePartitionSlots.forEach(
                        tTimePartitionSlot ->
                            partitionSlotsMap
                                .get(storageGroup)
                                .get(seriesPartitionSlot)
                                .add(new TimePartitionSlot(tTimePartitionSlot.getStartTime())));
                  }));
            }));
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getType().ordinal());

    buffer.putInt(partitionSlotsMap.size());
    partitionSlotsMap.forEach(
        ((storageGroup, seriesPartitionTimePartitionSlots) -> {
          SerializeDeserializeUtil.write(storageGroup, buffer);
          buffer.putInt(seriesPartitionTimePartitionSlots.size());
          seriesPartitionTimePartitionSlots.forEach(
              ((seriesPartitionSlot, timePartitionSlots) -> {
                seriesPartitionSlot.serializeImpl(buffer);
                buffer.putInt(timePartitionSlots.size());
                timePartitionSlots.forEach(
                    timePartitionSlot -> timePartitionSlot.serializeImpl(buffer));
              }));
        }));
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    partitionSlotsMap = new HashMap<>();
    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = SerializeDeserializeUtil.readString(buffer);
      partitionSlotsMap.put(storageGroup, new HashMap<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot();
        seriesPartitionSlot.deserializeImpl(buffer);
        partitionSlotsMap.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
        int timePartitionSlotNum = buffer.getInt();
        for (int k = 0; k < timePartitionSlotNum; k++) {
          TimePartitionSlot timePartitionSlot = new TimePartitionSlot();
          timePartitionSlot.deserializeImpl(buffer);
          partitionSlotsMap.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetOrCreateDataPartitionPlan that = (GetOrCreateDataPartitionPlan) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
