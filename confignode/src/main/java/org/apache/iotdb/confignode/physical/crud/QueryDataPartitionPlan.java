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
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Query or apply DataPartition by the specific storageGroup and the deviceGroupStartTimeMap. */
public class QueryDataPartitionPlan extends PhysicalPlan {

  private Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> partitionSlotsMap;

  public QueryDataPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public Map<String, Map<SeriesPartitionSlot, List<TimePartitionSlot>>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  /**
   * Convert TDataPartitionReq to DataPartitionPlan
   *
   * @param req TDataPartitionReq
   */
  public void convertFromRpcTDataPartitionReq(TDataPartitionReq req) {
    partitionSlotsMap = new HashMap<>();
    req.getPartitionSlotsMap().forEach(((storageGroup, tSeriesPartitionTimePartitionSlots) -> {
      // Extract StorageGroupName
      partitionSlotsMap.putIfAbsent(storageGroup, new HashMap<>());

      tSeriesPartitionTimePartitionSlots.forEach(((tSeriesPartitionSlot, tTimePartitionSlots) -> {
        // Extract SeriesPartitionSlot
        SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot(tSeriesPartitionSlot.getSlotId());
        partitionSlotsMap.get(storageGroup).putIfAbsent(seriesPartitionSlot, new ArrayList<>());

        // Extract TimePartitionSlots
        tTimePartitionSlots.forEach(tTimePartitionSlot -> partitionSlotsMap.get(storageGroup).get(seriesPartitionSlot).add(new TimePartitionSlot(tTimePartitionSlot.getStartTime())));
      }));
    }));
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.GetDataPartition.ordinal());

  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {

  }
}
