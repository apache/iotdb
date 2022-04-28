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
package org.apache.iotdb.confignode.consensus.request.read;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Get or create DataPartition by the specific partitionSlotsMap. */
public class GetDataPartitionReq extends ConfigRequest {

  private Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap;

  public GetDataPartitionReq() {
    super(ConfigRequestType.GetDataPartition);
  }

  public GetDataPartitionReq(ConfigRequestType configRequestType) {
    super(configRequestType);
  }

  public Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  @TestOnly
  public void setPartitionSlotsMap(
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap) {
    this.partitionSlotsMap = partitionSlotsMap;
  }

  /**
   * Convert TDataPartitionReq to GetOrCreateDataPartitionPlan
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
                    partitionSlotsMap
                        .get(storageGroup)
                        .putIfAbsent(tSeriesPartitionSlot, new ArrayList<>());

                    // Extract TimePartitionSlots
                    tTimePartitionSlots.forEach(
                        tTimePartitionSlot ->
                            partitionSlotsMap
                                .get(storageGroup)
                                .get(tSeriesPartitionSlot)
                                .add(tTimePartitionSlot));
                  }));
            }));
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getType().ordinal());

    buffer.putInt(partitionSlotsMap.size());
    partitionSlotsMap.forEach(
        ((storageGroup, seriesPartitionTimePartitionSlots) -> {
          BasicStructureSerDeUtil.write(storageGroup, buffer);
          buffer.putInt(seriesPartitionTimePartitionSlots.size());
          seriesPartitionTimePartitionSlots.forEach(
              ((seriesPartitionSlot, timePartitionSlots) -> {
                ThriftCommonsSerDeUtils.writeTSeriesPartitionSlot(seriesPartitionSlot, buffer);
                buffer.putInt(timePartitionSlots.size());
                timePartitionSlots.forEach(
                    timePartitionSlot ->
                        ThriftCommonsSerDeUtils.writeTTimePartitionSlot(timePartitionSlot, buffer));
              }));
        }));
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    partitionSlotsMap = new HashMap<>();
    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      partitionSlotsMap.put(storageGroup, new HashMap<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot =
            ThriftCommonsSerDeUtils.readTSeriesPartitionSlot(buffer);
        partitionSlotsMap.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
        int timePartitionSlotNum = buffer.getInt();
        for (int k = 0; k < timePartitionSlotNum; k++) {
          TTimePartitionSlot timePartitionSlot =
              ThriftCommonsSerDeUtils.readTTimePartitionSlot(buffer);
          partitionSlotsMap.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetDataPartitionReq that = (GetDataPartitionReq) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
