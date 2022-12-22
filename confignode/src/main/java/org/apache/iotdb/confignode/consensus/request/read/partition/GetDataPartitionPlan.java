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
package org.apache.iotdb.confignode.consensus.request.read.partition;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Get or create DataPartition by the specific partitionSlotsMap. */
public class GetDataPartitionPlan extends ConfigPhysicalPlan {

  // Map<StorageGroup, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
  protected Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap;

  public GetDataPartitionPlan() {
    super(ConfigPhysicalPlanType.GetDataPartition);
  }

  public GetDataPartitionPlan(ConfigPhysicalPlanType configPhysicalPlanType) {
    super(configPhysicalPlanType);
  }

  public GetDataPartitionPlan(
      Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap) {
    this();
    this.partitionSlotsMap = partitionSlotsMap;
  }

  public Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  /**
   * Convert TDataPartitionReq to GetDataPartitionPlan
   *
   * @param req TDataPartitionReq
   * @return GetDataPartitionPlan
   */
  public static GetDataPartitionPlan convertFromRpcTDataPartitionReq(TDataPartitionReq req) {
    return new GetDataPartitionPlan(new ConcurrentHashMap<>(req.getPartitionSlotsMap()));
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(partitionSlotsMap.size());
    for (Entry<String, Map<TSeriesPartitionSlot, TTimeSlotList>> entry :
        partitionSlotsMap.entrySet()) {
      String storageGroup = entry.getKey();
      Map<TSeriesPartitionSlot, TTimeSlotList> seriesPartitionTimePartitionSlots = entry.getValue();
      BasicStructureSerDeUtil.write(storageGroup, stream);
      stream.writeInt(seriesPartitionTimePartitionSlots.size());
      for (Entry<TSeriesPartitionSlot, TTimeSlotList> e :
          seriesPartitionTimePartitionSlots.entrySet()) {
        TSeriesPartitionSlot seriesPartitionSlot = e.getKey();
        TTimeSlotList timePartitionSlotList = e.getValue();
        ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesPartitionSlot, stream);
        ThriftCommonsSerDeUtils.serializeTTimePartitionSlotList(timePartitionSlotList, stream);
      }
    }
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
            ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
        TTimeSlotList timePartitionSlotList =
            ThriftCommonsSerDeUtils.deserializeTTimePartitionSlotList(buffer);
        partitionSlotsMap.get(storageGroup).put(seriesPartitionSlot, timePartitionSlotList);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetDataPartitionPlan that = (GetDataPartitionPlan) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
