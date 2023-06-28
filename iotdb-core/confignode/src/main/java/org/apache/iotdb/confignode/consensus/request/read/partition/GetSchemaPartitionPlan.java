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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/** Get or create SchemaPartition by the specific partitionSlotsMap. */
public class GetSchemaPartitionPlan extends ConfigPhysicalPlan {

  // Map<StorageGroup, List<SeriesPartitionSlot>>
  // Get all SchemaPartitions when the partitionSlotsMap is empty
  // Get all exists SchemaPartitions in one StorageGroup when the SeriesPartitionSlot is empty
  protected Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap;

  public GetSchemaPartitionPlan() {
    super(ConfigPhysicalPlanType.GetSchemaPartition);
  }

  public GetSchemaPartitionPlan(ConfigPhysicalPlanType configPhysicalPlanType) {
    super(configPhysicalPlanType);
  }

  public GetSchemaPartitionPlan(Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    this();
    this.partitionSlotsMap = partitionSlotsMap;
  }

  public Map<String, List<TSeriesPartitionSlot>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(partitionSlotsMap.size());
    for (Entry<String, List<TSeriesPartitionSlot>> entry : partitionSlotsMap.entrySet()) {
      String storageGroup = entry.getKey();
      List<TSeriesPartitionSlot> seriesPartitionSlots = entry.getValue();
      BasicStructureSerDeUtil.write(storageGroup, stream);
      stream.writeInt(seriesPartitionSlots.size());
      seriesPartitionSlots.forEach(
          seriesPartitionSlot ->
              ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesPartitionSlot, stream));
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    partitionSlotsMap = new HashMap<>();
    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      partitionSlotsMap.put(storageGroup, new ArrayList<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot =
            ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
        partitionSlotsMap.get(storageGroup).add(seriesPartitionSlot);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetSchemaPartitionPlan that = (GetSchemaPartitionPlan) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
