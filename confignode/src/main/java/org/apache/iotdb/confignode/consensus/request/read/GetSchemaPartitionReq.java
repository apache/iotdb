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
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Get or create SchemaPartition by the specific partitionSlotsMap. */
public class GetSchemaPartitionReq extends ConfigRequest {

  // Map<StorageGroup, List<SeriesPartitionSlot>>
  // Get all SchemaPartitions when the partitionSlotsMap is empty
  // Get all exists SchemaPartitions in one StorageGroup when the SeriesPartitionSlot is empty
  private Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap;

  public GetSchemaPartitionReq() {
    super(ConfigRequestType.GetSchemaPartition);
  }

  public GetSchemaPartitionReq(ConfigRequestType configRequestType) {
    super(configRequestType);
  }

  public void setPartitionSlotsMap(Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    this.partitionSlotsMap = partitionSlotsMap;
  }

  public Map<String, List<TSeriesPartitionSlot>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(getType().ordinal());

    buffer.putInt(partitionSlotsMap.size());
    partitionSlotsMap.forEach(
        (storageGroup, seriesPartitionSlots) -> {
          BasicStructureSerDeUtil.write(storageGroup, buffer);
          buffer.putInt(seriesPartitionSlots.size());
          seriesPartitionSlots.forEach(
              seriesPartitionSlot ->
                  ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(
                      seriesPartitionSlot, buffer));
        });
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
    GetSchemaPartitionReq that = (GetSchemaPartitionReq) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
