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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Query or apply SchemaPartition by the specific storageGroup and the deviceGroupStartTimeMap. */
public class GetOrCreateSchemaPartitionPlan extends PhysicalPlan {
  private String storageGroup;
  private List<Integer> seriesPartitionSlots;
  private Map<Integer, RegionReplicaSet> schemaPartitionReplicaSets;

  public GetOrCreateSchemaPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public GetOrCreateSchemaPartitionPlan(
      PhysicalPlanType physicalPlanType, String storageGroup, List<Integer> seriesPartitionSlots) {
    this(physicalPlanType);
    this.storageGroup = storageGroup;
    this.seriesPartitionSlots = seriesPartitionSlots;
  }

  public void setSchemaPartitionReplicaSet(
      Map<Integer, RegionReplicaSet> deviceGroupIdReplicaSets) {
    this.schemaPartitionReplicaSets = deviceGroupIdReplicaSets;
  }

  public Map<Integer, RegionReplicaSet> getSchemaPartitionReplicaSets() {
    return schemaPartitionReplicaSets;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.GetDataPartition.ordinal());
    SerializeDeserializeUtil.write(storageGroup, buffer);
    buffer.putInt(seriesPartitionSlots.size());
    seriesPartitionSlots.forEach(id -> SerializeDeserializeUtil.write(id, buffer));

    buffer.putInt(schemaPartitionReplicaSets.size());
    for (Map.Entry<Integer, RegionReplicaSet> entry : schemaPartitionReplicaSets.entrySet()) {
      buffer.putInt(entry.getKey());
      entry.getValue().serializeImpl(buffer);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    storageGroup = SerializeDeserializeUtil.readString(buffer);
    int idSize = SerializeDeserializeUtil.readInt(buffer);
    for (int i = 0; i < idSize; i++) {
      seriesPartitionSlots.add(SerializeDeserializeUtil.readInt(buffer));
    }

    if (schemaPartitionReplicaSets == null) {
      schemaPartitionReplicaSets = new HashMap<>();
    }
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      RegionReplicaSet schemaRegionReplicaSet = new RegionReplicaSet();
      schemaRegionReplicaSet.deserializeImpl(buffer);
      schemaPartitionReplicaSets.put(buffer.getInt(), schemaRegionReplicaSet);
    }
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public List<Integer> getSeriesPartitionSlots() {
    return seriesPartitionSlots;
  }
}
