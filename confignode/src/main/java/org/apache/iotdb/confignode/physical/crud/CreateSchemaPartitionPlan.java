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
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Create SchemaPartition by assignedSchemaPartition */
public class CreateSchemaPartitionPlan extends PhysicalPlan {

  private Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> assignedSchemaPartition;

  public CreateSchemaPartitionPlan() {
    super(PhysicalPlanType.CreateSchemaPartition);
  }

  public Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> getAssignedSchemaPartition() {
    return assignedSchemaPartition;
  }

  public void setAssignedSchemaPartition(
      Map<String, Map<SeriesPartitionSlot, RegionReplicaSet>> assignedSchemaPartition) {
    this.assignedSchemaPartition = assignedSchemaPartition;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.CreateSchemaPartition.ordinal());

    buffer.putInt(assignedSchemaPartition.size());
    assignedSchemaPartition.forEach(
        (storageGroup, partitionSlots) -> {
          SerializeDeserializeUtil.write(storageGroup, buffer);
          buffer.putInt(partitionSlots.size());
          partitionSlots.forEach(
              (seriesPartitionSlot, regionReplicaSet) -> {
                seriesPartitionSlot.serializeImpl(buffer);
                regionReplicaSet.serializeImpl(buffer);
              });
        });
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedSchemaPartition = new HashMap<>();

    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = SerializeDeserializeUtil.readString(buffer);
      assignedSchemaPartition.put(storageGroup, new HashMap<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        SeriesPartitionSlot seriesPartitionSlot = new SeriesPartitionSlot();
        seriesPartitionSlot.deserializeImpl(buffer);
        assignedSchemaPartition
            .get(storageGroup)
            .put(seriesPartitionSlot, RegionReplicaSet.deserializeImpl(buffer));
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateSchemaPartitionPlan that = (CreateSchemaPartitionPlan) o;
    return assignedSchemaPartition.equals(that.assignedSchemaPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedSchemaPartition);
  }
}
