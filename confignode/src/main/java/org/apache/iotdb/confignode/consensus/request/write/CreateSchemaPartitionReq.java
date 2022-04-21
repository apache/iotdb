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
package org.apache.iotdb.confignode.consensus.request.write;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigRequest;
import org.apache.iotdb.confignode.consensus.request.ConfigRequestType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Create SchemaPartition by assignedSchemaPartition */
public class CreateSchemaPartitionReq extends ConfigRequest {

  private Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition;

  public CreateSchemaPartitionReq() {
    super(ConfigRequestType.CreateSchemaPartition);
  }

  public Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> getAssignedSchemaPartition() {
    return assignedSchemaPartition;
  }

  public void setAssignedSchemaPartition(
      Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> assignedSchemaPartition) {
    this.assignedSchemaPartition = assignedSchemaPartition;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.CreateSchemaPartition.ordinal());

    buffer.putInt(assignedSchemaPartition.size());
    assignedSchemaPartition.forEach(
        (storageGroup, partitionSlots) -> {
          BasicStructureSerDeUtil.write(storageGroup, buffer);
          buffer.putInt(partitionSlots.size());
          partitionSlots.forEach(
              (seriesPartitionSlot, regionReplicaSet) -> {
                ThriftCommonsSerDeUtils.writeTSeriesPartitionSlot(seriesPartitionSlot, buffer);
                ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, buffer);
              });
        });
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    assignedSchemaPartition = new HashMap<>();

    int storageGroupNum = buffer.getInt();
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = BasicStructureSerDeUtil.readString(buffer);
      assignedSchemaPartition.put(storageGroup, new HashMap<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot =
            ThriftCommonsSerDeUtils.readTSeriesPartitionSlot(buffer);
        assignedSchemaPartition
            .get(storageGroup)
            .put(seriesPartitionSlot, ThriftCommonsSerDeUtils.readTRegionReplicaSet(buffer));
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateSchemaPartitionReq that = (CreateSchemaPartitionReq) o;
    return assignedSchemaPartition.equals(that.assignedSchemaPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedSchemaPartition);
  }
}
