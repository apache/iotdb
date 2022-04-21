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
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
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

/** Create DataPartition by assignedDataPartition */
public class CreateDataPartitionReq extends ConfigRequest {

  private Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      assignedDataPartition;

  public CreateDataPartitionReq() {
    super(ConfigRequestType.CreateDataPartition);
  }

  public Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
      getAssignedDataPartition() {
    return assignedDataPartition;
  }

  public void setAssignedDataPartition(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          assignedDataPartition) {
    this.assignedDataPartition = assignedDataPartition;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(ConfigRequestType.CreateDataPartition.ordinal());

    buffer.putInt(assignedDataPartition.size());
    for (Map.Entry<
            String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
        seriesPartitionTimePartitionEntry : assignedDataPartition.entrySet()) {
      BasicStructureSerDeUtil.write(seriesPartitionTimePartitionEntry.getKey(), buffer);
      buffer.putInt(seriesPartitionTimePartitionEntry.getValue().size());
      for (Map.Entry<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>
          timePartitionEntry : seriesPartitionTimePartitionEntry.getValue().entrySet()) {
        ThriftCommonsSerDeUtils.writeTSeriesPartitionSlot(timePartitionEntry.getKey(), buffer);
        buffer.putInt(timePartitionEntry.getValue().size());
        for (Map.Entry<TTimePartitionSlot, List<TRegionReplicaSet>> regionReplicaSetEntry :
            timePartitionEntry.getValue().entrySet()) {
          ThriftCommonsSerDeUtils.writeTTimePartitionSlot(regionReplicaSetEntry.getKey(), buffer);
          buffer.putInt(regionReplicaSetEntry.getValue().size());
          for (TRegionReplicaSet regionReplicaSet : regionReplicaSetEntry.getValue()) {
            ThriftCommonsSerDeUtils.writeTRegionReplicaSet(regionReplicaSet, buffer);
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
      String storageGroupName = BasicStructureSerDeUtil.readString(buffer);
      assignedDataPartition.put(storageGroupName, new HashMap<>());
      int seriesPartitionSlotNum = buffer.getInt();
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot =
            ThriftCommonsSerDeUtils.readTSeriesPartitionSlot(buffer);
        assignedDataPartition.get(storageGroupName).put(seriesPartitionSlot, new HashMap<>());
        int timePartitionSlotNum = buffer.getInt();
        for (int k = 0; k < timePartitionSlotNum; k++) {
          TTimePartitionSlot timePartitionSlot =
              ThriftCommonsSerDeUtils.readTTimePartitionSlot(buffer);
          assignedDataPartition
              .get(storageGroupName)
              .get(seriesPartitionSlot)
              .put(timePartitionSlot, new ArrayList<>());
          int regionReplicaSetNum = buffer.getInt();
          for (int l = 0; l < regionReplicaSetNum; l++) {
            assignedDataPartition
                .get(storageGroupName)
                .get(seriesPartitionSlot)
                .get(timePartitionSlot)
                .add(ThriftCommonsSerDeUtils.readTRegionReplicaSet(buffer));
          }
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateDataPartitionReq that = (CreateDataPartitionReq) o;
    return assignedDataPartition.equals(that.assignedDataPartition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedDataPartition);
  }
}
