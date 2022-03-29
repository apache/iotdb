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
package org.apache.iotdb.confignode.physical.sys;

import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.nio.ByteBuffer;
import java.util.List;

/** Get DataNodeInfo by the specific DataNode's id. And return all when dataNodeID is set to -1. */
public class SchemaPartitionPlan extends PhysicalPlan {
  private String storageGroup;
  private List<Integer> deviceGroupIDs;

  public SchemaPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public SchemaPartitionPlan(
      PhysicalPlanType physicalPlanType, String storageGroup, List<Integer> deviceGroupIDs) {
    this(physicalPlanType);
    this.storageGroup = storageGroup;
    this.deviceGroupIDs = deviceGroupIDs;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.QueryDataPartition.ordinal());
    SerializeDeserializeUtil.write(storageGroup, buffer);
    buffer.putInt(deviceGroupIDs.size());
    deviceGroupIDs.forEach(id -> SerializeDeserializeUtil.write(id, buffer));
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    storageGroup = SerializeDeserializeUtil.readString(buffer);
    int idSize = SerializeDeserializeUtil.readInt(buffer);
    for (int i = 0; i < idSize; i++) {
      deviceGroupIDs.add(SerializeDeserializeUtil.readInt(buffer));
    }
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public List<Integer> getDeviceGroupIDs() {
    return deviceGroupIDs;
  }
}
