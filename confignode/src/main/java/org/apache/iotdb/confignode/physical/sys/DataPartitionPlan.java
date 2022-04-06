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
import java.util.Map;

/**
 * query DataPartition or apply DataPartition by the specific storageGroup and
 * deviceGroupStartTimeMap.
 */
public class DataPartitionPlan extends PhysicalPlan {
  private String storageGroup;
  private Map<Integer, List<Integer>> deviceGroupStartTimeMap;

  public DataPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
  }

  public DataPartitionPlan(
      PhysicalPlanType physicalPlanType,
      String storageGroup,
      Map<Integer, List<Integer>> deviceGroupStartTimeMap) {
    this(physicalPlanType);
    this.storageGroup = storageGroup;
    this.deviceGroupStartTimeMap = deviceGroupStartTimeMap;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(String storageGroup) {
    this.storageGroup = storageGroup;
  }

  public Map<Integer, List<Integer>> getDeviceGroupIDs() {
    return deviceGroupStartTimeMap;
  }

  public void setDeviceGroupIDs(Map<Integer, List<Integer>> deviceGroupIDs) {
    this.deviceGroupStartTimeMap = deviceGroupIDs;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.QueryDataPartition.ordinal());
    SerializeDeserializeUtil.write(storageGroup, buffer);
    SerializeDeserializeUtil.writeIntMapLists(deviceGroupStartTimeMap, buffer);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    storageGroup = SerializeDeserializeUtil.readString(buffer);
    deviceGroupStartTimeMap = SerializeDeserializeUtil.readIntMapLists(buffer);
  }
}
