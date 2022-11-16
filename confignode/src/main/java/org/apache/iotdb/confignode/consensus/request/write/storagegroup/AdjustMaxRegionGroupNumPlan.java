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
package org.apache.iotdb.confignode.consensus.request.write.storagegroup;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AdjustMaxRegionGroupNumPlan extends ConfigPhysicalPlan {

  // Map<StorageGroupName, Pair<maxSchemaRegionGroupNum, maxDataRegionGroupNum>>
  public final Map<String, Pair<Integer, Integer>> maxRegionGroupNumMap;

  public AdjustMaxRegionGroupNumPlan() {
    super(ConfigPhysicalPlanType.AdjustMaxRegionGroupNum);
    this.maxRegionGroupNumMap = new HashMap<>();
  }

  public void putEntry(String storageGroup, Pair<Integer, Integer> maxRegionGroupNum) {
    maxRegionGroupNumMap.put(storageGroup, maxRegionGroupNum);
  }

  public Map<String, Pair<Integer, Integer>> getMaxRegionGroupNumMap() {
    return maxRegionGroupNumMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);

    ReadWriteIOUtils.write(maxRegionGroupNumMap.size(), stream);
    for (Map.Entry<String, Pair<Integer, Integer>> maxRegionGroupNumEntry :
        maxRegionGroupNumMap.entrySet()) {
      ReadWriteIOUtils.write(maxRegionGroupNumEntry.getKey(), stream);
      ReadWriteIOUtils.write(maxRegionGroupNumEntry.getValue().getLeft(), stream);
      ReadWriteIOUtils.write(maxRegionGroupNumEntry.getValue().getRight(), stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int storageGroupNum = buffer.getInt();

    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = ReadWriteIOUtils.readString(buffer);
      int maxSchemaRegionGroupNum = buffer.getInt();
      int maxDataRegionGroupNum = buffer.getInt();
      maxRegionGroupNumMap.put(
          storageGroup, new Pair<>(maxSchemaRegionGroupNum, maxDataRegionGroupNum));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AdjustMaxRegionGroupNumPlan that = (AdjustMaxRegionGroupNumPlan) o;
    return maxRegionGroupNumMap.equals(that.maxRegionGroupNumMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(maxRegionGroupNumMap);
  }
}
