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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CreateRegionsPlan extends PhysicalPlan {

  private String storageGroup;

  private final List<RegionReplicaSet> regionReplicaSets;

  public CreateRegionsPlan() {
    super(PhysicalPlanType.CreateRegions);
    this.regionReplicaSets = new ArrayList<>();
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(String storageGroup) {
    this.storageGroup = storageGroup;
  }

  public void addRegion(RegionReplicaSet regionReplicaSet) {
    this.regionReplicaSets.add(regionReplicaSet);
  }

  public List<RegionReplicaSet> getRegionReplicaSets() {
    return regionReplicaSets;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.CreateRegions.ordinal());

    SerializeDeserializeUtil.write(storageGroup, buffer);

    buffer.putInt(regionReplicaSets.size());
    for (RegionReplicaSet regionReplicaSet : regionReplicaSets) {
      regionReplicaSet.serializeImpl(buffer);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    storageGroup = SerializeDeserializeUtil.readString(buffer);

    int length = buffer.getInt();
    for (int i = 0; i < length; i++) {
      RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
      regionReplicaSet.deserializeImpl(buffer);
      regionReplicaSets.add(regionReplicaSet);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CreateRegionsPlan that = (CreateRegionsPlan) o;
    return storageGroup.equals(that.storageGroup)
        && regionReplicaSets.equals(that.regionReplicaSets);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storageGroup, regionReplicaSets);
  }
}
