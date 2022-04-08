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

import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.confignode.partition.StorageGroupSchema;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SetStorageGroupPlan extends PhysicalPlan {

  private StorageGroupSchema schema;

  private List<RegionReplicaSet> regionReplicaSets;

  public SetStorageGroupPlan() {
    super(PhysicalPlanType.SetStorageGroup);
    this.schema = new StorageGroupSchema();
  }

  public SetStorageGroupPlan(StorageGroupSchema schema) {
    this();
    this.schema = schema;
  }

  public StorageGroupSchema getSchema() {
    return schema;
  }

  public void setSchema(StorageGroupSchema schema) {
    this.schema = schema;
  }

  public List<RegionReplicaSet> getRegionReplicaSets() {
    return regionReplicaSets;
  }

  public void addRegion(RegionReplicaSet regionReplicaSet) {
    if (regionReplicaSets == null) {
      regionReplicaSets = new ArrayList<>();
    }
    regionReplicaSets.add(regionReplicaSet);
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.SetStorageGroup.ordinal());
    schema.serialize(buffer);

    buffer.putInt(regionReplicaSets.size());
    for (RegionReplicaSet regionReplicaSet : regionReplicaSets) {
      regionReplicaSet.serializeImpl(buffer);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    schema.deserialize(buffer);

    int length = buffer.getInt();
    regionReplicaSets = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      RegionReplicaSet regionReplicaSet = new RegionReplicaSet();
      regionReplicaSet.deserializeImpl(buffer);
      regionReplicaSets.add(regionReplicaSet);
    }
  }
}
