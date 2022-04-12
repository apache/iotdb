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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.physical.PhysicalPlanType;
import org.apache.iotdb.confignode.util.SerializeDeserializeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Get or create SchemaPartition by the specific partitionSlotsMap. */
public class GetOrCreateSchemaPartitionPlan extends PhysicalPlan {

  // Map<StorageGroup, List<SeriesPartitionSlot>>
  // Return all SchemaPartitions when the partitionSlotsMap is empty
  // Return all exists SchemaPartitions in one StorageGroup when the SeriesPartitionSlot is empty
  private Map<String, List<SeriesPartitionSlot>> partitionSlotsMap;

  public GetOrCreateSchemaPartitionPlan(PhysicalPlanType physicalPlanType) {
    super(physicalPlanType);
    partitionSlotsMap = new HashMap<>();
  }

  public void setPartitionSlotsMap(Map<String, List<SeriesPartitionSlot>> partitionSlotsMap) {
    this.partitionSlotsMap = partitionSlotsMap;
  }

  @Override
  protected void serializeImpl(ByteBuffer buffer) {
    buffer.putInt(PhysicalPlanType.GetDataPartition.ordinal());
    // TODO:
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    // TODO:
  }
}
