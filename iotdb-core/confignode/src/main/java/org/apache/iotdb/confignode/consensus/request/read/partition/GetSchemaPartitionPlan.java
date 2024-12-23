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

package org.apache.iotdb.confignode.consensus.request.read.partition;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Get or create SchemaPartition by the specific partitionSlotsMap. */
public class GetSchemaPartitionPlan extends ConfigPhysicalReadPlan {

  // Map<StorageGroup, List<SeriesPartitionSlot>>
  // Get all SchemaPartitions when the partitionSlotsMap is empty
  // Get all exists SchemaPartitions in one StorageGroup when the SeriesPartitionSlot is empty
  protected Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap;

  public GetSchemaPartitionPlan(final ConfigPhysicalPlanType configPhysicalPlanType) {
    super(configPhysicalPlanType);
  }

  public GetSchemaPartitionPlan(final Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap) {
    super(ConfigPhysicalPlanType.GetSchemaPartition);
    this.partitionSlotsMap = partitionSlotsMap;
  }

  public Map<String, List<TSeriesPartitionSlot>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetSchemaPartitionPlan that = (GetSchemaPartitionPlan) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
