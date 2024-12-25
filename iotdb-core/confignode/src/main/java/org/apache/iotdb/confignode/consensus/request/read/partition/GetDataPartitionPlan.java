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
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Get or create DataPartition by the specific partitionSlotsMap. */
public class GetDataPartitionPlan extends ConfigPhysicalReadPlan {

  // Map<StorageGroup, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
  protected Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap;

  public GetDataPartitionPlan(final ConfigPhysicalPlanType configPhysicalPlanType) {
    super(configPhysicalPlanType);
  }

  public GetDataPartitionPlan(
      final Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap) {
    super(ConfigPhysicalPlanType.GetDataPartition);
    this.partitionSlotsMap = partitionSlotsMap;
  }

  public Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> getPartitionSlotsMap() {
    return partitionSlotsMap;
  }

  /**
   * Convert TDataPartitionReq to GetDataPartitionPlan.
   *
   * @param req TDataPartitionReq
   * @return GetDataPartitionPlan
   */
  public static GetDataPartitionPlan convertFromRpcTDataPartitionReq(final TDataPartitionReq req) {
    return new GetDataPartitionPlan(new ConcurrentHashMap<>(req.getPartitionSlotsMap()));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetDataPartitionPlan that = (GetDataPartitionPlan) o;
    return partitionSlotsMap.equals(that.partitionSlotsMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionSlotsMap);
  }
}
