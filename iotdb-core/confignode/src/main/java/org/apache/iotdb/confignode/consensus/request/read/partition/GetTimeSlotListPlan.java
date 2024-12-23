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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;

public class GetTimeSlotListPlan extends ConfigPhysicalReadPlan {

  private String database;

  private TSeriesPartitionSlot seriesSlotId;

  private TConsensusGroupId regionId;

  private final long startTime;

  private final long endTime;

  public GetTimeSlotListPlan(final long startTime, final long endTime) {
    super(ConfigPhysicalPlanType.GetTimeSlotList);
    this.startTime = startTime;
    this.endTime = endTime;
    this.database = "";
    this.seriesSlotId = new TSeriesPartitionSlot(-1);
    this.regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, -1);
  }

  public void setDatabase(final String database) {
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  public void setRegionId(final TConsensusGroupId regionId) {
    this.regionId = regionId;
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public void setSeriesSlotId(final TSeriesPartitionSlot seriesSlotId) {
    this.seriesSlotId = seriesSlotId;
  }

  public TSeriesPartitionSlot getSeriesSlotId() {
    return seriesSlotId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetTimeSlotListPlan that = (GetTimeSlotListPlan) o;
    return database.equals(that.database)
        && seriesSlotId.equals(that.seriesSlotId)
        && regionId.equals(that.regionId)
        && startTime == that.startTime
        && endTime == that.endTime;
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(database);
    hashcode = hashcode * 31 + seriesSlotId.hashCode();
    hashcode = hashcode * 31 + regionId.hashCode();
    hashcode = hashcode * 31 + (int) startTime;
    hashcode = hashcode * 31 + (int) endTime;
    return hashcode;
  }
}
