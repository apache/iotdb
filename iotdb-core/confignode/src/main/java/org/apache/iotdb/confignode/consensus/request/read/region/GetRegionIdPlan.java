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

package org.apache.iotdb.confignode.consensus.request.read.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;

public class GetRegionIdPlan extends ConfigPhysicalReadPlan {

  private String database;

  private final TConsensusGroupType partitionType;

  private TTimePartitionSlot startTimeSlotId;

  private TTimePartitionSlot endTimeSlotId;

  private TSeriesPartitionSlot seriesSlotId;

  public GetRegionIdPlan(final TConsensusGroupType partitionType) {
    super(ConfigPhysicalPlanType.GetRegionId);
    this.partitionType = partitionType;
    this.database = "";
    this.seriesSlotId = new TSeriesPartitionSlot(-1);
    this.startTimeSlotId = new TTimePartitionSlot(-1);
    this.endTimeSlotId = new TTimePartitionSlot(-1);
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(final String database) {
    this.database = database;
  }

  public void setSeriesSlotId(final TSeriesPartitionSlot seriesSlotId) {
    this.seriesSlotId = seriesSlotId;
  }

  public TSeriesPartitionSlot getSeriesSlotId() {
    return seriesSlotId;
  }

  public TTimePartitionSlot getStartTimeSlotId() {
    return startTimeSlotId;
  }

  public void setStartTimeSlotId(final TTimePartitionSlot startTimeSlotId) {
    this.startTimeSlotId = startTimeSlotId;
  }

  public TTimePartitionSlot getEndTimeSlotId() {
    return endTimeSlotId;
  }

  public void setEndTimeSlotId(final TTimePartitionSlot endTimeSlotId) {
    this.endTimeSlotId = endTimeSlotId;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetRegionIdPlan that = (GetRegionIdPlan) o;
    return database.equals(that.database)
        && partitionType.equals(that.partitionType)
        && seriesSlotId.equals(that.seriesSlotId)
        && startTimeSlotId.equals(that.startTimeSlotId)
        && endTimeSlotId.equals(that.endTimeSlotId);
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(partitionType.ordinal());
    hashcode = hashcode * 31 + Objects.hash(database);
    hashcode = hashcode * 31 + seriesSlotId.hashCode();
    hashcode = hashcode * 31 + startTimeSlotId.hashCode();
    hashcode = hashcode * 31 + endTimeSlotId.hashCode();
    return hashcode;
  }
}
