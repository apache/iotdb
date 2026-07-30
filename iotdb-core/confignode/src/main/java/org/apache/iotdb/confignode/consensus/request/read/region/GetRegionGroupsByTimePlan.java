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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetRegionGroupsByTimePlan extends ConfigPhysicalReadPlan {

  private String database;

  private TTimePartitionSlot startTimeSlot;

  private TTimePartitionSlot endTimeSlot;

  public GetRegionGroupsByTimePlan() {
    super(ConfigPhysicalPlanType.GetRegionGroupsByTime);
  }

  public GetRegionGroupsByTimePlan(
      final String database, final long startTime, final long endTime) {
    super(ConfigPhysicalPlanType.GetRegionGroupsByTime);
    this.database = database;
    this.startTimeSlot = TimePartitionUtils.getTimePartitionSlot(startTime);
    this.endTimeSlot = TimePartitionUtils.getTimePartitionSlot(endTime);
  }

  public String getDatabase() {
    return database;
  }

  public TTimePartitionSlot getStartTimeSlot() {
    return startTimeSlot;
  }

  public TTimePartitionSlot getEndTimeSlot() {
    return endTimeSlot;
  }

  @Override
  protected void serializeImpl(final DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    BasicStructureSerDeUtil.write(database, stream);
    stream.writeLong(startTimeSlot.getStartTime());
    stream.writeLong(endTimeSlot.getStartTime());
  }

  @Override
  protected void deserializeImpl(final ByteBuffer buffer) throws IOException {
    database = BasicStructureSerDeUtil.readString(buffer);
    startTimeSlot = new TTimePartitionSlot(buffer.getLong());
    endTimeSlot = new TTimePartitionSlot(buffer.getLong());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GetRegionGroupsByTimePlan that = (GetRegionGroupsByTimePlan) o;
    return Objects.equals(database, that.database)
        && Objects.equals(startTimeSlot, that.startTimeSlot)
        && Objects.equals(endTimeSlot, that.endTimeSlot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database, startTimeSlot, endTimeSlot);
  }
}
