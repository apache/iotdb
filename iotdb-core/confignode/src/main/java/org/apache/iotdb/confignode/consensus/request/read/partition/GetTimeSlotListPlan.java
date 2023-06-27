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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetTimeSlotListPlan extends ConfigPhysicalPlan {

  private String database;

  private TSeriesPartitionSlot seriesSlotId;

  private TConsensusGroupId regionId;

  private long startTime;

  private long endTime;

  public GetTimeSlotListPlan() {
    super(ConfigPhysicalPlanType.GetTimeSlotList);
  }

  public GetTimeSlotListPlan(long startTime, long endTime) {
    this();
    this.startTime = startTime;
    this.endTime = endTime;
    this.database = "";
    this.seriesSlotId = new TSeriesPartitionSlot(-1);
    this.regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, -1);
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  public void setRegionId(TConsensusGroupId regionId) {
    this.regionId = regionId;
  }

  public TConsensusGroupId getRegionId() {
    return regionId;
  }

  public void setSeriesSlotId(TSeriesPartitionSlot seriesSlotId) {
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
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(database, stream);
    ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesSlotId, stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    stream.writeLong(startTime);
    stream.writeLong(endTime);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.database = ReadWriteIOUtils.readString(buffer);
    this.seriesSlotId = ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
    this.regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(buffer);
    this.startTime = buffer.getLong();
    this.endTime = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTimeSlotListPlan that = (GetTimeSlotListPlan) o;
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
