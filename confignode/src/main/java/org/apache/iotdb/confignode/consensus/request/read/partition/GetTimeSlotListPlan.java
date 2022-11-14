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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetTimeSlotListPlan extends ConfigPhysicalPlan {

  private String storageGroup;

  private TSeriesPartitionSlot seriesSlotId;

  private long startTime;

  private long endTime;

  public GetTimeSlotListPlan() {
    super(ConfigPhysicalPlanType.GetTimeSlotList);
  }

  public GetTimeSlotListPlan(
      String storageGroup, TSeriesPartitionSlot seriesSlotId, long startTime, long endTime) {
    this();
    this.storageGroup = storageGroup;
    this.seriesSlotId = seriesSlotId;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public String getStorageGroup() {
    return storageGroup;
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
    ReadWriteIOUtils.write(storageGroup, stream);
    ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesSlotId, stream);
    stream.writeLong(startTime);
    stream.writeLong(endTime);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.storageGroup = ReadWriteIOUtils.readString(buffer);
    this.seriesSlotId = ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
    this.startTime = buffer.getLong();
    this.endTime = buffer.getLong();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetTimeSlotListPlan that = (GetTimeSlotListPlan) o;
    return storageGroup.equals(that.storageGroup)
        && seriesSlotId.equals(that.seriesSlotId)
        && startTime == that.startTime
        && endTime == that.endTime;
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(storageGroup);
    hashcode = hashcode * 31 + seriesSlotId.hashCode();
    hashcode = hashcode * 31 + (int) startTime;
    hashcode = hashcode * 31 + (int) endTime;
    return hashcode;
  }
}
