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
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetRegionIdPlan extends ConfigPhysicalPlan {

  private String storageGroup;

  private TConsensusGroupType partitionType;

  private TSeriesPartitionSlot seriesSlotId;

  private TTimePartitionSlot timeSlotId;

  public GetRegionIdPlan() {
    super(ConfigPhysicalPlanType.GetRegionId);
  }

  public GetRegionIdPlan(
      String storageGroup,
      TConsensusGroupType partitionType,
      TSeriesPartitionSlot seriesSlotId,
      TTimePartitionSlot timeSlotId) {
    this();
    this.partitionType = partitionType;
    this.storageGroup = storageGroup;
    this.timeSlotId = timeSlotId;
    this.seriesSlotId = seriesSlotId;
  }

  public String getStorageGroup() {
    return storageGroup;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  public TSeriesPartitionSlot getSeriesSlotId() {
    return seriesSlotId;
  }

  public TTimePartitionSlot getTimeSlotId() {
    return timeSlotId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(storageGroup, stream);
    stream.writeInt(partitionType.ordinal());
    ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesSlotId, stream);
    ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(timeSlotId, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.storageGroup = ReadWriteIOUtils.readString(buffer);
    this.partitionType = TConsensusGroupType.findByValue(buffer.getInt());
    this.seriesSlotId = ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
    this.timeSlotId = ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetRegionIdPlan that = (GetRegionIdPlan) o;
    return storageGroup.equals(that.storageGroup)
        && seriesSlotId.equals(that.seriesSlotId)
        && timeSlotId.equals(that.timeSlotId);
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(storageGroup);
    hashcode = hashcode * 31 + seriesSlotId.hashCode();
    hashcode = hashcode * 31 + timeSlotId.hashCode();
    return hashcode;
  }
}
