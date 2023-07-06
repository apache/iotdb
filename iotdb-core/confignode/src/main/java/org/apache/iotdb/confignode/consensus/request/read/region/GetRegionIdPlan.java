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

  private String database;

  private TConsensusGroupType partitionType;

  private TTimePartitionSlot timeSlotId;

  private TSeriesPartitionSlot seriesSlotId;

  public GetRegionIdPlan() {
    super(ConfigPhysicalPlanType.GetRegionId);
  }

  public GetRegionIdPlan(TConsensusGroupType partitionType) {
    this();
    this.partitionType = partitionType;
    this.database = "";
    this.seriesSlotId = new TSeriesPartitionSlot(-1);
    this.timeSlotId = new TTimePartitionSlot(-1);
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setSeriesSlotId(TSeriesPartitionSlot seriesSlotId) {
    this.seriesSlotId = seriesSlotId;
  }

  public TSeriesPartitionSlot getSeriesSlotId() {
    return seriesSlotId;
  }

  public void setTimeSlotId(TTimePartitionSlot timeSlotId) {
    this.timeSlotId = timeSlotId;
  }

  public TTimePartitionSlot getTimeSlotId() {
    return timeSlotId;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeInt(partitionType.ordinal());
    ReadWriteIOUtils.write(database, stream);
    ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesSlotId, stream);
    ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(timeSlotId, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.partitionType = TConsensusGroupType.findByValue(buffer.getInt());
    this.database = ReadWriteIOUtils.readString(buffer);
    this.seriesSlotId = ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(buffer);
    this.timeSlotId = ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetRegionIdPlan that = (GetRegionIdPlan) o;
    return database.equals(that.database)
        && partitionType.equals(that.partitionType)
        && seriesSlotId.equals(that.seriesSlotId)
        && timeSlotId.equals(that.timeSlotId);
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(partitionType.ordinal());
    hashcode = hashcode * 31 + Objects.hash(database);
    hashcode = hashcode * 31 + seriesSlotId.hashCode();
    hashcode = hashcode * 31 + timeSlotId.hashCode();
    return hashcode;
  }
}
