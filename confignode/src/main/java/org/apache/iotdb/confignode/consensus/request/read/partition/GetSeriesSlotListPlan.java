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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GetSeriesSlotListPlan extends ConfigPhysicalPlan {

  private String database;

  private TConsensusGroupType partitionType;

  public GetSeriesSlotListPlan() {
    super(ConfigPhysicalPlanType.GetSeriesSlotList);
  }

  public GetSeriesSlotListPlan(String database, TConsensusGroupType partitionType) {
    this();
    this.database = database;
    this.partitionType = partitionType;
  }

  public String getDatabase() {
    return database;
  }

  public TConsensusGroupType getPartitionType() {
    return partitionType;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(database, stream);
    stream.writeInt(partitionType.ordinal());
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.database = ReadWriteIOUtils.readString(buffer);
    this.partitionType = TConsensusGroupType.findByValue(buffer.getInt());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GetSeriesSlotListPlan that = (GetSeriesSlotListPlan) o;
    return database.equals(that.database) && partitionType.equals(that.partitionType);
  }

  @Override
  public int hashCode() {
    int hashcode = 1;
    hashcode = hashcode * 31 + Objects.hash(database);
    hashcode = hashcode * 31 + Objects.hash(partitionType.ordinal());
    return hashcode;
  }
}
