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

package org.apache.iotdb.confignode.consensus.request.write.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class PollSpecificRegionMaintainTaskPlan extends ConfigPhysicalPlan {

  private Set<TConsensusGroupId> regionIdSet;

  public PollSpecificRegionMaintainTaskPlan() {
    super(ConfigPhysicalPlanType.PollSpecificRegionMaintainTask);
  }

  public PollSpecificRegionMaintainTaskPlan(Set<TConsensusGroupId> regionIdSet) {
    super(ConfigPhysicalPlanType.PollSpecificRegionMaintainTask);
    this.regionIdSet = regionIdSet;
  }

  public Set<TConsensusGroupId> getRegionIdSet() {
    return regionIdSet;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(regionIdSet.size());
    for (TConsensusGroupId regionId : regionIdSet) {
      stream.writeInt(regionId.getType().getValue());
      stream.writeInt(regionId.getId());
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    regionIdSet = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      regionIdSet.add(
          new TConsensusGroupId(TConsensusGroupType.findByValue(buffer.getInt()), buffer.getInt()));
    }
  }
}
