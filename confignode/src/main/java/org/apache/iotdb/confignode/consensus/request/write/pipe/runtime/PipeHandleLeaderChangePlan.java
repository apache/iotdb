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

package org.apache.iotdb.confignode.consensus.request.write.pipe.runtime;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PipeHandleLeaderChangePlan extends ConfigPhysicalPlan {

  private Map<TConsensusGroupId, Integer> consensusGroupId2NewDataRegionLeaderIdMap =
      new HashMap<>();

  public PipeHandleLeaderChangePlan() {
    super(ConfigPhysicalPlanType.PipeHandleLeaderChange);
  }

  public PipeHandleLeaderChangePlan(
      Map<TConsensusGroupId, Integer> consensusGroupId2NewDataRegionLeaderIdMap) {
    super(ConfigPhysicalPlanType.PipeHandleLeaderChange);
    this.consensusGroupId2NewDataRegionLeaderIdMap = consensusGroupId2NewDataRegionLeaderIdMap;
  }

  public Map<TConsensusGroupId, Integer> getConsensusGroupId2NewDataRegionLeaderIdMap() {
    return consensusGroupId2NewDataRegionLeaderIdMap;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(consensusGroupId2NewDataRegionLeaderIdMap.size());
    for (Map.Entry<TConsensusGroupId, Integer> entry :
        consensusGroupId2NewDataRegionLeaderIdMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey().getId(), stream);
      ReadWriteIOUtils.write(entry.getValue(), stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    for (int i = 0; i < size; ++i) {
      consensusGroupId2NewDataRegionLeaderIdMap.put(
          new TConsensusGroupId(TConsensusGroupType.DataRegion, ReadWriteIOUtils.readInt(buffer)),
          ReadWriteIOUtils.readInt(buffer));
    }
  }
}
