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
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class GetRegionInfoListPlan extends ConfigPhysicalPlan {

  private TShowRegionReq showRegionReq;

  public GetRegionInfoListPlan() {
    super(ConfigPhysicalPlanType.GetRegionInfoList);
  }

  public GetRegionInfoListPlan(TShowRegionReq showRegionReq) {
    super(ConfigPhysicalPlanType.GetRegionInfoList);
    this.showRegionReq = showRegionReq;
  }

  public TShowRegionReq getShowRegionReq() {
    return showRegionReq;
  }

  public void setShowRegionReq(TShowRegionReq showRegionReq) {
    this.showRegionReq = showRegionReq;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    stream.writeBoolean(showRegionReq != null);
    if (showRegionReq != null) {
      boolean setConsensusGroupType = showRegionReq.isSetConsensusGroupType();
      stream.writeBoolean(setConsensusGroupType);
      if (setConsensusGroupType) {
        ReadWriteIOUtils.write(showRegionReq.getConsensusGroupType().ordinal(), stream);
      }
      boolean setStorageGroups = showRegionReq.isSetDatabases();
      stream.writeBoolean(setStorageGroups);
      if (setStorageGroups) {
        ReadWriteIOUtils.writeStringList(showRegionReq.getDatabases(), stream);
      }
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    if (ReadWriteIOUtils.readBool(buffer)) {
      this.showRegionReq = new TShowRegionReq();
      if (ReadWriteIOUtils.readBool(buffer)) {
        this.showRegionReq.setConsensusGroupType(
            TConsensusGroupType.values()[ReadWriteIOUtils.readInt(buffer)]);
      }
      if (ReadWriteIOUtils.readBool(buffer)) {
        this.showRegionReq.setDatabases(ReadWriteIOUtils.readStringList(buffer));
      }
    }
  }
}
