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

package org.apache.iotdb.confignode.consensus.request.read;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetRegionInfoListPlan extends ConfigPhysicalPlan {

  private boolean filterByStorageGroup = false;
  private TConsensusGroupType regionType;
  private List<String> storageGroups = new ArrayList<>();

  public GetRegionInfoListPlan() {
    super(ConfigPhysicalPlanType.GetRegionInfoList);
  }

  public GetRegionInfoListPlan(TConsensusGroupType regionType) {
    super(ConfigPhysicalPlanType.GetRegionInfoList);
    this.regionType = regionType;
  }

  public TConsensusGroupType getRegionType() {
    return regionType;
  }

  public void setRegionType(TConsensusGroupType regionType) {
    this.regionType = regionType;
  }

  public List<String> getStorageGroups() {
    return storageGroups;
  }

  public void setStorageGroups(List<String> storageGroups) {
    this.storageGroups = storageGroups;
  }

  public boolean isFilterByStorageGroup() {
    return filterByStorageGroup;
  }

  public void setFilterByStorageGroup(boolean filterByStorageGroup) {
    this.filterByStorageGroup = filterByStorageGroup;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeInt(getType().ordinal());
    ReadWriteIOUtils.write(regionType.ordinal(), stream);
    ReadWriteIOUtils.write(filterByStorageGroup, stream);
    ReadWriteIOUtils.writeStringList(storageGroups, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    regionType = TConsensusGroupType.values()[ReadWriteIOUtils.readInt(buffer)];
    filterByStorageGroup = ReadWriteIOUtils.readBool(buffer);
    storageGroups = ReadWriteIOUtils.readStringList(buffer);
  }
}
