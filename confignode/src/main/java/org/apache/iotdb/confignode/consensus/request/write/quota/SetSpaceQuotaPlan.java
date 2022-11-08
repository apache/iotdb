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

package org.apache.iotdb.confignode.consensus.request.write.quota;

import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

public class SetSpaceQuotaPlan extends ConfigPhysicalPlan {

  private List<String> prefixPathList;
  private TSpaceQuota spaceLimit;

  public SetSpaceQuotaPlan() {
    super(ConfigPhysicalPlanType.SET_SPACE_QUOTA);
  }

  public SetSpaceQuotaPlan(List<String> prefixPathList, TSpaceQuota spaceLimit) {
    super(ConfigPhysicalPlanType.SET_SPACE_QUOTA);
    this.prefixPathList = prefixPathList;
    this.spaceLimit = spaceLimit;
  }

  public List<String> getPrefixPathList() {
    return prefixPathList;
  }

  public void setPrefixPathList(List<String> prefixPathList) {
    this.prefixPathList = prefixPathList;
  }

  public TSpaceQuota getSpaceLimit() {
    return spaceLimit;
  }

  public void setSpaceLimit(TSpaceQuota spaceLimit) {
    this.spaceLimit = spaceLimit;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    BasicStructureSerDeUtil.write(prefixPathList, stream);
    BasicStructureSerDeUtil.write(spaceLimit.getDeviceNum(), stream);
    BasicStructureSerDeUtil.write(spaceLimit.getTimeserieNum(), stream);
    BasicStructureSerDeUtil.write(spaceLimit.getDisk(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    List<String> prefixPathList = BasicStructureSerDeUtil.readStringList(buffer);
    int deviceNum = BasicStructureSerDeUtil.readInt(buffer);
    int timeserieNum = BasicStructureSerDeUtil.readInt(buffer);
    long disk = BasicStructureSerDeUtil.readLong(buffer);
    this.prefixPathList = prefixPathList;
    TSpaceQuota spaceLimit = new TSpaceQuota();
    spaceLimit.setDeviceNum(deviceNum);
    spaceLimit.setTimeserieNum(timeserieNum);
    spaceLimit.setDisk(disk);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SetSpaceQuotaPlan that = (SetSpaceQuotaPlan) o;
    return Objects.equals(prefixPathList, that.prefixPathList)
        && Objects.equals(spaceLimit, that.spaceLimit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), prefixPathList, spaceLimit);
  }
}
