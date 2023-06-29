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

import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

public class SetThrottleQuotaPlan extends ConfigPhysicalPlan {

  private String userName;
  private TThrottleQuota throttleQuota;

  public SetThrottleQuotaPlan() {
    super(ConfigPhysicalPlanType.setThrottleQuota);
  }

  public SetThrottleQuotaPlan(String userName, TThrottleQuota throttleQuota) {
    super(ConfigPhysicalPlanType.setThrottleQuota);
    this.userName = userName;
    this.throttleQuota = throttleQuota;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public TThrottleQuota getThrottleQuota() {
    return throttleQuota;
  }

  public void setThrottleQuota(TThrottleQuota throttleQuota) {
    this.throttleQuota = throttleQuota;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    BasicStructureSerDeUtil.write(userName, stream);
    BasicStructureSerDeUtil.write(throttleQuota.getThrottleLimit().size(), stream);
    for (Map.Entry<ThrottleType, TTimedQuota> entry : throttleQuota.getThrottleLimit().entrySet()) {
      BasicStructureSerDeUtil.write(entry.getKey().name(), stream);
      BasicStructureSerDeUtil.write(entry.getValue().getTimeUnit(), stream);
      BasicStructureSerDeUtil.write(entry.getValue().getSoftLimit(), stream);
    }
    BasicStructureSerDeUtil.write(throttleQuota.getMemLimit(), stream);
    BasicStructureSerDeUtil.write(throttleQuota.getCpuLimit(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    this.userName = BasicStructureSerDeUtil.readString(buffer);
    Map<ThrottleType, TTimedQuota> throttleLimit = new EnumMap<>(ThrottleType.class);
    int size = BasicStructureSerDeUtil.readInt(buffer);
    for (int i = 0; i < size; i++) {
      ThrottleType throttleType = ThrottleType.valueOf(BasicStructureSerDeUtil.readString(buffer));
      long timeUnit = BasicStructureSerDeUtil.readLong(buffer);
      long softLimit = BasicStructureSerDeUtil.readLong(buffer);
      throttleLimit.put(throttleType, new TTimedQuota(timeUnit, softLimit));
    }
    TThrottleQuota tmpThrottleQuota = new TThrottleQuota();
    tmpThrottleQuota.setThrottleLimit(throttleLimit);
    tmpThrottleQuota.setMemLimit(BasicStructureSerDeUtil.readLong(buffer));
    tmpThrottleQuota.setCpuLimit(BasicStructureSerDeUtil.readInt(buffer));
    this.throttleQuota = tmpThrottleQuota;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SetThrottleQuotaPlan that = (SetThrottleQuotaPlan) o;
    return Objects.equals(userName, that.userName)
        && Objects.equals(throttleQuota, that.throttleQuota);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), userName, throttleQuota);
  }
}
