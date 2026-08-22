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

import org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange;
import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.utils.BasicStructureSerDeUtil;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SetUserResourceQuotaPlan extends ConfigPhysicalPlan {

  private String userName;
  private TUserResourceQuota userResourceQuota;

  public SetUserResourceQuotaPlan() {
    super(ConfigPhysicalPlanType.setUserResourceQuota);
  }

  public SetUserResourceQuotaPlan(String userName, TUserResourceQuota userResourceQuota) {
    super(ConfigPhysicalPlanType.setUserResourceQuota);
    this.userName = userName;
    this.userResourceQuota = userResourceQuota;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public TUserResourceQuota getUserResourceQuota() {
    return userResourceQuota;
  }

  public void setUserResourceQuota(TUserResourceQuota userResourceQuota) {
    this.userResourceQuota = userResourceQuota;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    BasicStructureSerDeUtil.write(userName, stream);
    serializeQuota(userResourceQuota, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    userName = BasicStructureSerDeUtil.readString(buffer);
    userResourceQuota = deserializeQuota(buffer);
  }

  static void serializeQuota(TUserResourceQuota quota, DataOutputStream stream) throws IOException {
    writeRangeMap(quota.getReadQuota(), stream);
    writeRangeMap(quota.getWriteQuota(), stream);
    Map<ThrottleType, TTimedQuota> throttleLimit =
        quota.isSetThrottleLimit() ? quota.getThrottleLimit() : new HashMap<>();
    BasicStructureSerDeUtil.write(throttleLimit.size(), stream);
    for (Map.Entry<ThrottleType, TTimedQuota> entry : throttleLimit.entrySet()) {
      BasicStructureSerDeUtil.write(entry.getKey().name(), stream);
      BasicStructureSerDeUtil.write(entry.getValue().getTimeUnit(), stream);
      BasicStructureSerDeUtil.write(entry.getValue().getSoftLimit(), stream);
    }
  }

  static TUserResourceQuota deserializeQuota(ByteBuffer buffer) throws IOException {
    TUserResourceQuota quota = new TUserResourceQuota();
    quota.setReadQuota(readRangeMap(buffer));
    quota.setWriteQuota(readRangeMap(buffer));
    int throttleSize = BasicStructureSerDeUtil.readInt(buffer);
    Map<ThrottleType, TTimedQuota> throttleLimit = new HashMap<>();
    for (int i = 0; i < throttleSize; i++) {
      ThrottleType type = ThrottleType.valueOf(BasicStructureSerDeUtil.readString(buffer));
      long timeUnit = BasicStructureSerDeUtil.readLong(buffer);
      long softLimit = BasicStructureSerDeUtil.readLong(buffer);
      throttleLimit.put(type, new TTimedQuota(timeUnit, softLimit));
    }
    quota.setThrottleLimit(throttleLimit);
    return quota;
  }

  private static void writeRangeMap(
      Map<TResourceType, TResourceQuotaRange> rangeMap, DataOutputStream stream)
      throws IOException {
    Map<TResourceType, TResourceQuotaRange> map =
        rangeMap == null ? new EnumMap<>(TResourceType.class) : rangeMap;
    BasicStructureSerDeUtil.write(map.size(), stream);
    for (Map.Entry<TResourceType, TResourceQuotaRange> entry : map.entrySet()) {
      BasicStructureSerDeUtil.write(entry.getKey().name(), stream);
      BasicStructureSerDeUtil.write(entry.getValue().getMinValue(), stream);
      BasicStructureSerDeUtil.write(entry.getValue().getMaxValue(), stream);
    }
  }

  private static Map<TResourceType, TResourceQuotaRange> readRangeMap(ByteBuffer buffer)
      throws IOException {
    int size = BasicStructureSerDeUtil.readInt(buffer);
    Map<TResourceType, TResourceQuotaRange> map = new EnumMap<>(TResourceType.class);
    for (int i = 0; i < size; i++) {
      TResourceType type = TResourceType.valueOf(BasicStructureSerDeUtil.readString(buffer));
      long min = BasicStructureSerDeUtil.readLong(buffer);
      long max = BasicStructureSerDeUtil.readLong(buffer);
      map.put(type, new TResourceQuotaRange(min, max));
    }
    return map;
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
    SetUserResourceQuotaPlan that = (SetUserResourceQuotaPlan) o;
    return Objects.equals(userName, that.userName)
        && Objects.equals(userResourceQuota, that.userResourceQuota);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), userName, userResourceQuota);
  }
}
