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

package org.apache.iotdb.commons.quota;

import org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange;
import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

public final class UserResourceQuotaConverter {

  private UserResourceQuotaConverter() {}

  public static UserResourceQuota fromThrift(TUserResourceQuota thrift) {
    UserResourceQuota quota = new UserResourceQuota();
    if (thrift == null) {
      return quota;
    }
    if (thrift.isSetReadQuota()) {
      for (Map.Entry<TResourceType, TResourceQuotaRange> entry : thrift.getReadQuota().entrySet()) {
        quota.getReadQuota().put(toResourceType(entry.getKey()), fromRange(entry.getValue()));
      }
    }
    if (thrift.isSetWriteQuota()) {
      for (Map.Entry<TResourceType, TResourceQuotaRange> entry :
          thrift.getWriteQuota().entrySet()) {
        quota.getWriteQuota().put(toResourceType(entry.getKey()), fromRange(entry.getValue()));
      }
    }
    if (thrift.isSetThrottleLimit()) {
      quota.getThrottleLimit().putAll(thrift.getThrottleLimit());
    }
    return quota;
  }

  public static TUserResourceQuota toThrift(UserResourceQuota quota) {
    TUserResourceQuota thrift = new TUserResourceQuota();
    if (quota == null) {
      return thrift;
    }
    if (!quota.getReadQuota().isEmpty()) {
      Map<TResourceType, TResourceQuotaRange> readQuota = new EnumMap<>(TResourceType.class);
      for (Map.Entry<ResourceType, ResourceQuotaRange> entry : quota.getReadQuota().entrySet()) {
        readQuota.put(toThriftType(entry.getKey()), toRange(entry.getValue()));
      }
      thrift.setReadQuota(readQuota);
    }
    if (!quota.getWriteQuota().isEmpty()) {
      Map<TResourceType, TResourceQuotaRange> writeQuota = new EnumMap<>(TResourceType.class);
      for (Map.Entry<ResourceType, ResourceQuotaRange> entry : quota.getWriteQuota().entrySet()) {
        writeQuota.put(toThriftType(entry.getKey()), toRange(entry.getValue()));
      }
      thrift.setWriteQuota(writeQuota);
    }
    if (!quota.getThrottleLimit().isEmpty()) {
      thrift.setThrottleLimit(new HashMap<>(quota.getThrottleLimit()));
    }
    return thrift;
  }

  public static UserResourceQuota fromThrottleQuota(TThrottleQuota throttleQuota) {
    UserResourceQuota quota = new UserResourceQuota();
    if (throttleQuota == null) {
      return quota;
    }
    if (throttleQuota.isSetCpuLimit() && throttleQuota.getCpuLimit() > 0) {
      quota
          .getReadQuota()
          .put(
              ResourceType.CPU,
              new ResourceQuotaRange(IoTDBConstant.UNLIMITED_VALUE, throttleQuota.getCpuLimit()));
    }
    if (throttleQuota.isSetMemLimit() && throttleQuota.getMemLimit() > 0) {
      quota
          .getReadQuota()
          .put(
              ResourceType.MEMORY,
              new ResourceQuotaRange(IoTDBConstant.UNLIMITED_VALUE, throttleQuota.getMemLimit()));
    }
    if (throttleQuota.isSetThrottleLimit()) {
      quota.getThrottleLimit().putAll(throttleQuota.getThrottleLimit());
    }
    return quota;
  }

  public static TThrottleQuota toThrottleQuota(UserResourceQuota quota) {
    TThrottleQuota thrift = new TThrottleQuota();
    if (quota == null) {
      return thrift;
    }
    ResourceQuotaRange readCpu = quota.getReadQuota().get(ResourceType.CPU);
    if (readCpu != null && readCpu.getMaxValue() > 0) {
      thrift.setCpuLimit((int) readCpu.getMaxValue());
    }
    ResourceQuotaRange readMem = quota.getReadQuota().get(ResourceType.MEMORY);
    if (readMem != null && readMem.getMaxValue() > 0) {
      thrift.setMemLimit(readMem.getMaxValue());
    }
    if (!quota.getThrottleLimit().isEmpty()) {
      thrift.setThrottleLimit(new HashMap<>(quota.getThrottleLimit()));
    } else {
      thrift.setThrottleLimit(new HashMap<>());
    }
    return thrift;
  }

  private static ResourceQuotaRange fromRange(TResourceQuotaRange range) {
    if (range == null) {
      return new ResourceQuotaRange();
    }
    return new ResourceQuotaRange(range.getMinValue(), range.getMaxValue());
  }

  private static TResourceQuotaRange toRange(ResourceQuotaRange range) {
    TResourceQuotaRange thrift = new TResourceQuotaRange();
    if (range == null) {
      thrift.setMinValue(IoTDBConstant.UNLIMITED_VALUE);
      thrift.setMaxValue(IoTDBConstant.UNLIMITED_VALUE);
      return thrift;
    }
    thrift.setMinValue(range.getMinValue());
    thrift.setMaxValue(range.getMaxValue());
    return thrift;
  }

  private static ResourceType toResourceType(TResourceType type) {
    return ResourceType.valueOf(type.name());
  }

  private static TResourceType toThriftType(ResourceType type) {
    return TResourceType.valueOf(type.name());
  }
}
