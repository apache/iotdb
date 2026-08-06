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

import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UserResourceQuota {

  private final Map<ResourceType, ResourceQuotaRange> readQuota = new EnumMap<>(ResourceType.class);
  private final Map<ResourceType, ResourceQuotaRange> writeQuota =
      new EnumMap<>(ResourceType.class);
  private final Map<ThrottleType, TTimedQuota> throttleLimit = new HashMap<>();

  public Map<ResourceType, ResourceQuotaRange> getReadQuota() {
    return readQuota;
  }

  public Map<ResourceType, ResourceQuotaRange> getWriteQuota() {
    return writeQuota;
  }

  public Map<ThrottleType, TTimedQuota> getThrottleLimit() {
    return throttleLimit;
  }

  public ResourceQuotaRange getRange(OperationType op, ResourceType resource) {
    Map<ResourceType, ResourceQuotaRange> quota = op == OperationType.READ ? readQuota : writeQuota;
    return quota.get(resource);
  }

  public void mergeFrom(UserResourceQuota other) {
    if (other == null) {
      return;
    }
    for (Map.Entry<ResourceType, ResourceQuotaRange> entry : other.readQuota.entrySet()) {
      readQuota.put(entry.getKey(), copyRange(entry.getValue()));
    }
    for (Map.Entry<ResourceType, ResourceQuotaRange> entry : other.writeQuota.entrySet()) {
      writeQuota.put(entry.getKey(), copyRange(entry.getValue()));
    }
    throttleLimit.putAll(other.throttleLimit);
  }

  private static ResourceQuotaRange copyRange(ResourceQuotaRange range) {
    if (range == null) {
      return null;
    }
    return new ResourceQuotaRange(range.getMinValue(), range.getMaxValue());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserResourceQuota that = (UserResourceQuota) o;
    return Objects.equals(readQuota, that.readQuota)
        && Objects.equals(writeQuota, that.writeQuota)
        && Objects.equals(throttleLimit, that.throttleLimit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(readQuota, writeQuota, throttleLimit);
  }
}
