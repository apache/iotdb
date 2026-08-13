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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;

import java.util.HashMap;
import java.util.Map;

public class ThrottleQuotaLimit {
  private Map<Long, QuotaLimiter> userQuotaLimiter;
  private final Map<Long, Long> memLimit;
  private final Map<Long, Integer> cpuLimit;

  public ThrottleQuotaLimit() {
    userQuotaLimiter = new HashMap<>();
    memLimit = new HashMap<>();
    cpuLimit = new HashMap<>();
  }

  /**
   * Apply a throttle quota update for {@code req.userId}. This never removes an existing entry:
   * empty rate-limit maps or zero cpu/mem are treated as incremental updates (same as the legacy
   * userName-keyed behavior). Call {@link #removeQuota(long)} explicitly for DROP USER cleanup.
   */
  public void setQuotas(TSetThrottleQuotaReq req) {
    if (!req.isSetUserId() || req.getThrottleQuota() == null) {
      return;
    }
    long userId = req.getUserId();
    TThrottleQuota throttleQuota = req.getThrottleQuota();
    if (throttleQuota.isSetThrottleLimit()
        && throttleQuota.getThrottleLimit() != null
        && !throttleQuota.getThrottleLimit().isEmpty()) {
      userQuotaLimiter.put(userId, QuotaLimiter.fromThrottle(throttleQuota.getThrottleLimit()));
    }
    memLimit.put(userId, throttleQuota.getMemLimit());
    cpuLimit.put(userId, throttleQuota.cpuLimit);
  }

  public void removeQuota(long userId) {
    userQuotaLimiter.remove(userId);
    memLimit.remove(userId);
    cpuLimit.remove(userId);
  }

  public Map<Long, QuotaLimiter> getUserQuotaLimiter() {
    return userQuotaLimiter;
  }

  public void setUserQuotaLimiter(Map<Long, QuotaLimiter> userQuotaLimiter) {
    this.userQuotaLimiter = userQuotaLimiter;
  }

  public QuotaLimiter getUserLimiter(long userId) {
    return userQuotaLimiter.get(userId);
  }

  public boolean checkCpu(long userId, int cpuNum) {
    if (cpuLimit.get(userId) == null
        || cpuLimit.get(userId) == 0
        || cpuLimit.get(userId) > cpuNum) {
      return true;
    }
    return false;
  }

  public boolean checkMemory(long userId, long estimatedMemory) {
    if (memLimit.get(userId) == null
        || memLimit.get(userId) == 0
        || memLimit.get(userId) > estimatedMemory) {
      return true;
    }
    return false;
  }
}
