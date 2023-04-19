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

package org.apache.iotdb.db.quotas;

import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;

import java.util.HashMap;
import java.util.Map;

public class ThrottleQuotaLimit {
  private Map<String, QuotaLimiter> userQuotaLimiter;
  private final Map<String, Long> memLimit;
  private final Map<String, Integer> cpuLimit;

  public ThrottleQuotaLimit() {
    userQuotaLimiter = new HashMap<>();
    memLimit = new HashMap<>();
    cpuLimit = new HashMap<>();
  }

  public void setQuotas(TSetThrottleQuotaReq req) {
    if (!req.getThrottleQuota().getThrottleLimit().isEmpty()) {
      userQuotaLimiter.put(
          req.getUserName(), QuotaLimiter.fromThrottle(req.getThrottleQuota().getThrottleLimit()));
    }
    memLimit.put(req.getUserName(), req.getThrottleQuota().getMemLimit());
    cpuLimit.put(req.getUserName(), req.getThrottleQuota().cpuLimit);
  }

  public Map<String, QuotaLimiter> getUserQuotaLimiter() {
    return userQuotaLimiter;
  }

  public void setUserQuotaLimiter(Map<String, QuotaLimiter> userQuotaLimiter) {
    this.userQuotaLimiter = userQuotaLimiter;
  }

  public QuotaLimiter getUserLimiter(String userName) {
    return userQuotaLimiter.get(userName);
  }

  public boolean checkCpu(String userName, int cpuNum) {
    if (cpuLimit.get(userName) == null
        || cpuLimit.get(userName) == 0
        || cpuLimit.get(userName) > cpuNum) {
      return true;
    }
    return false;
  }

  public boolean checkMemory(String userName, long estimatedMemory) {
    if (memLimit.get(userName) == null
        || memLimit.get(userName) == 0
        || memLimit.get(userName) > estimatedMemory) {
      return true;
    }
    return false;
  }
}
