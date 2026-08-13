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
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.conf.IoTDBConstant;

import org.junit.Assert;
import org.junit.Test;

import java.util.EnumMap;
import java.util.HashMap;

public class ThrottleQuotaLimitTest {

  @Test
  public void setQuotasDoesNotClearExistingRateLimiterOnEmptyDerivedFields() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    long userId = 42L;

    TSetThrottleQuotaReq withRate = new TSetThrottleQuotaReq();
    withRate.setUserId(userId);
    withRate.setUserName("u");
    TThrottleQuota rateQuota = new TThrottleQuota();
    rateQuota.setThrottleLimit(
        new EnumMap<>(ThrottleType.class) {
          {
            put(ThrottleType.READ_NUMBER, new TTimedQuota(1000, 10));
          }
        });
    rateQuota.setMemLimit(1024);
    rateQuota.setCpuLimit(2);
    withRate.setThrottleQuota(rateQuota);
    limit.setQuotas(withRate);
    Assert.assertNotNull(limit.getUserLimiter(userId));

    // Simulates USER QUOTA sync that only carries temp-disk / non-throttle fields: empty rate map
    // and zero cpu/mem. Must keep the existing rate limiter.
    TSetThrottleQuotaReq derivedEmpty = new TSetThrottleQuotaReq();
    derivedEmpty.setUserId(userId);
    derivedEmpty.setUserName("u");
    TThrottleQuota empty = new TThrottleQuota();
    empty.setThrottleLimit(new HashMap<>());
    empty.setMemLimit(0);
    empty.setCpuLimit(0);
    derivedEmpty.setThrottleQuota(empty);
    limit.setQuotas(derivedEmpty);

    Assert.assertNotNull(limit.getUserLimiter(userId));
    Assert.assertTrue(limit.checkMemory(userId, Long.MAX_VALUE));
  }

  @Test
  public void unlimitedValuesAreNotTreatedAsDropClear() {
    TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
    req.setUserId(1L);
    TThrottleQuota quota = new TThrottleQuota();
    quota.setThrottleLimit(new HashMap<>());
    quota.setMemLimit(IoTDBConstant.UNLIMITED_VALUE);
    quota.setCpuLimit(IoTDBConstant.UNLIMITED_VALUE);
    req.setThrottleQuota(quota);
    Assert.assertFalse(DataNodeThrottleQuotaManager.isDropClearRequest(req));
  }

  @Test
  public void dropBroadcastIsDetectedAsClear() {
    TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
    req.setUserId(1L);
    TThrottleQuota quota = new TThrottleQuota();
    quota.setThrottleLimit(new HashMap<>());
    quota.setMemLimit(0);
    quota.setCpuLimit(0);
    req.setThrottleQuota(quota);
    Assert.assertTrue(DataNodeThrottleQuotaManager.isDropClearRequest(req));
  }

  @Test
  public void removeQuotaClearsAllFields() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    long userId = 7L;
    TSetThrottleQuotaReq req = new TSetThrottleQuotaReq();
    req.setUserId(userId);
    TThrottleQuota quota = new TThrottleQuota();
    quota.setThrottleLimit(
        new EnumMap<>(ThrottleType.class) {
          {
            put(ThrottleType.WRITE_SIZE, new TTimedQuota(1000, 100));
          }
        });
    quota.setMemLimit(2048);
    quota.setCpuLimit(4);
    req.setThrottleQuota(quota);
    limit.setQuotas(req);
    limit.removeQuota(userId);
    Assert.assertNull(limit.getUserLimiter(userId));
    Assert.assertTrue(limit.checkCpu(userId, 1));
    Assert.assertTrue(limit.checkMemory(userId, 1));
  }
}
