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

import org.junit.Assert;
import org.junit.Test;

import java.util.EnumMap;
import java.util.Map;

public class UserResourceQuotaConverterTest {

  @Test
  public void testThrottleMigration() {
    TThrottleQuota throttle = new TThrottleQuota();
    throttle.setCpuLimit(4);
    throttle.setMemLimit(1024);
    UserResourceQuota quota = UserResourceQuotaConverter.fromThrottleQuota(throttle);
    Assert.assertEquals(4, quota.getReadQuota().get(ResourceType.CPU).getMaxValue());
    Assert.assertEquals(1024, quota.getReadQuota().get(ResourceType.MEMORY).getMaxValue());
    Assert.assertEquals(4, UserResourceQuotaConverter.toThrottleQuota(quota).getCpuLimit());
  }

  @Test
  public void testThriftRoundTrip() {
    TUserResourceQuota thrift = new TUserResourceQuota();
    Map<TResourceType, TResourceQuotaRange> read = new EnumMap<>(TResourceType.class);
    read.put(TResourceType.CPU, new TResourceQuotaRange(1, 8));
    read.put(TResourceType.MEMORY, new TResourceQuotaRange(IoTDBConstant.UNLIMITED_VALUE, 2048));
    thrift.setReadQuota(read);
    UserResourceQuota quota = UserResourceQuotaConverter.fromThrift(thrift);
    TUserResourceQuota roundTrip = UserResourceQuotaConverter.toThrift(quota);
    Assert.assertEquals(1, roundTrip.getReadQuota().get(TResourceType.CPU).getMinValue());
    Assert.assertEquals(8, roundTrip.getReadQuota().get(TResourceType.CPU).getMaxValue());
  }

  @Test
  public void testPartialMergeKeepsUnmentionedDims() {
    UserResourceQuota base = new UserResourceQuota();
    base.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(1, 4));
    base.getWriteQuota().put(ResourceType.MEMORY, new ResourceQuotaRange(0, 1024));

    UserResourceQuota patch = new UserResourceQuota();
    patch.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(2, 8));

    base.mergeFrom(patch);
    Assert.assertEquals(2, base.getReadQuota().get(ResourceType.CPU).getMinValue());
    Assert.assertEquals(8, base.getReadQuota().get(ResourceType.CPU).getMaxValue());
    Assert.assertEquals(1024, base.getWriteQuota().get(ResourceType.MEMORY).getMaxValue());
  }
}
