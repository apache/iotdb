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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSpaceQuota;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class QuotaInfoTest {

  private QuotaInfo quotaInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @Before
  public void setup() throws IOException {
    quotaInfo = new QuotaInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @After
  public void cleanup() throws IOException {
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  private void prepareSpaceQuotaInfo() {
    List<String> prefixPathList = new ArrayList<>();
    prefixPathList.add("root.sg");
    prefixPathList.add("root.ln");
    TSpaceQuota spaceQuota = new TSpaceQuota();
    spaceQuota.setTimeserieNum(10000);
    spaceQuota.setDeviceNum(100);
    spaceQuota.setDiskSize(512);
    SetSpaceQuotaPlan setSpaceQuotaPlan = new SetSpaceQuotaPlan(prefixPathList, spaceQuota);
    quotaInfo.setSpaceQuota(setSpaceQuotaPlan);
  }

  private void prepareThrottleQuotaInfo() {
    String userName = "tempUser";
    Map<ThrottleType, TTimedQuota> quotaLimit = new HashMap<>();
    quotaLimit.put(ThrottleType.READ_NUMBER, new TTimedQuota(1000, 1000));
    quotaLimit.put(ThrottleType.READ_SIZE, new TTimedQuota(2000, 2000));
    TThrottleQuota throttleQuota = new TThrottleQuota();
    throttleQuota.setThrottleLimit(quotaLimit);
    throttleQuota.setMemLimit(1000);
    throttleQuota.setCpuLimit(3);
    SetThrottleQuotaPlan setThrottleQuotaPlan = new SetThrottleQuotaPlan(userName, throttleQuota);
    quotaInfo.setThrottleQuota(setThrottleQuotaPlan);
  }

  @Test
  public void testSnapshot() throws TException, IOException {
    prepareSpaceQuotaInfo();
    prepareThrottleQuotaInfo();

    quotaInfo.processTakeSnapshot(snapshotDir);
    QuotaInfo quotaInfo2 = new QuotaInfo();
    quotaInfo2.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(quotaInfo.getSpaceQuotaLimit(), quotaInfo2.getSpaceQuotaLimit());
    Assert.assertEquals(quotaInfo.getThrottleQuotaLimit(), quotaInfo2.getThrottleQuotaLimit());
  }
}
