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
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

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

  private void prepareQuotaInfo() {
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

  @Test
  public void testSnapshot() throws TException, IOException {
    prepareQuotaInfo();

    quotaInfo.processTakeSnapshot(snapshotDir);
    QuotaInfo quotaInfo2 = new QuotaInfo();
    quotaInfo2.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(quotaInfo.getSpaceQuotaLimit(), quotaInfo2.getSpaceQuotaLimit());
  }
}
