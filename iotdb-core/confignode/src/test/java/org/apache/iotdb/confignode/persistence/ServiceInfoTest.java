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

import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.service.external.ServiceStatus;
import org.apache.iotdb.confignode.consensus.request.write.service.CreateServicePlan;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class ServiceInfoTest {
  private static ServiceInfo serviceInfo;
  private static ServiceInfo serviceInfoSaveBefore;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    serviceInfo = new ServiceInfo();
    serviceInfoSaveBefore = new ServiceInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    serviceInfo.clear();
    if (snapshotDir.exists()) {
      // Clean up the snapshot directory after tests
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testTakeSnapshot() throws Exception {
    ServiceInformation serviceInformation =
        new ServiceInformation(
            "testService",
            "TestServiceClass",
            true,
            "testService.jar",
            "md5checksum",
            ServiceStatus.INACTIVE);
    CreateServicePlan createServicePlan =
        new CreateServicePlan(serviceInformation, new Binary(new byte[] {1, 2, 3}));
    serviceInfo.addServiceInTable(createServicePlan);
    serviceInfoSaveBefore.addServiceInTable(createServicePlan);

    // Take snapshot
    serviceInfo.processTakeSnapshot(snapshotDir);

    // Clear current state
    serviceInfo.clear();

    // Load from snapshot
    serviceInfo.processLoadSnapshot(snapshotDir);

    // Verify the loaded state matches the saved state
    Assert.assertEquals(
        serviceInfo.getRawExistedJarToMD5(), serviceInfoSaveBefore.getRawExistedJarToMD5());
    Assert.assertEquals(
        serviceInfo.getRawServiceTable(), serviceInfoSaveBefore.getRawServiceTable());
  }
}
