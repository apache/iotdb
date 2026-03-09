/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.CreateExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StartExternalServicePlan;
import org.apache.iotdb.confignode.manager.externalservice.ExternalServiceInfo;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class ExternalServiceInfoTest {

  private static ExternalServiceInfo serviceInfo;
  private static ExternalServiceInfo serviceInfoBefore;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() throws IOException {
    serviceInfo = new ExternalServiceInfo();
    serviceInfoBefore = new ExternalServiceInfo();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException {
    serviceInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException, IllegalPathException {
    // test empty
    serviceInfoBefore.processTakeSnapshot(snapshotDir);
    serviceInfo.processLoadSnapshot(snapshotDir);

    CreateExternalServicePlan createExternalServicePlan =
        new CreateExternalServicePlan(
            1, new ServiceInfo("TEST1", "testClassName", ServiceInfo.ServiceType.USER_DEFINED));
    serviceInfo.addService(createExternalServicePlan);
    serviceInfoBefore.addService(createExternalServicePlan);

    createExternalServicePlan =
        new CreateExternalServicePlan(
            2, new ServiceInfo("TEST1", "testClassName", ServiceInfo.ServiceType.USER_DEFINED));
    serviceInfo.addService(createExternalServicePlan);
    serviceInfoBefore.addService(createExternalServicePlan);

    StartExternalServicePlan startExternalServicePlan = new StartExternalServicePlan(1, "TEST1");
    serviceInfo.startService(startExternalServicePlan);
    serviceInfoBefore.startService(startExternalServicePlan);

    serviceInfo.processTakeSnapshot(snapshotDir);
    serviceInfo.clear();
    serviceInfo.processLoadSnapshot(snapshotDir);

    Assert.assertEquals(
        serviceInfo.getRawDatanodeToServiceInfos(),
        serviceInfoBefore.getRawDatanodeToServiceInfos());
  }
}
