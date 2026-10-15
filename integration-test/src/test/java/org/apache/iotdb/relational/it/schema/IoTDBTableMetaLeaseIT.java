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

package org.apache.iotdb.relational.it.schema;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Category({TableLocalStandaloneIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBTableMetaLeaseIT {

  private static final long CUSTOM_FENCE_MS = 20101L;
  private static final String EXPECTED_LOG =
      "Updated metadata lease fence threshold to " + CUSTOM_FENCE_MS + " ms";

  private static DataNodeWrapper dataNodeWrapper;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getConfigNodeConfig().setMetadataLeaseFenceMs(CUSTOM_FENCE_MS);
    EnvFactory.getEnv().initClusterEnvironment();
    dataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDnStartupLogsFenceThreshold() throws IOException {
    assertTrue(
        "DN startup log should contain updated fence threshold",
        dataNodeWrapper.logContains(EXPECTED_LOG));
  }

  @Test
  public void testDnRestartLogsFenceThreshold() throws Exception {
    EnvFactory.getEnv().shutdownDataNode(0);
    dataNodeWrapper.clearLogContent();
    EnvFactory.getEnv().startDataNode(0);
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () ->
                assertTrue(
                    "DN restart log should contain updated fence threshold",
                    dataNodeWrapper.logContains(EXPECTED_LOG)));
    ;
  }
}
