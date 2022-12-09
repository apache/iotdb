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

import org.apache.iotdb.commons.exception.sync.PipeSinkException;
import org.apache.iotdb.commons.sync.pipesink.PipeSink;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.GetPipeSinkPlan;
import org.apache.iotdb.confignode.persistence.sync.ClusterSyncInfo;
import org.apache.iotdb.confignode.rpc.thrift.TPipeSinkInfo;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class ClusterSyncInfoTest {

  private ClusterSyncInfo clusterSyncInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @Before
  public void setup() throws IOException {
    clusterSyncInfo = new ClusterSyncInfo();
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

  private void prepareClusterSyncInfo() {
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put("ip", "192.168.11.11");
    attributes1.put("port", "7766");
    TPipeSinkInfo pipeSinkInfo1 =
        new TPipeSinkInfo()
            .setPipeSinkName("demo1")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes1);
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put("ip", "192.168.22.2");
    attributes2.put("port", "7777");
    TPipeSinkInfo pipeSinkInfo2 =
        new TPipeSinkInfo()
            .setPipeSinkName("demo2")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes2);

    clusterSyncInfo.addPipeSink(new CreatePipeSinkPlan(pipeSinkInfo1));
    clusterSyncInfo.addPipeSink(new CreatePipeSinkPlan(pipeSinkInfo2));
  }

  @Test
  public void testEmptySnapshot() throws Exception {
    // test empty snapshot
    Assert.assertTrue(clusterSyncInfo.processTakeSnapshot(snapshotDir));
    ClusterSyncInfo clusterSyncInfo2 = new ClusterSyncInfo();
    clusterSyncInfo2.processLoadSnapshot(snapshotDir);

    List<PipeSink> expectedPipeSink =
        clusterSyncInfo.getPipeSink(new GetPipeSinkPlan()).getPipeSinkList();
    List<PipeSink> actualPipeSink =
        clusterSyncInfo2.getPipeSink(new GetPipeSinkPlan()).getPipeSinkList();
    Assert.assertEquals(expectedPipeSink, actualPipeSink);
  }

  @Test
  public void testSnapshot() throws Exception {
    // test snapshot with data
    prepareClusterSyncInfo();
    Assert.assertTrue(clusterSyncInfo.processTakeSnapshot(snapshotDir));
    ClusterSyncInfo clusterSyncInfo2 = new ClusterSyncInfo();
    clusterSyncInfo2.processLoadSnapshot(snapshotDir);

    List<PipeSink> expectedPipeSink =
        clusterSyncInfo.getPipeSink(new GetPipeSinkPlan()).getPipeSinkList();
    List<PipeSink> actualPipeSink =
        clusterSyncInfo2.getPipeSink(new GetPipeSinkPlan()).getPipeSinkList();
    Assert.assertEquals(expectedPipeSink, actualPipeSink);
  }

  @Test
  public void testPipeSinkOperation() {
    prepareClusterSyncInfo();
    Map<String, String> attributes1 = new HashMap<>();
    attributes1.put("ip", "192.168.11.11");
    attributes1.put("port", "7766");
    Map<String, String> attributes2 = new HashMap<>();
    attributes2.put("ip", "Nonstandard");
    attributes2.put("port", "7777");
    TPipeSinkInfo alreadyExistSink =
        new TPipeSinkInfo()
            .setPipeSinkName("demo1")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes1);
    TPipeSinkInfo errorAttributeSink =
        new TPipeSinkInfo()
            .setPipeSinkName("demo3")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes2);
    TPipeSinkInfo nonExistSink =
        new TPipeSinkInfo()
            .setPipeSinkName("demo3")
            .setPipeSinkType("IoTDB")
            .setAttributes(attributes1);

    try {
      clusterSyncInfo.checkAddPipeSink(new CreatePipeSinkPlan(alreadyExistSink));
      Assert.fail("checkOperatePipeSink ignore failure.");
    } catch (PipeSinkException e) {
      // nothing
    }
    try {
      clusterSyncInfo.checkAddPipeSink(new CreatePipeSinkPlan(alreadyExistSink));
      Assert.fail("checkOperatePipeSink ignore failure.");
    } catch (PipeSinkException e) {
      // nothing
    }
    try {
      clusterSyncInfo.checkAddPipeSink(new CreatePipeSinkPlan(errorAttributeSink));
      Assert.fail("checkOperatePipeSink ignore failure.");
    } catch (PipeSinkException e) {
      // nothing
    }

    try {
      clusterSyncInfo.checkAddPipeSink(new CreatePipeSinkPlan(nonExistSink));
      clusterSyncInfo.checkDropPipeSink("demo1");
    } catch (PipeSinkException e) {
      Assert.fail("checkOperatePipeSink should not throw exception.");
    }
  }
}
