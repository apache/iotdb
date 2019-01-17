/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.writelog;

import static junit.framework.TestCase.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.MetadataArgsErrorException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.RecoverException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.manager.WriteLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WriteLogNodeManagerTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean enableWal;

  @Before
  public void setUp() throws Exception {
    enableWal = config.enableWal;
    config.enableWal = true;
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.enableWal = enableWal;
  }

  @Test
  public void testAutoSync() throws IOException, InterruptedException {
    // this test check that nodes in a manager will sync periodically.
    int flushWalPeriod = config.flushWalThreshold;
    config.flushWalPeriodInMs = 10000;
    File tempRestore = File.createTempFile("managerTest", "restore");
    File tempProcessorStore = File.createTempFile("managerTest", "processorStore");

    WriteLogNodeManager manager = MultiFileLogNodeManager.getInstance();
    WriteLogNode logNode = manager
        .getNode("root.managerTest", tempRestore.getPath(), tempProcessorStore.getPath());

    InsertPlan bwInsertPlan = new InsertPlan(1, "logTestDevice", 100,
        Arrays.asList("s1", "s2", "s3", "s4"),
        Arrays.asList("1.0", "15", "str", "false"));
    UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path("root.logTestDevice.s1"));
    DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan);
    logNode.write(updatePlan);
    logNode.write(deletePlan);

    File walFile = new File(logNode.getLogDirectory() + File.separator + "wal");
    assertTrue(!walFile.exists());

    Thread.sleep(config.flushWalPeriodInMs + 1000);
    assertTrue(walFile.exists());

    logNode.delete();
    config.flushWalPeriodInMs = flushWalPeriod;
    tempRestore.delete();
    tempProcessorStore.delete();
    tempRestore.getParentFile().delete();
  }

  @Test
  public void testRecoverAll() throws IOException, RecoverException, MetadataArgsErrorException {
    // this test create 5 log nodes and recover them
    File tempRestore = File.createTempFile("managerTest", "restore");
    File tempProcessorStore = File.createTempFile("managerTest", "processorStore");

    WriteLogNodeManager manager = MultiFileLogNodeManager.getInstance();
    for (int i = 0; i < 5; i++) {
      String deviceName = "root.managerTest" + i;
      try {
        MManager.getInstance().setStorageLevelToMTree(deviceName);
        MManager.getInstance().addPathToMTree(deviceName + ".s1", TSDataType.DOUBLE.name(),
            TSEncoding.PLAIN.name(), new String[]{});
        MManager.getInstance().addPathToMTree(deviceName + ".s2", TSDataType.INT32.name(),
            TSEncoding.PLAIN.name(), new String[]{});
        MManager.getInstance().addPathToMTree(deviceName + ".s3", TSDataType.TEXT.name(),
            TSEncoding.PLAIN.name(), new String[]{});
        MManager.getInstance().addPathToMTree(deviceName + ".s4", TSDataType.BOOLEAN.name(),
            TSEncoding.PLAIN.name(), new String[]{});
      } catch (PathErrorException ignored) {
      }
      WriteLogNode logNode = manager
          .getNode(deviceName, tempRestore.getPath(), tempProcessorStore.getPath());

      InsertPlan bwInsertPlan = new InsertPlan(1, deviceName, 100,
          Arrays.asList("s1", "s2", "s3", "s4"),
          Arrays.asList("1.0", "15", "str", "false"));
      UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0", new Path(deviceName + ".s1"));
      DeletePlan deletePlan = new DeletePlan(50, new Path(deviceName + ".s1"));

      logNode.write(bwInsertPlan);
      logNode.write(updatePlan);
      logNode.write(deletePlan);

      logNode.forceSync();
      logNode.close();
    }
    manager.recover();
  }
}
