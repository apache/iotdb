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
package org.apache.iotdb.db.writelog;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.storageGroup.StorageGroupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class PerformanceTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean enableWal;
  private boolean skip = true;

  @Before
  public void setUp() throws Exception {
    enableWal = config.isEnableWal();
    config.setEnableWal(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setEnableWal(enableWal);
  }

  @Test
  public void writeLogTest() throws IOException {
    // this test insert 1000000 * 3 logs and report elapsed time
    if (skip) {
      return;
    }
    int[] batchSizes = new int[]{100, 500, 1000, 5000, 10000};
    long[] forceCycle = new long[]{10, 0};
    int oldBatchSize = config.getFlushWalThreshold();
    long oldForceCycle = config.getForceWalPeriodInMs();
    for (int j = 0; j < batchSizes.length; j++) {
      for (int k = 0; k < forceCycle.length; k++) {
        config.setFlushWalThreshold(batchSizes[j]);
        config.setForceWalPeriodInMs(forceCycle[k]);
        File tempRestore = new File("testtemp", "restore");
        File tempProcessorStore = new File("testtemp", "processorStore");
        tempRestore.getParentFile().mkdirs();
        tempRestore.createNewFile();
        tempProcessorStore.createNewFile();

        WriteLogNode logNode = new ExclusiveWriteLogNode("root.testLogNode");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
          InsertPlan bwInsertPlan = new InsertPlan("logTestDevice", 100,
              new String[]{"s1", "s2", "s3", "s4"},
              new String[]{"1.0", "15", "str", "false"});
          UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0",
              new Path("root.logTestDevice.s1"));
          DeletePlan deletePlan = new DeletePlan(50,
              new Path("root.logTestDevice.s1"));

          logNode.write(bwInsertPlan);
          logNode.write(updatePlan);
          logNode.write(deletePlan);
        }
        logNode.forceSync();

        System.out.println(
            3000000 + " logs use " + (System.currentTimeMillis() - time) + " ms at batch size "
                + config.getFlushWalThreshold());

        logNode.delete();
        tempRestore.delete();
        tempProcessorStore.delete();
        tempRestore.getParentFile().delete();
      }
    }
    config.setFlushWalThreshold(oldBatchSize);
    config.setForceWalPeriodInMs(oldForceCycle);
  }

  @Test
  public void recoverTest()
      throws IOException, MetadataException, StorageGroupException {
    // this test insert 1000000 * 3 logs , recover from them and report elapsed time
    if (skip) {
      return;
    }
    File tempRestore = new File("testtemp", "restore");
    File tempProcessorStore = new File("testtemp", "processorStore");
    tempRestore.getParentFile().mkdirs();
    tempRestore.createNewFile();
    tempProcessorStore.createNewFile();

    try {
      MManager.getInstance().setStorageGroup("root.logTestDevice");
    } catch (MetadataException ignored) {
    }
    MManager.getInstance().createTimeseries("root.logTestDevice.s1",
        TSDataType.DOUBLE.name(),
        TSEncoding.PLAIN.name());
    MManager.getInstance()
        .createTimeseries("root.logTestDevice.s2", TSDataType.INT32.name(),
            TSEncoding.PLAIN.name());
    MManager.getInstance()
        .createTimeseries("root.logTestDevice.s3", TSDataType.TEXT.name(),
            TSEncoding.PLAIN.name());
    MManager.getInstance().createTimeseries("root.logTestDevice.s4", TSDataType.BOOLEAN.name(),
        TSEncoding.PLAIN.name());
    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");

    for (int i = 0; i < 1000000; i++) {
      InsertPlan bwInsertPlan = new InsertPlan("root.logTestDevice", 100,
          new String[]{"s1", "s2", "s3", "s4"}, new String[]{"1.0", "15", "str", "false"});
      UpdatePlan updatePlan = new UpdatePlan(0, 100, "2.0",
          new Path("root.logTestDevice.s1"));
      DeletePlan deletePlan = new DeletePlan(50, new Path("root.logTestDevice.s1"));

      logNode.write(bwInsertPlan);
      logNode.write(updatePlan);
      logNode.write(deletePlan);
    }
    try {
      logNode.forceSync();
      long time = System.currentTimeMillis();
      System.out.println(
          3000000 + " logs use " + (System.currentTimeMillis() - time) + "ms when recovering ");
    } finally {
      logNode.delete();
      tempRestore.delete();
      tempProcessorStore.delete();
      tempRestore.getParentFile().delete();
    }
  }
}
