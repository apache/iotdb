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
package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.pipesink.IoTDBPipeSink;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.transport.client.IoTDBSyncClient;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

@Category({LocalStandaloneTest.class})
public class IoTDBSyncReceiverIT {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBSyncReceiverIT.class);

  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  String pipeName1 = "pipe1";
  String remoteIp1;
  long createdTime1 = System.currentTimeMillis();
  String showPipeSql = "SHOW PIPE";
  IoTDBSyncClient client;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableUnseqSpaceCompaction(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableCrossSpaceCompaction(false);
    SyncTestUtil.insertData();
    EnvironmentUtils.shutdownDaemon();
    File srcDir = new File(IoTDBDescriptor.getInstance().getConfig().getDataDirs()[0]);
    if (tmpDir.exists()) {
      FileUtils.deleteDirectory(tmpDir);
    }
    FileUtils.moveDirectory(srcDir, tmpDir);
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
    Pipe pipe = new TsFilePipe(createdTime1, pipeName1, new IoTDBPipeSink("sink"), 0, false);
    remoteIp1 = "127.0.0.1";
    client = new IoTDBSyncClient(pipe, remoteIp1, 6667, "root.vehicle");
    client.handshake();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    FileUtils.deleteDirectory(tmpDir);
    client.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testReceiveDataAndLoad() {
    logger.info("testReceiveDataAndLoad");
    try {
      // 1. handshake
      client.handshake();

      // 2. send pipe data
      int serialNum = 0;
      List<File> tsFiles = SyncTestUtil.getTsFilePaths(tmpDir);
      SyncTestUtil.renameTsFiles(tsFiles);
      for (File f : tsFiles) {
        client.send(new TsFilePipeData(f.getPath(), serialNum++));
      }
      Deletion deletion = new Deletion(new PartialPath("root.vehicle.**"), 0, 33, 38);
      PipeData pipeData = new DeletionPipeData(deletion, serialNum++);
      client.send(pipeData);

      // wait collector to load pipe data
      Thread.sleep(1000);

      // 3. check result
      String sql1 = "select * from root.vehicle.*";
      String[] retArray1 =
          new String[] {
            "6,120,null,null,null",
            "9,null,123,null,null",
            "16,128,null,null,16.0",
            "18,189,198,true,18.0",
            "20,null,null,false,null",
            "29,null,null,true,1205.0",
            "99,null,1234,null,null"
          };
      String[] columnNames1 = {
        "root.vehicle.d0.s0", "root.vehicle.d0.s1", "root.vehicle.d1.s3", "root.vehicle.d1.s2"
      };
      SyncTestUtil.checkResult(sql1, columnNames1, retArray1);
      String sql2 = "select * from root.sg1.d1";
      String[] retArray2 =
          new String[] {
            "1,1.0,1,null,true,aligned_test1",
            "2,2.0,2,null,null,aligned_test2",
            "3,3.0,null,null,false,aligned_test3",
            "4,4.0,4,null,true,aligned_test4",
            "5,130000.0,130000,130000,false,aligned_unseq_test1",
            "6,6.0,6,6,true,null",
            "7,7.0,7,7,false,aligned_test7",
            "8,8.0,8,8,null,aligned_test8",
            "9,9.0,9,9,false,aligned_test9",
          };
      String[] columnNames2 = {
        "root.sg1.d1.s1", "root.sg1.d1.s2", "root.sg1.d1.s3", "root.sg1.d1.s4", "root.sg1.d1.s5",
      };
      SyncTestUtil.checkResult(sql2, columnNames2, retArray2);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
