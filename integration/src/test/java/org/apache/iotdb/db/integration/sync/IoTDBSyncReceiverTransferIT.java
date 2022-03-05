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
 *
 */
package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.newsync.receiver.ReceiverService;
import org.apache.iotdb.db.newsync.transport.client.TransportClient;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Category({LocalStandaloneTest.class})
public class IoTDBSyncReceiverTransferIT {
  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;
  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  String pipeName1 = "pipe1";
  String remoteIp1 = "192.168.0.11";
  long createdTime1 = System.currentTimeMillis();
  File pipeLogDir1 =
      new File(SyncPathUtil.getReceiverPipeLogDir(pipeName1, remoteIp1, createdTime1));
  //    String pipeName2 = "pipe2";
  //    String remoteIp2 = "192.168.0.22";
  //    long createdTime2 = System.currentTimeMillis();
  //    File pipeLogDir2 =
  //            new File(SyncPathUtil.getReceiverPipeLogDir(pipeName2, remoteIp2, createdTime2));

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
    FileUtils.moveDirectory(srcDir, tmpDir);
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
    FileUtils.deleteDirectory(pipeLogDir1);
    //        FileUtils.deleteDirectory(pipeLogDir2);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws Exception {
    // 1. restart IoTDB
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();

    // 2. prepare pipelog and pipeDataQueue
    if (!pipeLogDir1.exists()) {
      pipeLogDir1.mkdirs();
    }
    int serialNum = 0;
    List<PipeData> pipeDataList = new ArrayList<>();
    List<PhysicalPlan> planList = new ArrayList<>();
    planList.add(new SetStorageGroupPlan(new PartialPath("root.vehicle")));
    planList.add(
        new CreateTimeSeriesPlan(
            new PartialPath("root.vehicle.d0.s0"),
            new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.RLE)));
    planList.add(
        new CreateTimeSeriesPlan(
            new PartialPath("root.vehicle.d0.s1"),
            new MeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN)));
    planList.add(
        new CreateTimeSeriesPlan(
            new PartialPath("root.vehicle.d1.s2"),
            new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE)));
    planList.add(
        new CreateTimeSeriesPlan(
            new PartialPath("root.vehicle.d1.s3"),
            new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.PLAIN)));
    planList.add(new SetStorageGroupPlan(new PartialPath("root.sg1")));
    planList.add(
        new CreateAlignedTimeSeriesPlan(
            new PartialPath("root.sg1.d1"),
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                TSDataType.FLOAT,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.BOOLEAN,
                TSDataType.TEXT),
            Arrays.asList(
                TSEncoding.RLE,
                TSEncoding.GORILLA,
                TSEncoding.RLE,
                TSEncoding.RLE,
                TSEncoding.PLAIN),
            Arrays.asList(
                CompressionType.SNAPPY,
                CompressionType.SNAPPY,
                CompressionType.SNAPPY,
                CompressionType.SNAPPY,
                CompressionType.SNAPPY),
            null));
    for (PhysicalPlan plan : planList) {
      pipeDataList.add(new SchemaPipeData(plan, serialNum++));
    }
    List<File> tsFiles = SyncTestUtil.getTsFilePaths(tmpDir);
    for (File f : tsFiles) {
      pipeDataList.add(new TsFilePipeData(f.getPath(), serialNum++));
    }
    Deletion deletion = new Deletion(new PartialPath("root.vehicle.**"), 0, 33, 38);
    pipeDataList.add(new DeletionPipeData(deletion, serialNum++));

    // 3. start server
    ReceiverService.getInstance().startPipeServer();

    // 4. start client
    TransportClient client = new TransportClient(null, "127.0.0.1", 5555);
    for (PipeData pipeData : pipeDataList) {
      client.senderTransport(pipeData);
    }
  }
}
