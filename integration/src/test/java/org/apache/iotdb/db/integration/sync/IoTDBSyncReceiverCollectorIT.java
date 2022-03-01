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

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.conf.SyncConstant;
import org.apache.iotdb.db.newsync.conf.SyncPathUtil;
import org.apache.iotdb.db.newsync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.newsync.pipedata.queue.BufferedPipeDataQueue;
import org.apache.iotdb.db.newsync.receiver.collector.Collector;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Category({LocalStandaloneTest.class})
public class IoTDBSyncReceiverCollectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSyncReceiverLoaderIT.class);
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
  String pipeName2 = "pipe2";
  String remoteIp2 = "192.168.0.22";
  long createdTime2 = System.currentTimeMillis();
  File pipeLogDir2 =
      new File(SyncPathUtil.getReceiverPipeLogDir(pipeName2, remoteIp2, createdTime2));

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
    FileUtils.deleteDirectory(pipeLogDir2);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testOnePipe() throws Exception {
    // 1. restart IoTDB
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();

    // 2. prepare pipelog and pipeDataQueue
    if (!pipeLogDir1.exists()) {
      pipeLogDir1.mkdirs();
    }
    DataOutputStream outputStream =
        new DataOutputStream(
            new FileOutputStream(new File(pipeLogDir1, SyncConstant.COMMIT_LOG_NAME), true));
    outputStream.writeLong(-1);
    outputStream.close();
    int serialNum = 0;
    File pipeLog1 = new File(pipeLogDir1.getPath(), SyncConstant.getPipeLogName(serialNum));
    DataOutputStream pipeLogOutput = new DataOutputStream(new FileOutputStream(pipeLog1, false));
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
      PipeData pipeData = new SchemaPipeData(plan, serialNum++);
      pipeData.serialize(pipeLogOutput);
    }
    pipeLogOutput.close();
    File pipeLog2 = new File(pipeLogDir1.getPath(), SyncConstant.getPipeLogName(serialNum));
    pipeLogOutput = new DataOutputStream(new FileOutputStream(pipeLog2, false));
    List<File> tsFiles = SyncTestUtil.getTsFilePaths(tmpDir);
    for (File f : tsFiles) {
      PipeData pipeData = new TsFilePipeData(f.getPath(), serialNum++);
      pipeData.serialize(pipeLogOutput);
    }
    pipeLogOutput.close();

    Deletion deletion = new Deletion(new PartialPath("root.vehicle.**"), 0, 33, 38);
    PipeData pipeData = new DeletionPipeData(deletion, serialNum++);
    BufferedPipeDataQueue pipeDataQueue =
        BufferedPipeDataQueue.getInstance(
            SyncPathUtil.getReceiverPipeLogDir(pipeName1, remoteIp1, createdTime1));
    pipeDataQueue.offer(pipeData);

    // 3. create and start collector
    Collector collector = new Collector();
    collector.startCollect();

    // 4. start collect pipe
    collector.startPipe(pipeName1, remoteIp1, createdTime1);

    // 5. if all pipeData has been loaded into IoTDB, check result
    CountDownLatch latch = new CountDownLatch(1);
    ExecutorService es1 = Executors.newSingleThreadExecutor();
    int finalSerialNum = serialNum - 1;
    es1.execute(
        () -> {
          while (true) {
            if (pipeDataQueue.getCommitSerialNumber() == finalSerialNum) {
              break;
            }
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          latch.countDown();
        });
    es1.shutdown();
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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
    // 5.2 check aligned timeseries
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

    // 6. stop pipe, check interrupt thread
    collector.stopPipe(pipeName1, remoteIp1, createdTime1);
    Thread.sleep(1000);
    Deletion deletion1 = new Deletion(new PartialPath("root.vehicle.**"), 0, 0, 99);
    PipeData pipeData1 = new DeletionPipeData(deletion1, serialNum++);
    pipeDataQueue.offer(pipeData1);
    Thread.sleep(1000);
    SyncTestUtil.checkResult(sql1, columnNames1, retArray1);

    // 7. stop collector, check release thread pool
    collector.stopCollect();
    Thread.sleep(1000);
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains(ThreadName.SYNC_RECEIVER_COLLECTOR.getName())) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testMultiplePipe() throws Exception {
    // 1. restart IoTDB
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();

    // 2. prepare pipelog and pipeDataQueue
    if (!pipeLogDir1.exists()) {
      pipeLogDir1.mkdirs();
    }
    if (!pipeLogDir2.exists()) {
      pipeLogDir2.mkdirs();
    }
    // 2.1 prepare for pipe1
    DataOutputStream outputStream =
        new DataOutputStream(
            new FileOutputStream(new File(pipeLogDir1, SyncConstant.COMMIT_LOG_NAME), true));
    outputStream.writeLong(-1);
    outputStream.close();
    int serialNum1 = 0;
    File pipeLog1 = new File(pipeLogDir1.getPath(), SyncConstant.getPipeLogName(serialNum1));
    DataOutputStream pipeLogOutput = new DataOutputStream(new FileOutputStream(pipeLog1, false));
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
    for (PhysicalPlan plan : planList) {
      PipeData pipeData = new SchemaPipeData(plan, serialNum1++);
      pipeData.serialize(pipeLogOutput);
    }
    pipeLogOutput.close();
    File pipeLog2 = new File(pipeLogDir1.getPath(), SyncConstant.getPipeLogName(serialNum1));
    pipeLogOutput = new DataOutputStream(new FileOutputStream(pipeLog2, false));
    List<File> tsFiles =
        SyncTestUtil.getTsFilePaths(new File(tmpDir, "sequence" + File.separator + "root.vehicle"));
    for (File f : tsFiles) {
      PipeData pipeData = new TsFilePipeData(f.getPath(), serialNum1++);
      pipeData.serialize(pipeLogOutput);
    }
    tsFiles =
        SyncTestUtil.getTsFilePaths(
            new File(tmpDir, "unsequence" + File.separator + "root.vehicle"));
    for (File f : tsFiles) {
      PipeData pipeData = new TsFilePipeData(f.getPath(), serialNum1++);
      pipeData.serialize(pipeLogOutput);
    }
    Deletion deletion = new Deletion(new PartialPath("root.vehicle.**"), 0, 33, 38);
    PipeData pipeData = new DeletionPipeData(deletion, serialNum1++);
    pipeData.serialize(pipeLogOutput);
    pipeLogOutput.close();

    // 2.2 prepare for pipe2
    int serialNum2 = 0;
    outputStream =
        new DataOutputStream(
            new FileOutputStream(new File(pipeLogDir2, SyncConstant.COMMIT_LOG_NAME), true));
    outputStream.writeLong(-1);
    outputStream.close();
    pipeLog1 = new File(pipeLogDir2.getPath(), SyncConstant.getPipeLogName(serialNum2));
    pipeLogOutput = new DataOutputStream(new FileOutputStream(pipeLog1, false));
    pipeData =
        new SchemaPipeData(new SetStorageGroupPlan(new PartialPath("root.sg1")), serialNum2++);
    pipeData.serialize(pipeLogOutput);
    pipeData =
        new SchemaPipeData(
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
                null),
            serialNum2++);
    pipeData.serialize(pipeLogOutput);
    pipeLogOutput.close();
    pipeLog2 = new File(pipeLogDir2.getPath(), SyncConstant.getPipeLogName(serialNum2));
    pipeLogOutput = new DataOutputStream(new FileOutputStream(pipeLog2, false));
    tsFiles =
        SyncTestUtil.getTsFilePaths(new File(tmpDir, "sequence" + File.separator + "root.sg1"));
    for (File f : tsFiles) {
      pipeData = new TsFilePipeData(f.getPath(), serialNum2++);
      pipeData.serialize(pipeLogOutput);
    }
    tsFiles =
        SyncTestUtil.getTsFilePaths(new File(tmpDir, "unsequence" + File.separator + "root.sg1"));
    for (File f : tsFiles) {
      pipeData = new TsFilePipeData(f.getPath(), serialNum2++);
      pipeData.serialize(pipeLogOutput);
    }
    pipeLogOutput.close();

    // 3. create and start collector
    BufferedPipeDataQueue pipeDataQueue1 =
        BufferedPipeDataQueue.getInstance(
            SyncPathUtil.getReceiverPipeLogDir(pipeName1, remoteIp1, createdTime1));
    BufferedPipeDataQueue pipeDataQueue2 =
        BufferedPipeDataQueue.getInstance(
            SyncPathUtil.getReceiverPipeLogDir(pipeName2, remoteIp2, createdTime2));
    Collector collector = new Collector();
    collector.startCollect();

    // 4. start collect pipe
    collector.startPipe(pipeName1, remoteIp1, createdTime1);
    collector.startPipe(pipeName2, remoteIp2, createdTime2);

    // 5. if all pipeData has been loaded into IoTDB, check result
    CountDownLatch latch = new CountDownLatch(2);
    ExecutorService es1 = Executors.newSingleThreadExecutor();
    int finalSerialNum1 = serialNum1 - 1;
    int finalSerialNum2 = serialNum2 - 1;
    es1.execute(
        () -> {
          while (true) {
            if (pipeDataQueue1.getCommitSerialNumber() == finalSerialNum1) {
              break;
            }
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          latch.countDown();
          while (true) {
            if (pipeDataQueue2.getCommitSerialNumber() == finalSerialNum2) {
              break;
            }
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          latch.countDown();
        });
    es1.shutdown();
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
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
    // 5.2 check aligned timeseries
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

    // 6. stop pipe, check interrupt thread
    collector.stopPipe(pipeName1, remoteIp1, createdTime1);
    collector.stopPipe(pipeName2, remoteIp2, createdTime2);
    Thread.sleep(1000);
    Deletion deletion1 = new Deletion(new PartialPath("root.vehicle.**"), 0, 0, 99);
    PipeData pipeData1 = new DeletionPipeData(deletion1, serialNum1++);
    pipeDataQueue1.offer(pipeData1);
    Thread.sleep(1000);
    SyncTestUtil.checkResult(sql1, columnNames1, retArray1);

    // 7. stop collector, check release thread pool
    collector.stopCollect();
    Thread.sleep(1000);
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains(ThreadName.SYNC_RECEIVER_COLLECTOR.getName())) {
        Assert.fail();
      }
    }
  }
}
