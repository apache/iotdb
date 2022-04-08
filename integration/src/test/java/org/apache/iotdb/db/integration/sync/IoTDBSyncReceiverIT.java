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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.sync.PipeServerException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.newsync.pipedata.PipeData;
import org.apache.iotdb.db.newsync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.newsync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.newsync.receiver.ReceiverService;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe;
import org.apache.iotdb.db.newsync.sender.pipe.Pipe.PipeStatus;
import org.apache.iotdb.db.newsync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.newsync.transport.client.TransportClient;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.*;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.service.transport.thrift.RequestType;
import org.apache.iotdb.service.transport.thrift.ResponseType;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.service.transport.thrift.SyncResponse;
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

import java.io.File;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
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

  TransportClient client;

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
    try {
      ReceiverService.getInstance().startPipeServer();
      new Socket("localhost", 6670).close();
    } catch (Exception e) {
      Assert.fail("Failed to start pipe server because " + e.getMessage());
    }
    Pipe pipe = new TsFilePipe(createdTime1, pipeName1, null, 0, false);
    client = new TransportClient(pipe, "127.0.0.1", 6670);
    remoteIp1 = InetAddress.getLocalHost().getHostAddress();
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

  /** cannot stop */
  @Test
  public void testStopPipeServerCheck() {
    logger.info("testStopPipeServerCheck");
    ReceiverService.getInstance()
        .receiveMsg(new SyncRequest(RequestType.CREATE, pipeName1, remoteIp1, createdTime1));
    ReceiverService.getInstance()
        .receiveMsg(new SyncRequest(RequestType.START, pipeName1, remoteIp1, createdTime1));
    try {
      ReceiverService.getInstance().stopPipeServer();
      Assert.fail("Should not stop pipe server");
    } catch (PipeServerException e) {
      // nothing
    }
    ReceiverService.getInstance()
        .receiveMsg(new SyncRequest(RequestType.DROP, pipeName1, remoteIp1, createdTime1));
  }

  @Test
  public void testPipeOperation() {
    logger.info("testPipeOperation");
    String[] columnNames = {"create time", "name", "role", "remote", "status", "message"};
    String showPipeSql = "SHOW PIPE";
    try {
      // create
      client.heartbeat(new SyncRequest(RequestType.CREATE, pipeName1, remoteIp1, createdTime1));
      String[] retArray =
          new String[] {
            String.format(
                "%s,%s,%s,%s,%s,%s",
                DatetimeUtils.convertLongToDate(createdTime1),
                pipeName1,
                "receiver",
                remoteIp1,
                PipeStatus.STOP.name(),
                "")
          };
      SyncTestUtil.checkResult(showPipeSql, columnNames, retArray, false);
      // start
      client.heartbeat(new SyncRequest(RequestType.START, pipeName1, remoteIp1, createdTime1));
      retArray =
          new String[] {
            String.format(
                "%s,%s,%s,%s,%s,%s",
                DatetimeUtils.convertLongToDate(createdTime1),
                pipeName1,
                "receiver",
                remoteIp1,
                PipeStatus.RUNNING.name(),
                "")
          };
      SyncTestUtil.checkResult(showPipeSql, columnNames, retArray, false);
      // stop
      client.heartbeat(new SyncRequest(RequestType.STOP, pipeName1, remoteIp1, createdTime1));
      retArray =
          new String[] {
            String.format(
                "%s,%s,%s,%s,%s,%s",
                DatetimeUtils.convertLongToDate(createdTime1),
                pipeName1,
                "receiver",
                remoteIp1,
                PipeStatus.STOP.name(),
                "")
          };
      SyncTestUtil.checkResult(showPipeSql, columnNames, retArray, false);
      // drop
      client.heartbeat(new SyncRequest(RequestType.DROP, pipeName1, remoteIp1, createdTime1));
      retArray =
          new String[] {
            String.format(
                "%s,%s,%s,%s,%s,%s",
                DatetimeUtils.convertLongToDate(createdTime1),
                pipeName1,
                "receiver",
                remoteIp1,
                PipeStatus.DROP.name(),
                "")
          };
      SyncTestUtil.checkResult(showPipeSql, columnNames, retArray, false);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testReceiveDataAndLoad() {
    logger.info("testReceiveDataAndLoad");
    try {
      // 1. create pipe
      client.heartbeat(new SyncRequest(RequestType.CREATE, pipeName1, remoteIp1, createdTime1));
      client.heartbeat(new SyncRequest(RequestType.START, pipeName1, remoteIp1, createdTime1));

      // 2. send pipe data
      int serialNum = 0;
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
              null,
              null,
              null));
      planList.add(new SetStorageGroupPlan(new PartialPath("root.sg1")));
      for (PhysicalPlan plan : planList) {
        client.senderTransport(new SchemaPipeData(plan, serialNum++));
      }
      List<File> tsFiles = SyncTestUtil.getTsFilePaths(tmpDir);
      for (File f : tsFiles) {
        client.senderTransport(new TsFilePipeData(f.getPath(), serialNum++));
      }
      Deletion deletion = new Deletion(new PartialPath("root.vehicle.**"), 0, 33, 38);
      PipeData pipeData = new DeletionPipeData(deletion, serialNum++);
      client.senderTransport(pipeData);

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

      // 4. stop pipe
      client.heartbeat(new SyncRequest(RequestType.STOP, pipeName1, remoteIp1, createdTime1));
      Thread.sleep(500);
      client.senderTransport(
          new DeletionPipeData(
              new Deletion(new PartialPath("root.vehicle.**"), 0, 0, 99), serialNum++));
      Thread.sleep(1000);
      SyncTestUtil.checkResult(sql1, columnNames1, retArray1);
      // check heartbeat
      SyncResponse response1 =
          ReceiverService.getInstance()
              .receiveMsg(
                  new SyncRequest(RequestType.HEARTBEAT, pipeName1, remoteIp1, createdTime1));
      Assert.assertEquals(ResponseType.WARN, response1.type);

      // 5. restart pipe
      client.heartbeat(new SyncRequest(RequestType.START, pipeName1, remoteIp1, createdTime1));
      Thread.sleep(1000);
      SyncTestUtil.checkResult(sql1, columnNames1, new String[] {});
      // check heartbeat
      SyncResponse response2 =
          ReceiverService.getInstance()
              .receiveMsg(
                  new SyncRequest(RequestType.HEARTBEAT, pipeName1, remoteIp1, createdTime1));
      Assert.assertEquals(ResponseType.INFO, response2.type);

      // 6. drop pipe
      client.heartbeat(new SyncRequest(RequestType.DROP, pipeName1, remoteIp1, createdTime1));
      Thread.sleep(500);
      client.senderTransport(
          new DeletionPipeData(
              new Deletion(new PartialPath("root.sg1.**"), 0, 0, 99), serialNum++));
      Thread.sleep(1000);
      SyncTestUtil.checkResult(sql2, columnNames2, retArray2);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
