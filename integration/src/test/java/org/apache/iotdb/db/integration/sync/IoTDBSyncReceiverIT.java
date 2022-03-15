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
import org.apache.iotdb.db.exception.sync.PipeServerException;
import org.apache.iotdb.db.newsync.receiver.ReceiverService;
import org.apache.iotdb.db.newsync.receiver.manager.PipeStatus;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeServerPlan;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.service.transport.thrift.RequestType;
import org.apache.iotdb.service.transport.thrift.SyncRequest;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

@Category({LocalStandaloneTest.class})
public class IoTDBSyncReceiverIT {

  /** create tsfile and move to tmpDir for sync test */
  File tmpDir = new File("target/synctest");

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;

  String pipeName1 = "pipe1";
  String remoteIp1 = "192.168.0.11";
  long createdTime1 = System.currentTimeMillis();

  String pipeName2 = "pipe2";
  String remoteIp2 = "192.168.0.22";
  long createdTime2 = System.currentTimeMillis();

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
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.envSetUp();
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
    EnvironmentUtils.cleanEnv();
  }

  /** normal case */
  @Test
  public void testStartAndStopPipeServer1() {
    try {
      ReceiverService.getInstance().startPipeServer();
      try {
        new Socket("localhost", 5555);
      } catch (UnknownHostException | ConnectException e) {
        Assert.fail(e.getMessage());
      }
      ReceiverService.getInstance().stopPipeServer();
      try {
        new Socket("localhost", 5555);
        Assert.fail("Pipe Server is still running.");
      } catch (UnknownHostException | ConnectException e) {
        // nothing
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  /** cannot stop */
  @Test
  public void testStartAndStopPipeServer2() {
    try {
      ReceiverService.getInstance().startPipeServer();
      new Socket("localhost", 5555);
      ReceiverService.getInstance()
          .recMsg(new SyncRequest(RequestType.CREATE, pipeName1, remoteIp1, createdTime1));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
    try {
      ReceiverService.getInstance().stopPipeServer();
      Assert.fail("Should not stop pipe server");
    } catch (PipeServerException e) {
      // nothing
    }
  }

  @Test
  public void testShowPipe() {
    try {
      ReceiverService.getInstance().startPipeServer();
      ReceiverService.getInstance()
          .recMsg(new SyncRequest(RequestType.CREATE, pipeName1, remoteIp1, createdTime1));
      ReceiverService.getInstance()
          .recMsg(new SyncRequest(RequestType.CREATE, pipeName2, remoteIp2, createdTime2));
      ReceiverService.getInstance()
          .recMsg(new SyncRequest(RequestType.STOP, pipeName2, remoteIp2, createdTime2));
      QueryDataSet allDataSet = ReceiverService.getInstance().showPipe(new ShowPipeServerPlan(""));
      while (allDataSet.hasNext()) {
        RowRecord rowRecord = allDataSet.next();
        List<Field> fields = rowRecord.getFields();
        Assert.assertEquals(4, fields.size());
        if (fields.get(0).getStringValue().equals(pipeName1)) {
          Assert.assertEquals(pipeName1, fields.get(0).getStringValue());
          Assert.assertEquals(remoteIp1, fields.get(1).getStringValue());
          Assert.assertEquals(PipeStatus.RUNNING.name(), fields.get(2).getStringValue());
          Assert.assertEquals(
              DatetimeUtils.convertLongToDate(createdTime1), fields.get(3).getStringValue());
        } else {
          Assert.assertEquals(pipeName2, fields.get(0).getStringValue());
          Assert.assertEquals(remoteIp2, fields.get(1).getStringValue());
          Assert.assertEquals(PipeStatus.STOP.name(), fields.get(2).getStringValue());
          Assert.assertEquals(
              DatetimeUtils.convertLongToDate(createdTime2), fields.get(3).getStringValue());
        }
      }
      QueryDataSet pipe2DataSet =
          ReceiverService.getInstance().showPipe(new ShowPipeServerPlan(pipeName2));
      RowRecord rowRecord = pipe2DataSet.next();
      List<Field> fields = rowRecord.getFields();
      Assert.assertEquals(4, fields.size());
      Assert.assertEquals(pipeName2, fields.get(0).getStringValue());
      Assert.assertEquals(remoteIp2, fields.get(1).getStringValue());
      Assert.assertEquals(PipeStatus.STOP.name(), fields.get(2).getStringValue());
      Assert.assertEquals(
          DatetimeUtils.convertLongToDate(createdTime2), fields.get(3).getStringValue());
      Assert.assertFalse(pipe2DataSet.hasNext());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testPipeOperation(){

  }

  @Test
  public void testReceiveDataAndLoad(){

  }

  @Test
  public void testHeartbeat(){

  }
}
