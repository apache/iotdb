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
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.TsFilePipe;
import org.apache.iotdb.db.sync.sender.service.SenderService;
import org.apache.iotdb.db.sync.sender.service.TransportHandler;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

@Category({LocalStandaloneTest.class})
public class IoTDBSyncSenderIT {
  private boolean enableSeqSpaceCompaction;
  private boolean enableUnseqSpaceCompaction;
  private boolean enableCrossSpaceCompaction;

  private static final String pipeSinkName = "test_pipesink";
  private static final String pipeName = "test_pipe";

  private TsFilePipe pipe;
  private TransportClientMock mock;

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
    Class.forName(Config.JDBC_DRIVER_NAME);
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
    EnvironmentUtils.shutdownDaemon();
    EnvironmentUtils.cleanEnv();
  }

  private void prepareSchema() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("set storage group to root.sg1");
      statement.execute("set storage group to root.sg2");
      statement.execute("create timeseries root.sg1.d1.s1 with datatype=int32, encoding=PLAIN");
      statement.execute("create timeseries root.sg1.d1.s2 with datatype=float, encoding=RLE");
      statement.execute("create timeseries root.sg1.d1.s3 with datatype=TEXT, encoding=PLAIN");
      statement.execute("create timeseries root.sg1.d2.s4 with datatype=int64, encoding=PLAIN");
      statement.execute("create timeseries root.sg2.d1.s0 with datatype=int32, encoding=PLAIN");
      statement.execute("create timeseries root.sg2.d2.s1 with datatype=boolean, encoding=PLAIN");
    }
  }

  private void prepareIns1() throws Exception { // add one seq tsfile in sg1
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg1.d1(timestamp, s1, s2, s3) values(1, 1, 16.0, 'a')");
      statement.execute("insert into root.sg1.d1(timestamp, s1, s2, s3) values(2, 2, 25.16, 'b')");
      statement.execute("insert into root.sg1.d1(timestamp, s1, s2, s3) values(3, 3, 65.25, 'c')");
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(16, 25, 100.0, 'd')");
      statement.execute("insert into root.sg1.d2(timestamp, s4) values(1, 1)");
      statement.execute("flush");
    }
  }

  private void prepareIns2() throws Exception { // add one seq tsfile in sg1
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(100, 65, 16.25, 'e')");
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(65, 100, 25.0, 'f')");
      statement.execute("insert into root.sg1.d2(timestamp, s4) values(200, 100)");
      statement.execute("flush");
    }
  }

  private void prepareIns3()
      throws
          Exception { // add one seq tsfile in sg1, one unseq tsfile in sg1, one seq tsfile in sg2
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg2.d1(timestamp, s0) values(100, 100)");
      statement.execute("insert into root.sg2.d1(timestamp, s0) values(65, 65)");
      statement.execute("insert into root.sg2.d2(timestamp, s1) values(1, true)");
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(25, 16, 65.16, 'g')");
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(200, 100, 16.65, 'h')");
      statement.execute("flush");
    }
  }

  private void preparePipeAndSetMock() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("start pipeserver");
      statement.execute("create pipesink " + pipeSinkName + " as iotdb");
      statement.execute("create pipe " + pipeName + " to " + pipeSinkName);
      pipe = (TsFilePipe) SenderService.getInstance().getRunningPipe();
      mock =
          new TransportClientMock(SenderService.getInstance().getRunningPipe(), "127.0.0.1", 2333);
      TransportHandler handler =
          new TransportHandler(
              mock, pipeName, SenderService.getInstance().getRunningPipe().getCreateTime());
      SenderService.getInstance().setTransportHandler(handler);
      Thread.sleep(1000L);
      statement.execute("stop pipeserver");
    }
  }

  private void startPipe() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("start pipe " + pipeName);
    }
  }

  private void stopPipe() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("stop pipe " + pipeName);
    }
  }

  private void dropPipe() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("drop pipe " + pipeName);
    }
  }

  private void checkResult(List<PipeData> list) { // check ins1, ins2, ins3
    Assert.assertEquals(list.size(), 13);
    for (int i = 0; i < 8; i++) {
      Assert.assertTrue(list.get(i) instanceof SchemaPipeData);
    }
    for (int i = 9; i < list.size(); i++) {
      Assert.assertTrue(list.get(i) instanceof TsFilePipeData);
    }
  }

  @Test
  public void testHistoryInsert() {
    try {
      prepareSchema();
      prepareIns1();
      prepareIns2();
      prepareIns3();

      preparePipeAndSetMock();
      startPipe();
      Thread.sleep(1000L);
      checkResult(mock.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(1000L);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testHistoryAndRealTimeInsert() {
    try {
      prepareSchema();
      prepareIns1();
      prepareIns2();

      preparePipeAndSetMock();
      startPipe();
      Thread.sleep(1000L);
      prepareIns3();
      Thread.sleep(1000L);
      checkResult(mock.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(1000L);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testStopAndStartInsert() {
    try {
      prepareSchema();
      prepareIns1();

      preparePipeAndSetMock();
      startPipe();
      prepareIns2();
      stopPipe();
      prepareIns3();
      startPipe();
      Thread.sleep(1000L);
      checkResult(mock.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(1000L);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testRealTimeSchemaAndStopInsert() {
    try {
      preparePipeAndSetMock();
      prepareSchema();
      startPipe();
      prepareIns1();
      stopPipe();
      prepareIns2();
      startPipe();
      prepareIns3();
      stopPipe();

      Thread.sleep(1000L);
      checkResult(mock.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(1000L);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }
}
