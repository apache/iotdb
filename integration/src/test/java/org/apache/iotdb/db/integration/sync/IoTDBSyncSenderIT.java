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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.qp.physical.sys.ShowPipeSinkTypePlan;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.sync.common.LocalSyncInfoFetcher;
import org.apache.iotdb.db.sync.pipedata.DeletionPipeData;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.SchemaPipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.IoTDBPipeSink;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.wal.recover.WALRecoverManager;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
@Category({LocalStandaloneTest.class})
public class IoTDBSyncSenderIT {
  private boolean enableSeqSpaceCompaction;
  private boolean enableUnseqSpaceCompaction;
  private boolean enableCrossSpaceCompaction;

  private static final long waitTime = 2000; // 2000 ok1

  private static final String pipeSinkName = "test_pipesink";
  private static final String pipeName = "test_pipe";

  private MockSyncClient syncClient;

  private final Map<String, List<PipeData>> resultMap = new HashMap<>();
  private static final TsFilePipeData simpleTsFilePipeData =
      new TsFilePipeData("path", "tsfile", 0L);
  private static final SchemaPipeData simpleSchemaPipeData =
      new SchemaPipeData(new ShowPipeSinkTypePlan(), 0L);
  private static final DeletionPipeData simpleDeletionPipeData =
      new DeletionPipeData(new Deletion(new PartialPath(), 0L, 0L), 0L);

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

    IoTDBPipeSink pipeSink = new IoTDBPipeSink(pipeSinkName);
    syncClient = new MockSyncClient();
    LocalSyncInfoFetcher.getInstance().reset();
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

  private void prepareSchema() throws Exception { // 8 schema plans
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

    List<PipeData> resultList = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      resultList.add(simpleSchemaPipeData);
    }
    resultMap.put("schemaWithDel3InHistory", resultList); // del3 in history

    resultList = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      resultList.add(simpleSchemaPipeData);
    }
    resultMap.put("schema", resultList); // del3 do not in history
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

    resultMap.put("ins1", Collections.singletonList(simpleTsFilePipeData));
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

    resultMap.put("ins2", Collections.singletonList(simpleTsFilePipeData));
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

    resultMap.put(
        "ins3",
        Arrays.asList(
            simpleTsFilePipeData, simpleTsFilePipeData, simpleTsFilePipeData)); // del3 in history
    resultMap.put(
        "ins3WithDel3InHistory",
        Arrays.asList(simpleTsFilePipeData, simpleTsFilePipeData)); // del3 do not in history
  }

  private void prepareIns4() throws Exception { // ins unsealed tsfile
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(300, 300, 316.25, 'i')");
      statement.execute(
          "insert into root.sg1.d1(timestamp, s1, s2, s3) values(165, 165, 165.25, 'j')");
    }

    resultMap.put("ins4", Arrays.asList(simpleTsFilePipeData, simpleTsFilePipeData));
  }

  private void prepareDel1() throws Exception { // after ins1, add 2 deletions
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.sg1.d1.s1 where time == 3");
      statement.execute("delete from root.sg1.d1.s2 where time >= 1 and time <= 2");
    }

    resultMap.put("del1", Arrays.asList(simpleDeletionPipeData, simpleDeletionPipeData));
  }

  private void prepareDel2() throws Exception { // after ins2, add 3 deletions
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.sg1.d1.s3 where time <= 65");
    }

    resultMap.put(
        "del2",
        Arrays.asList(simpleDeletionPipeData, simpleDeletionPipeData, simpleDeletionPipeData));
    resultMap.put("del2WithoutIns3", Arrays.asList(simpleDeletionPipeData, simpleDeletionPipeData));
  }

  private void prepareDel3() throws Exception { // after ins3, add 5 deletions, 2 schemas
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.sg1.d1.* where time <= 2");
      statement.execute("delete timeseries root.sg1.d2.*");
      statement.execute("delete storage group root.sg2");
    }

    List<PipeData> resultList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      resultList.add(simpleDeletionPipeData);
    }
    for (int i = 0; i < 2; i++) {
      resultList.add(simpleSchemaPipeData);
    }
    resultMap.put("del3", resultList);
  }

  private void preparePipeAndSetMock() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("create pipesink " + pipeSinkName + " as iotdb");
      statement.execute("create pipe " + pipeName + " to " + pipeSinkName);
    }
    SyncService.getInstance().getSenderManager().setSyncClient(syncClient);
  }

  private void restart() throws Exception {
    //    EnvironmentUtils.restartDaemon();
    EnvironmentUtils.shutdownDaemon();
    WALRecoverManager.getInstance().clear();
    EnvironmentUtils.reactiveDaemon();
    SyncService.getInstance().getSenderManager().setSyncClient(syncClient);
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

  private void checkInsOnlyResult(List<PipeData> list) { // check ins1, ins2, ins3
    Assert.assertEquals(13, list.size());
    for (int i = 0; i < 8; i++) {
      Assert.assertTrue(list.get(i) instanceof SchemaPipeData);
    }
    for (int i = 9; i < list.size(); i++) {
      Assert.assertTrue(list.get(i) instanceof TsFilePipeData);
    }
  }

  private void checkResult(List<String> resultString, List<PipeData> list) {
    int totalNumber = 0;
    for (String string : resultString) {
      totalNumber += resultMap.get(string).size();
    }
    Assert.assertEquals(totalNumber, list.size());
    int cnt = 0;
    for (String string : resultString) {
      for (PipeData pipeData : resultMap.get(string)) {
        Assert.assertEquals(pipeData.getType(), list.get(cnt++).getType());
      }
    }
  }

  @Test
  public void testHistoryInsert() {
    try {
      prepareSchema(); // history
      prepareIns1();
      prepareIns2();
      prepareIns3();

      preparePipeAndSetMock(); // realtime
      startPipe();

      Thread.sleep(waitTime); // check
      checkInsOnlyResult(syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testHistoryAndRealTimeInsert() {
    try {
      prepareSchema(); // history
      prepareIns1();
      prepareIns2();

      preparePipeAndSetMock(); // realtime
      startPipe();
      Thread.sleep(waitTime);
      prepareIns3();

      Thread.sleep(1000L); // check
      checkInsOnlyResult(syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testStopAndStartInsert() {
    try {
      prepareSchema(); // history
      prepareIns1();

      preparePipeAndSetMock(); // realtime
      startPipe();
      prepareIns2();
      stopPipe();
      prepareIns3();
      startPipe();

      Thread.sleep(waitTime); // check
      checkInsOnlyResult(syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testRealTimeAndStopInsert() {
    try {
      preparePipeAndSetMock(); // realtime
      startPipe();
      prepareSchema();
      prepareIns1();
      stopPipe();
      prepareIns2();
      startPipe();
      prepareIns3();
      stopPipe();

      Thread.sleep(waitTime); // check
      checkInsOnlyResult(syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testHistoryDel() {
    try {
      prepareSchema(); // history
      prepareIns1();
      prepareIns2();
      prepareIns3();
      prepareDel1();
      prepareDel2();
      prepareDel3();

      preparePipeAndSetMock(); // realtime
      startPipe();

      Thread.sleep(waitTime); // check
      checkResult(
          Arrays.asList("schemaWithDel3InHistory", "ins1", "ins2", "ins3WithDel3InHistory"),
          syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  @Ignore
  public void testRealtimeDel() {
    try {
      prepareSchema(); // history
      prepareIns1();

      preparePipeAndSetMock(); // realtime
      startPipe();
      prepareIns2();
      prepareDel1();
      stopPipe();
      prepareIns3();
      startPipe();
      prepareDel2();
      prepareDel3();
      stopPipe();

      Thread.sleep(waitTime); // check
      checkResult(
          Arrays.asList("schema", "ins1", "ins2", "del1", "ins3", "del2", "del3"),
          syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testRestartWhileRunning() {
    try {
      prepareSchema(); // history
      prepareIns1();
      prepareIns2();

      preparePipeAndSetMock(); // realtime
      startPipe();
      restart();
      prepareIns3();

      Thread.sleep(waitTime); // check
      checkInsOnlyResult(syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testRestartWhileStopping() {
    try {
      prepareSchema(); // history
      prepareIns1();

      preparePipeAndSetMock(); // realtime
      startPipe();
      prepareIns2();
      stopPipe();
      restart();
      prepareIns3();
      startPipe();

      Thread.sleep(waitTime); // check
      checkInsOnlyResult(syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testRestartWithDel() {
    try {
      prepareSchema(); // history
      prepareIns1();
      prepareDel1();

      preparePipeAndSetMock(); // realtime
      startPipe();
      prepareIns2();
      stopPipe();
      prepareDel2();
      restart();
      startPipe();
      prepareIns3();
      stopPipe();
      prepareDel3();
      startPipe();

      Thread.sleep(waitTime); // check
      checkResult(
          Arrays.asList("schema", "ins1", "ins2", "del2WithoutIns3", "ins3", "del3"),
          syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }

  @Test
  public void testRestartWithUnsealedTsFile() {
    try {
      prepareSchema(); // history
      prepareIns1();
      prepareIns2();
      prepareDel1();

      preparePipeAndSetMock(); // realtime
      startPipe();
      stopPipe();
      prepareDel2();
      restart();
      startPipe();
      prepareIns3();
      stopPipe();
      prepareDel3();
      prepareIns4();
      startPipe();
      restart();

      Thread.sleep(waitTime); // check
      checkResult(
          Arrays.asList("schema", "ins1", "ins2", "del2WithoutIns3", "ins3", "del3", "ins4"),
          syncClient.getPipeDataList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      try {
        dropPipe();
        Thread.sleep(waitTime);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
  }
}
