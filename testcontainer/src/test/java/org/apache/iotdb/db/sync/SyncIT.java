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
package org.apache.iotdb.db.sync;

import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SyncIT {
  private static Logger receiverLogger = LoggerFactory.getLogger("iotdb-receiver_1");
  private static Logger senderLogger = LoggerFactory.getLogger("iotdb-sender_1");
  private static int RETRY_TIME = 30;

  protected Statement senderStatement;
  protected Connection senderConnection;
  protected Statement receiverStatement;
  protected Connection receiverConnection;

  // in TestContainer's document, it is @ClassRule, and the environment is `public static`
  // I am not sure the difference now.
  @Rule
  public DockerComposeContainer environment =
      new NoProjectNameDockerComposeContainer(
              "sync", new File("src/test/resources/sync/docker-compose.yaml"))
          .withExposedService("iotdb-sender_1", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-sender_1", new Slf4jLogConsumer(senderLogger))
          .withExposedService("iotdb-receiver_1", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-receiver_1", new Slf4jLogConsumer(receiverLogger))
          .withLocalCompose(true);

  protected int getSenderRpcPort() {
    return environment.getServicePort("iotdb-sender_1", 6667);
  }

  protected String getSenderIp() {
    return environment.getServiceHost("iotdb-sender_1", 6667);
  }

  protected int getReceiverRpcPort() {
    return environment.getServicePort("iotdb-receiver_1", 6667);
  }

  protected String getReceiverIp() {
    return environment.getServiceHost("iotdb-receiver_1", 6667);
  }

  @Before
  public void init() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    senderConnection =
        DriverManager.getConnection(
            "jdbc:iotdb://" + getSenderIp() + ":" + getSenderRpcPort(), "root", "root");
    senderStatement = senderConnection.createStatement();
    receiverConnection =
        DriverManager.getConnection(
            "jdbc:iotdb://" + getReceiverIp() + ":" + getReceiverRpcPort(), "root", "root");
    receiverStatement = receiverConnection.createStatement();
  }

  @After
  public void clean() throws Exception {
    senderStatement.close();
    senderConnection.close();
    receiverStatement.close();
    receiverConnection.close();
  }

  private void prepareSchema() throws Exception {
    senderStatement.execute("CREATE DATABASE root.sg1");
    senderStatement.execute("CREATE DATABASE root.sg2");
    senderStatement.execute("create timeseries root.sg1.d1.s1 with datatype=int32, encoding=PLAIN");
    senderStatement.execute("create timeseries root.sg1.d1.s2 with datatype=float, encoding=RLE");
    senderStatement.execute("create timeseries root.sg1.d1.s3 with datatype=TEXT, encoding=PLAIN");
    senderStatement.execute("create timeseries root.sg1.d2.s4 with datatype=int64, encoding=PLAIN");
    senderStatement.execute("create timeseries root.sg2.d1.s0 with datatype=int32, encoding=PLAIN");
    senderStatement.execute(
        "create timeseries root.sg2.d2.s1 with datatype=boolean, encoding=PLAIN");
  }

  /* add one seq tsfile in sg1 */
  private void prepareIns1() throws Exception {
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(1, 1, 16.0, 'a')");
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(2, 2, 25.16, 'b')");
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(3, 3, 65.25, 'c')");
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(16, 25, 100.0, 'd')");
    senderStatement.execute("insert into root.sg1.d2(timestamp, s4) values(1, 1)");
    senderStatement.execute("flush");
  }

  /* add one seq tsfile in sg1 */
  private void prepareIns2() throws Exception {
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(100, 65, 16.25, 'e')");
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(65, 100, 25.0, 'f')");
    senderStatement.execute("insert into root.sg1.d2(timestamp, s4) values(200, 100)");
    senderStatement.execute("flush");
  }

  /* add one seq tsfile in sg1, one unseq tsfile in sg1, one seq tsfile in sg2 */
  private void prepareIns3() throws Exception {
    senderStatement.execute("insert into root.sg2.d1(timestamp, s0) values(100, 100)");
    senderStatement.execute("insert into root.sg2.d1(timestamp, s0) values(65, 65)");
    senderStatement.execute("insert into root.sg2.d2(timestamp, s1) values(1, true)");
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(25, 16, 65.16, 'g')");
    senderStatement.execute(
        "insert into root.sg1.d1(timestamp, s1, s2, s3) values(200, 100, 16.65, 'h')");
    senderStatement.execute("flush");
  }

  private void prepareDel1() throws Exception { // after ins1, add 2 deletions
    senderStatement.execute("delete from root.sg1.d1.s1 where time == 3");
    senderStatement.execute("delete from root.sg1.d1.s2 where time >= 1 and time <= 2");
  }

  private void prepareDel2() throws Exception { // after ins2, add 3 deletions
    senderStatement.execute("delete from root.sg1.d1.s3 where time <= 65");
  }

  private void prepareDel3() throws Exception { // after ins3, add 5 deletions, 2 schemas{
    senderStatement.execute("delete from root.sg1.d1.* where time <= 2");
    senderStatement.execute("delete timeseries root.sg1.d2.*");
    senderStatement.execute("delete database root.sg2");
  }

  private void preparePipe() throws Exception {
    receiverStatement.execute("start pipeserver");
    senderStatement.execute(
        "create pipesink my_iotdb as iotdb(ip='sync_iotdb-receiver_1',port=6670)");
    senderStatement.execute("create pipe p to my_iotdb");
  }

  private void startPipe() throws Exception {
    senderStatement.execute("start pipe p");
  }

  private void stopPipe() throws Exception {
    senderStatement.execute("stop pipe p");
  }

  private void dropPipe() throws Exception {
    senderStatement.execute("drop pipe p");
  }

  private void checkResult() throws Exception {
    String[] columnNames =
        new String[] {
          "root.sg1.d1.s3",
          "root.sg1.d1.s1",
          "root.sg1.d1.s2",
          "root.sg1.d2.s4",
          "root.sg2.d1.s0",
          "root.sg2.d2.s1"
        };
    String[] results =
        new String[] {
          "1,a,1,16.0,1,null,true",
          "2,b,2,25.16,null,null,null",
          "3,c,3,65.25,null,null,null",
          "16,d,25,100.0,null,null,null",
          "25,g,16,65.16,null,null,null",
          "65,f,100,25.0,null,65,null",
          "100,e,65,16.25,null,100,null",
          "200,h,100,16.65,100,null,null"
        };
    checkResult(receiverStatement, "select ** from root", columnNames, results, true);
  }

  private void checkResultWithDeletion() throws Exception {
    String[] columnNames =
        new String[] {
          "root.sg1.d1.s3", "root.sg1.d1.s1", "root.sg1.d1.s2",
        };
    String[] results =
        new String[] {
          "3,null,null,65.25",
          "16,null,25,100.0",
          "25,null,16,65.16",
          "65,null,100,25.0",
          "100,e,65,16.25",
          "200,h,100,16.65"
        };
    checkResult(receiverStatement, "select ** from root", columnNames, results, true);
  }

  @Test
  public void testCreatePipe() throws Exception {
    preparePipe();
    checkResult(
        receiverStatement,
        "show pipe",
        new String[] {"name", "role", "status"},
        new String[] {"p,receiver,STOP"},
        false);
    dropPipe();
    checkResult(
        senderStatement,
        "show pipe",
        new String[] {"name", "role", "remote", "status"},
        new String[] {"p,sender,my_iotdb,DROP"},
        false);
  }

  @Test
  public void testHistoryInsert() {
    try {
      prepareSchema();
      prepareIns1();
      prepareIns2();
      prepareIns3();
      preparePipe();
      startPipe();
      checkResult();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testHistoryAndRealTimeInsert() {
    try {
      prepareSchema();
      prepareIns1();
      prepareIns2();
      preparePipe();
      startPipe();
      prepareIns3();
      checkResult();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testStopAndStartInsert() {
    try {
      prepareSchema();
      prepareIns1();
      preparePipe();
      startPipe();
      prepareIns2();
      stopPipe();
      prepareIns3();
      startPipe();
      checkResult();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testRealTimeAndStopInsert() {
    try {
      preparePipe(); // realtime
      startPipe();
      prepareSchema();
      prepareIns1();
      stopPipe();
      prepareIns2();
      startPipe();
      prepareIns3();
      checkResult();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
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
      preparePipe(); // realtime
      startPipe();
      checkResultWithDeletion();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testRealtimeDel() {
    try {
      prepareSchema(); // history
      prepareIns1();
      preparePipe(); // realtime
      startPipe();
      prepareIns2();
      prepareDel1();
      stopPipe();
      prepareIns3();
      startPipe();
      prepareDel2();
      prepareDel3();
      checkResultWithDeletion();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  /**
   * Execute sql in IoTDB and compare resultSet with expected result. This method only check columns
   * that is explicitly declared in columnNames. This method will compare expected result with
   * actual result RETRY_TIME times. Interval of each run is 1000ms.
   *
   * @param statement Statement of IoTDB.
   * @param sql SQL to be executed.
   * @param columnNames Columns to be compared with.
   * @param retArray Expected result set. Order of columns is as same as columnNames.
   * @param hasTimeColumn If result set contains time column (e.g. timeserires query), set
   *     hasTimeColumn = true.
   */
  private static void checkResult(
      Statement statement,
      String sql,
      String[] columnNames,
      String[] retArray,
      boolean hasTimeColumn)
      throws Exception {
    loop:
    for (int loop = 0; loop < RETRY_TIME; loop++) {
      try {
        Thread.sleep(1000);
        boolean hasResultSet = statement.execute(sql);
        if (!assertOrCompareEqual(true, hasResultSet, loop)) {
          continue;
        }
        ResultSet resultSet = statement.getResultSet();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          if (hasTimeColumn) {
            builder.append(resultSet.getString(1)).append(",");
          }
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(resultSet.getString(index)).append(",");
          }
          if (builder.length() > 0) {
            builder.deleteCharAt(builder.length() - 1);
          }
          if (!assertOrCompareEqual(retArray[cnt], builder.toString(), loop)) {
            continue loop;
          }
          cnt++;
        }
        if (!assertOrCompareEqual(retArray.length, cnt, loop)) {
          continue;
        }
        return;
      } catch (Exception e) {
        if (loop == RETRY_TIME - 1) {
          throw e;
        }
      }
    }
    Assert.fail();
  }

  private static boolean assertOrCompareEqual(Object expected, Object actual, int loop) {
    if (loop == RETRY_TIME - 1) {
      assertEquals(expected, actual);
      return true;
    } else {
      return expected.equals(actual);
    }
  }
}
