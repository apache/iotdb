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
package org.apache.iotdb.db.newsync;

import org.apache.iotdb.jdbc.Config;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SyncIT {
  private static Logger receiverLogger = LoggerFactory.getLogger("iotdb-receiver_1");
  private static Logger senderLogger = LoggerFactory.getLogger("iotdb-sender_1");

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

  @Test
  public void testCreatePipe() throws Exception {
    receiverStatement.execute("start pipeserver");
    checkResult(
        receiverStatement,
        "show pipeserver",
        new String[] {"enable"},
        new String[] {"true"},
        false);
    senderStatement.execute(
        String.format("create pipesink my_iotdb as iotdb(ip='sync_iotdb-receiver_1',port=6670)"));
    senderStatement.execute("create pipe p to my_iotdb");
    checkResult(
        receiverStatement,
        "show pipe",
        new String[] {"name", "role", "status"},
        new String[] {"p,receiver,STOP"},
        false);
    senderStatement.execute("drop pipe p");
    checkResult(
        senderStatement,
        "show pipe",
        new String[] {"name", "role", "remote", "status"},
        new String[] {"p,sender,my_iotdb,DROP"},
        false);
  }

  public static void checkResult(
      Statement statement,
      String sql,
      String[] columnNames,
      String[] retArray,
      boolean hasTimeColumn)
      throws Exception {
    boolean hasResultSet = statement.execute(sql);
    Assert.assertTrue(hasResultSet);
    ResultSet resultSet = statement.getResultSet();
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    Map<String, Integer> map = new HashMap<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      map.put(resultSetMetaData.getColumnName(i), i);
    }
    //      assertEquals(
    //              hasTimeColumn ? columnNames.length + 1 : columnNames.length,
    //              resultSetMetaData.getColumnCount());
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
      assertEquals(retArray[cnt], builder.toString());
      cnt++;
    }
    assertEquals(retArray.length, cnt);
  }
}
