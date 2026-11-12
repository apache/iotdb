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
package org.apache.iotdb.db.it.cq;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.mpp.rpc.thrift.TExecuteCQ;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCQExecIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCQExecutionWithReadDataPermission() throws Exception {
    testCQExecutionWithReadDataPermission(false);
  }

  @Test
  public void testCQExecutionWithReadDataPermissionAlignByDevice() throws Exception {
    testCQExecutionWithReadDataPermission(true);
  }

  private void testCQExecutionWithReadDataPermission(boolean alignByDevice) throws Exception {
    String cqId = alignByDevice ? "cq_read_auth_aligned" : "cq_read_auth";
    String database = "root." + cqId;
    String username = cqId + "_user";
    String password = "CQRead!Test2026";
    String queryBody =
        "SELECT * INTO "
            + database
            + ".target(::) FROM "
            + database
            + ".source"
            + (alignByDevice ? " ALIGN BY DEVICE" : "");
    String resultQuery = "SELECT s1, s2 FROM " + database + ".target";
    String resultHeader = "Time," + database + ".target.s1," + database + ".target.s2,";

    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER " + username + " '" + password + "'");
      statement.execute("CREATE DATABASE " + database);
      try {
        statement.execute("CREATE TIMESERIES " + database + ".source.s1 WITH DATATYPE=INT64");
        statement.execute("CREATE TIMESERIES " + database + ".source.s2 WITH DATATYPE=INT64");
        statement.execute("CREATE TIMESERIES " + database + ".target.s1 WITH DATATYPE=INT64");
        statement.execute("CREATE TIMESERIES " + database + ".target.s2 WITH DATATYPE=INT64");
        statement.execute(
            "INSERT INTO "
                + database
                + ".source(time, s1, s2) VALUES (1, 11, 21), (1001, 12, 22), (2001, 13, 23)");
        statement.execute("GRANT SYSTEM ON root.** TO USER " + username);
        statement.execute("GRANT WRITE_DATA ON " + database + ".target.** TO USER " + username);
        statement.execute("GRANT READ_DATA ON " + database + ".source.s1 TO USER " + username);

        try (Connection userConnection = EnvFactory.getEnv().getConnection(username, password);
            Statement userStatement = userConnection.createStatement()) {
          // Keep the scheduler idle and execute deterministic windows through its DataNode RPC.
          userStatement.execute(
              "CREATE CQ "
                  + cqId
                  + " RESAMPLE EVERY 1s BOUNDARY "
                  + (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1))
                  + " BEGIN "
                  + queryBody
                  + " END");
        }
        try {
          TExecuteCQ request = new TExecuteCQ(queryBody, 0, 1000, 10000, "UTC", cqId, username);
          executeCQOnAllDataNodes(clients, request);
          // The wildcard must not copy s2, for which the CQ owner has no READ_DATA privilege.
          resultSetEqualTest(resultQuery, resultHeader, new String[] {"1,11,null,"});

          statement.execute("REVOKE READ_DATA ON " + database + ".source.s1 FROM USER " + username);
          request.setStartTime(1000).setEndTime(2000);
          executeCQOnAllDataNodes(clients, request);
          resultSetEqualTest(resultQuery, resultHeader, new String[] {"1,11,null,"});

          statement.execute("GRANT READ_DATA ON " + database + ".source.** TO USER " + username);
          request.setStartTime(2000).setEndTime(3000);
          executeCQOnAllDataNodes(clients, request);
          resultSetEqualTest(resultQuery, resultHeader, new String[] {"1,11,null,", "2001,13,23,"});
        } finally {
          statement.execute("DROP CQ " + cqId);
        }
      } finally {
        statement.execute("DELETE DATABASE " + database);
        statement.execute("DROP USER " + username);
      }
    } finally {
      clients.close();
    }
  }

  private void executeCQOnAllDataNodes(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients, TExecuteCQ request)
      throws Exception {
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      try (SyncDataNodeInternalServiceClient client =
          clients.borrowClient(
              new TEndPoint(dataNode.getInternalAddress(), dataNode.getInternalPort()))) {
        TSStatus status = client.executeCQ(request);
        assertEquals(
            status.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }
  }

  @Test
  public void testCQExecution1() {
    String insertTemplate =
        "INSERT INTO root.sg.d1(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d1.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d1.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d1(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq1\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d1(s1_max)\n"
              + "  FROM root.sg.d1\n"
              + "  GROUP BY(1s) \n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq1");
        return;
      }

      long targetTime = firstExecutionTime + 10_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {
        startTime + 1_000, startTime + 2_000, startTime + 3_000, startTime + 4_000
      };
      long[] expectedValue = {4, 6, 8, 10};

      try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d1.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq1");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution2() {
    String insertTemplate =
        "INSERT INTO root.sg.d2(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d2.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d2.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d2(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq2\n"
              + "RESAMPLE \n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 4s \n"
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d2(s1_max)\n"
              + "  FROM root.sg.d2\n"
              + "  GROUP BY(1s) \n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq2");
        return;
      }

      long targetTime = firstExecutionTime + 10_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {
        startTime, startTime + 1_000, startTime + 2_000, startTime + 3_000, startTime + 4_000
      };
      long[] expectedValue = {2, 4, 6, 8, 10};

      try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d2")) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d2.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq2");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution3() {
    String insertTemplate =
        "INSERT INTO root.sg.d3(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d3.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d3.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d3(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq3\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 4s\n"
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d3(s1_max)\n"
              + "  FROM root.sg.d3\n"
              + "  GROUP BY(1s) \n"
              + "  FILL(100)\n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq3");
        return;
      }

      long targetTime = firstExecutionTime + 10_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {
        startTime - 1_000,
        startTime,
        startTime + 1_000,
        startTime + 2_000,
        startTime + 3_000,
        startTime + 4_000
      };
      long[] expectedValue = {100, 2, 4, 6, 8, 10};

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1_max from root.sg.d3 where time between "
                  + (startTime - 1_000)
                  + " and "
                  + (startTime + 4_000))) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d3.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq3");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution4() {
    String insertTemplate =
        "INSERT INTO root.sg.d4(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d4.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d4.s1_max WITH DATATYPE=INT64");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d4(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq4\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 2s, 1s\n"
              + "BEGIN \n"
              + "  SELECT max_value(s1) \n"
              + "  INTO root.sg.d4(s1_max)\n"
              + "  FROM root.sg.d4\n"
              + "  GROUP BY(1s) \n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq4");
        return;
      }

      long targetTime = firstExecutionTime + 10_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {startTime + 1_000, startTime + 3_000};
      long[] expectedValue = {4, 8};

      try (ResultSet resultSet = statement.executeQuery("select s1_max from root.sg.d4")) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(expectedValue[cnt], resultSet.getLong("root.sg.d4.s1_max"));
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq4");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCQExecution5() {
    String insertTemplate =
        "INSERT INTO root.sg.d5(time, s1) VALUES (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d), (%d, %d)";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      long now = System.currentTimeMillis();
      long firstExecutionTime = now + 10_000;
      long startTime = firstExecutionTime - 3_000;

      statement.execute("create timeseries root.sg.d5.s1 WITH DATATYPE=INT64");
      statement.execute("create timeseries root.sg.d5.precalculated_s1 WITH DATATYPE=DOUBLE");

      // firstly write one row to init data region, just for accelerating the following insert
      // statement.
      statement.execute("INSERT INTO root.sg.d5(time, s1) VALUES (0,0)");

      statement.execute(
          String.format(
              insertTemplate,
              startTime,
              1,
              startTime + 500,
              2,
              startTime + 1_000,
              3,
              startTime + 1_500,
              4,
              startTime + 2_000,
              5,
              startTime + 2_500,
              6,
              startTime + 3_000,
              7,
              startTime + 3_500,
              8,
              startTime + 4_000,
              9,
              startTime + 4_500,
              10));

      statement.execute(
          "CREATE CONTINUOUS QUERY cq5\n"
              + "RESAMPLE EVERY 2s\n"
              + String.format("BOUNDARY %d\n", firstExecutionTime)
              + "RANGE 4s\n"
              + "BEGIN \n"
              + "  SELECT s1 + 1 \n"
              + "  INTO root.sg.d5(precalculated_s1)\n"
              + "  FROM root.sg.d5\n"
              + "  align by device\n"
              + "END");

      // if the insert cost too much time
      if (System.currentTimeMillis() > firstExecutionTime) {
        statement.execute("DROP CQ cq5");
        return;
      }

      long targetTime = firstExecutionTime + 10_000;

      while (System.currentTimeMillis() - targetTime < 0) {
        TimeUnit.SECONDS.sleep(1);
      }

      long[] expectedTime = {
        startTime,
        startTime + 500,
        startTime + 1_000,
        startTime + 1_500,
        startTime + 2_000,
        startTime + 2_500,
        startTime + 3_000,
        startTime + 3_500,
        startTime + 4_000,
        startTime + 4_500
      };
      double[] expectedValue = {2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0};

      try (ResultSet resultSet =
          statement.executeQuery(
              "select precalculated_s1 from root.sg.d5 where time between "
                  + startTime
                  + " and "
                  + (startTime + 4_500))) {
        int cnt = 0;
        while (resultSet.next()) {
          assertEquals(expectedTime[cnt], resultSet.getLong(TIMESTAMP_STR));
          assertEquals(
              expectedValue[cnt], resultSet.getDouble("root.sg.d5.precalculated_s1"), 0.00001);
          cnt++;
        }
        assertEquals(expectedTime.length, cnt);
      } finally {
        statement.execute("DROP CQ cq5");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
