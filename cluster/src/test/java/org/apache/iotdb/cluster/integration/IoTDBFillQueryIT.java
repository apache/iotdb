/**
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
package org.apache.iotdb.cluster.integration;

import static org.apache.iotdb.cluster.utils.Utils.insertData;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.entity.Server;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.cluster.utils.QPExecutorUtils;
import org.apache.iotdb.cluster.utils.hash.PhysicalNode;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IoTDBFillQueryIT {

  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static final PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());

  private static String[] createSQLs = new String[]{
      "SET STORAGE GROUP TO root.ln.wf01.wt01",
      "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN"};
  private static String[] insertSQLs = new String[]{
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(1, 1.1, false, 11)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(2, 2.2, true, 22)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(3, 3.3, false, 33 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(4, 4.4, false, 44)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(5, 5.5, false, 55)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(100, 100.1, false, 110)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(150, 200.2, true, 220)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(200, 300.3, false, 330 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(250, 400.4, false, 440)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(300, 500.5, false, 550)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(10, 10.1, false, 110)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(20, 20.2, true, 220)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(30, 30.3, false, 330 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(40, 40.4, false, 440)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(50, 50.5, false, 550)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(500, 100.1, false, 110)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(510, 200.2, true, 220)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(520, 300.3, false, 330 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(530, 400.4, false, 440)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(540, 500.5, false, 550)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(580, 100.1, false, 110)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(590, 200.2, true, 220)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(600, 300.3, false, 330 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(610, 400.4, false, 440)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(620, 500.5, false, 550)",
  };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";
  private static final String STATUS_STR = "root.ln.wf01.wt01.status";
  private static final String HARDWARE_STR = "root.ln.wf01.wt01.hardware";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    CLUSTER_CONFIG.createAllPath();
    server = Server.getInstance();
    server.start();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
    QPExecutorUtils.setLocalNodeAddr(localNode.getIp(), localNode.getPort());
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void LinearFillTest() throws SQLException {
    String[] retArray1 = new String[]{
        "3,3.3,false,33",
        "70,70.34,false,374",
        "70,null,null,null",
        "625,null,false,null"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select temperature,status, hardware from "
          + "root.ln.wf01.wt01 where time = 3 "
          + "Fill(int32[linear, 5ms, 5ms], double[linear, 5ms, 5ms], boolean[previous, 5ms])");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        System.out.println(ans);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
      }
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 70 Fill(int32[linear, 500ms, 500ms], "
          + "double[linear, 500ms, 500ms], boolean[previous, 500ms])");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
        System.out.println(ans);
      }
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 70 "
          + "Fill(int32[linear, 25ms, 25ms], double[linear, 25ms, 25ms], boolean[previous, 5ms])");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
        System.out.println(ans);
      }
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 625 "
          + "Fill(int32[linear, 25ms, 25ms], double[linear, 25ms, 25ms], boolean[previous, 5ms])");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        System.out.println(cnt + " " + ans);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
      }
      statement.close();
      Assert.assertEquals(retArray1.length, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void LinearFillRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    LinearFillTest();
  }

  @Test
  public void PreviousFillTest() throws SQLException {
    String[] retArray1 = new String[]{
        "3,3.3,false,33",
        "70,50.5,false,550",
        "70,null,null,null"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 3 "
          + "Fill(int32[previous, 5ms], double[previous, 5ms], boolean[previous, 5ms])");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
      }
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 70 "
          + "Fill(int32[previous, 500ms], double[previous, 500ms], boolean[previous, 500ms])");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
        System.out.println(ans);
      }
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 70 "
          + "Fill(int32[previous, 15ms], double[previous, 15ms], boolean[previous, 5ms])");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
        System.out.println(ans);
      }
      statement.close();
      Assert.assertEquals(retArray1.length, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void PreviousFillRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    PreviousFillTest();
  }

  @Test
  public void EmptyTimeRangeFillTest() throws SQLException {
    String[] retArray1 = new String[]{
        "3,3.3,false,33",
        "70,70.34,false,374"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 3 "
          + "Fill(int32[linear], double[linear], boolean[previous])");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
      }
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select temperature,status, hardware "
          + "from root.ln.wf01.wt01 where time = 70 "
          + "Fill(int32[linear], double[linear], boolean[previous])");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(TEMPERATURE_STR)
            + "," + resultSet.getString(STATUS_STR) + "," + resultSet.getString(HARDWARE_STR);
        Assert.assertEquals(retArray1[cnt], ans);
        cnt++;
        System.out.println(ans);
      }
      statement.close();
      Assert.assertEquals(retArray1.length, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void EmptyTimeRangeFillRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    EmptyTimeRangeFillTest();
  }

  private void prepareData() throws SQLException {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root")) {
      insertData(connection, createSQLs, insertSQLs);
    }
  }
}
