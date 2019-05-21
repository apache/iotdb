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

import static org.apache.iotdb.cluster.integration.Constant.count;
import static org.apache.iotdb.cluster.integration.Constant.first;
import static org.apache.iotdb.cluster.integration.Constant.last;
import static org.apache.iotdb.cluster.integration.Constant.max_time;
import static org.apache.iotdb.cluster.integration.Constant.max_value;
import static org.apache.iotdb.cluster.integration.Constant.mean;
import static org.apache.iotdb.cluster.integration.Constant.min_time;
import static org.apache.iotdb.cluster.integration.Constant.min_value;
import static org.apache.iotdb.cluster.integration.Constant.sum;
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

public class IoTDBAggregationIT {

  private Server server;
  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();
  private static final PhysicalNode localNode = new PhysicalNode(CLUSTER_CONFIG.getIp(),
      CLUSTER_CONFIG.getPort());

  private static String[] creationSqls = new String[]{
      "SET STORAGE GROUP TO root.vehicle.d0",
      "SET STORAGE GROUP TO root.vehicle.d1",

      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN"
  };

  private static String[] dataSet2 = new String[]{
      "SET STORAGE GROUP TO root.ln.wf01.wt01",
      "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(1, 1.1, false, 11)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(2, 2.2, true, 22)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(3, 3.3, false, 33 )",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(4, 4.4, false, 44)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
          + "values(5, 5.5, false, 55)"
  };

  private String insertTemplate = "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3)"
      + " VALUES(%d,%d,%d,%f,%s)";

  private static final String TIMESTAMP_STR = "Time";
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.cleanEnv();
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
  public void test() throws SQLException {
    String[] retArray = new String[]{
        "0,2",
        "0,4",
        "0,3"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute(
          "select count(temperature) from root.ln.wf01.wt01 where time > 3");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," +
            resultSet.getString(count(TEMPERATURE_STR));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute(
          "select min_time(temperature) from root.ln.wf01.wt01 where time > 3");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," +
            resultSet.getString(min_time(TEMPERATURE_STR));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute(
          "select min_time(temperature) from root.ln.wf01.wt01 where temperature > 3");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," +
            resultSet.getString(min_time(TEMPERATURE_STR));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(3, cnt);
      statement.close();

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
  public void remoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    test();
  }

  @Test
  public void countTest() throws SQLException {
    String[] retArray = new String[]{
        "0,2001,2001,2001,2001",
        "0,7500,7500,7500,7500"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3) " +
          "from root.vehicle.d0 where time >= 6000 and time <= 9000");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
            + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
            + "," + resultSet.getString(count(d0s3));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select count(s0),count(s1),count(s2),count(s3) " +
          "from root.vehicle.d0");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(count(d0s0))
            + "," + resultSet.getString(count(d0s1)) + "," + resultSet.getString(count(d0s2))
            + "," + resultSet.getString(count(d0s3));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();

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
  public void countRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    countTest();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  @Test
  public void firstTest() throws SQLException {
    String[] retArray = new String[]{
        "0,2000,2000,2000.0,2000",
        "0,500,500,500.0,500"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select first(s0),first(s1),first(s2),first(s3) " +
          "from root.vehicle.d0 where time >= 1500 and time <= 9000");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(first(d0s0))
            + "," + resultSet.getString(first(d0s1)) + "," + resultSet.getString(first(d0s2))
            + "," + resultSet.getString(first(d0s3));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select first(s0),first(s1),first(s2),first(s3) " +
          "from root.vehicle.d0");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(first(d0s0))
            + "," + resultSet.getString(first(d0s1)) + "," + resultSet.getString(first(d0s2))
            + "," + resultSet.getString(first(d0s3));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();

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
  public void firstRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    firstTest();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void lastTest() throws SQLException {
    String[] retArray = new String[]{
        "0,8499,8499.0",
        "0,1499,1499.0",
        "0,2200,2200.0"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select last(s0),last(s2) " +
          "from root.vehicle.d0 where time >= 1500 and time < 9000");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(last(d0s0))
            + "," + resultSet.getString(last(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select last(s0),last(s2) " +
          "from root.vehicle.d0 where time <= 1600");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(last(d0s0))
            + "," + resultSet.getString(last(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select last(s0),last(s2) " +
          "from root.vehicle.d0 where time <= 2200");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(last(d0s0))
            + "," + resultSet.getString(last(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(3, cnt);
      statement.close();

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
  public void lastRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    lastTest();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void maxminTimeTest() throws SQLException {
    String[] retArray = new String[]{
        "0,8499,500",
        "0,2499,2000"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select max_time(s0),min_time(s2) " +
          "from root.vehicle.d0 where time >= 100 and time < 9000");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_time(d0s0))
            + "," + resultSet.getString(min_time(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select max_time(s0),min_time(s2) " +
          "from root.vehicle.d0 where time <= 2500 and time > 1800");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_time(d0s0))
            + "," + resultSet.getString(min_time(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();
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
  public void maxminTimeRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    maxminTimeTest();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void maxminValueTest() throws SQLException {
    String[] retArray = new String[]{
        "0,8499,500.0",
        "0,2499,500.0"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select max_value(s0),min_value(s2) " +
          "from root.vehicle.d0 where time >= 100 and time < 9000");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d0s0))
            + "," + resultSet.getString(min_value(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select max_value(s0),min_value(s2) " +
          "from root.vehicle.d0 where time < 2500");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(max_value(d0s0))
            + "," + resultSet.getString(min_value(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();
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
  public void maxminValueRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    maxminTimeTest();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void meanSumTest() throws SQLException {
    String[] retArray = new String[]{
        "0,1.4508E7,7250.374812593703",
        "0,626750.0,1250.998003992016"
    };
    Connection connection = null;
    try {
      connection = DriverManager.
          getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      boolean hasResultSet = statement.execute("select sum(s0),mean(s2)" +
          "from root.vehicle.d0 where time >= 6000 and time <= 9000");

      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(sum(d0s0))
            + "," + resultSet.getString(mean(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      statement.close();

      statement = connection.createStatement();
      hasResultSet = statement.execute("select sum(s0),mean(s2)" +
          "from root.vehicle.d0 where time >= 1000 and time <= 2000");

      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      while (resultSet.next()) {
        String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(sum(d0s0))
            + "," + resultSet.getString(mean(d0s2));
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      statement.close();
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
  public void meanSumRemoteTest() throws SQLException {
    QPExecutorUtils.setLocalNodeAddr("0.0.0.0", 0);
    meanSumTest();
    try {
      Thread.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void prepareData() throws SQLException {
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
              "root");
      Statement statement = connection.createStatement();
      insertData(connection, creationSqls, dataSet2);
      // prepare BufferWrite file
      for (int i = 5000; i < 7000; i++) {
        statement.addBatch(String.format(insertTemplate, i, i, i, (double) i, "\'" + i + "\'"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("flush");
      for (int i = 7500; i < 8500; i++) {
        statement.addBatch(String.format(insertTemplate, i, i, i, (double) i, "\'" + i + "\'"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("flush");

      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.addBatch(String.format(insertTemplate, i, i, i, (double) i, "\'" + i + "\'"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("flush");
      for (int i = 3000; i < 6500; i++) {
        statement.addBatch(String.format(insertTemplate, i, i, i, (double) i, "\'" + i + "\'"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.execute("merge");

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.addBatch(String.format(insertTemplate, i, i, i, (double) i, "\'" + i + "\'"));
      }
      statement.executeBatch();
      statement.clearBatch();
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.addBatch(String.format(insertTemplate, i, i, i, (double) i, "\'" + i + "\'"));
      }
      statement.executeBatch();
      statement.clearBatch();
      statement.close();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
