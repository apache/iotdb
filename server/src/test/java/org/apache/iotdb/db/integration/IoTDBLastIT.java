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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class IoTDBLastIT {

  private static String[] dataSet1 = new String[]{
      "SET STORAGE GROUP TO root.ln.wf01.wt01",
      "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt01.id WITH DATATYPE=INT32, ENCODING=PLAIN",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
          + "values(100, 25.1, false, 7)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
          + "values(200, 25.2, true, 8)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
          + "values(300, 15.7, false, 9)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
          + "values(400, 16.2, false, 6)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
          + "values(500, 22.1, false, 5)",
      "flush",
  };

  private static String[] dataSet2 = new String[]{
      "SET STORAGE GROUP TO root.ln.wf01.wt02",
      "CREATE TIMESERIES root.ln.wf01.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt02.id WITH DATATYPE=INT32, ENCODING=PLAIN",
      "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) "
          + "values(100, 18.6, false, 7)",
      "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) "
          + "values(300, 23.1, true, 8)",
      "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) "
          + "values(500, 15.7, false, 9)",
      "flush",
  };

  private static String[] dataSet3 = new String[]{
      "SET STORAGE GROUP TO root.ln.wf01.wt03",
      "CREATE TIMESERIES root.ln.wf01.wt03.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt03.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
      "CREATE TIMESERIES root.ln.wf01.wt03.id WITH DATATYPE=INT32, ENCODING=PLAIN",
      "INSERT INTO root.ln.wf01.wt03(timestamp,temperature,status, id) "
          + "values(100, 18.6, false, 7)",
      "INSERT INTO root.ln.wf01.wt03(timestamp,temperature,status, id) "
          + "values(300, 23.1, true, 8)",
      "flush",
  };

  private static final String TIMESTAMP_STR = "Time";
  private static final String TIMESEIRES_STR = "timeseries";
  private static final String VALUE_STR = "value";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void lastCacheUpdateTest() throws SQLException, MetadataException {
    String[] retArray =
        new String[] {
          "500,root.ln.wf01.wt01.temperature,22.1",
          "500,root.ln.wf01.wt01.status,false",
          "500,root.ln.wf01.wt01.id,5",
          "700,root.ln.wf01.wt01.temperature,33.1",
          "700,root.ln.wf01.wt01.status,false",
          "700,root.ln.wf01.wt01.id,3",
          "700,root.ln.wf01.wt01.temperature,33.1",
          "700,root.ln.wf01.wt01.status,false",
          "700,root.ln.wf01.wt01.id,3"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute(
              "select last temperature,status,id from root.ln.wf01.wt01");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
                  + resultSet.getString(TIMESEIRES_STR) + ","
                  + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      MeasurementMNode node =
          (MeasurementMNode) MManager.getInstance().getNodeByPath("root.ln.wf01.wt01.temperature");
      node.resetCache();

      statement.execute(
          "insert into root.ln.wf01.wt01(time, temperature, status, id) values(700, 33.1, false, 3)");

      // Last cache is updated with above insert sql
      long time = node.getCachedLast().getTimestamp();
      Assert.assertEquals(time, 700);

      hasResultSet = statement.execute("select last temperature,status,id from root.ln.wf01.wt01");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
                  + resultSet.getString(TIMESEIRES_STR) + ","
                  + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "insert into root.ln.wf01.wt01(time, temperature, status, id) values(600, 19.1, false, 1)");

      // Last cache is not updated with above insert sql
      time = node.getCachedLast().getTimestamp();
      Assert.assertEquals(time, 700);

      hasResultSet = statement.execute("select last temperature,status,id from root.ln.wf01.wt01");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
              + resultSet.getString(TIMESEIRES_STR) + ","
              + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(cnt, retArray.length);
    }
  }

  @Test
  public void lastWithUnSeqFilesTest() throws SQLException, MetadataException {
    String[] retArray =
        new String[] {
            "500,root.ln.wf01.wt02.temperature,15.7",
            "500,root.ln.wf01.wt02.status,false",
            "500,root.ln.wf01.wt02.id,9",
            "600,root.ln.wf01.wt02.temperature,10.2",
            "600,root.ln.wf01.wt02.status,false",
            "600,root.ln.wf01.wt02.id,6"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      MNode node = MManager.getInstance().getNodeByPath("root.ln.wf01.wt02.temperature");
      ((MeasurementMNode) node).resetCache();
      boolean hasResultSet =
          statement.execute(
              "select last temperature,status,id from root.ln.wf01.wt02");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + ","
                  + resultSet.getString(TIMESEIRES_STR) + ","
                  + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) values(600, 10.2, false, 6)");
      statement.execute(
          "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) values(450, 20.1, false, 5)");
      statement.execute("flush");
      hasResultSet = statement.execute(
              "select last temperature,status,id from root.ln.wf01.wt02");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + ","
                  + resultSet.getString(TIMESEIRES_STR) + ","
                  + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(cnt, retArray.length);
    }
  }

  @Test
  public void lastWithEmptyChunkMetadataTest() throws SQLException, MetadataException {
    String[] retArray =
        new String[] {
            "300,root.ln.wf01.wt03.temperature,23.1",
        };


    try (Connection connection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      MNode node = MManager.getInstance().getNodeByPath("root.ln.wf01.wt03.temperature");
      ((MeasurementMNode) node).resetCache();

      statement.execute("INSERT INTO root.ln.wf01.wt03(timestamp,status, id) values(500, false, 9)");
      statement.execute("flush");
      statement.execute("INSERT INTO root.ln.wf01.wt03(timestamp,status, id) values(400, false, 11)");
      statement.execute("flush");
      boolean hasResultSet = statement.execute(
          "select last temperature from root.ln.wf01.wt03");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + ","
                  + resultSet.getString(TIMESEIRES_STR) + ","
                  + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(cnt, 1);
      }
    }
  }

  @Test
  public void lastWithUnseqTimeLargerThanSeqTimeTest() throws SQLException, MetadataException {
    String[] retArray =
        new String[] {
            "150,root.ln.wf01.wt04.temperature,31.2",
        };


    try (Connection connection =
        DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt04");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt04.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.ln.wf01.wt04.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.wf01.wt04(timestamp,temperature) values(100,22.1)");
      statement.execute("flush");
      statement.execute("INSERT INTO root.ln.wf01.wt04(timestamp,status) values(200,true)");
      statement.execute("flush");
      statement.execute("INSERT INTO root.ln.wf01.wt04(timestamp,temperature) values(150,31.2)");
      statement.execute("flush");

      MNode node = MManager.getInstance().getNodeByPath("root.ln.wf01.wt03.temperature");
      ((MeasurementMNode) node).resetCache();

      boolean hasResultSet = statement.execute(
          "select last temperature from root.ln.wf01.wt04");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + ","
                  + resultSet.getString(TIMESEIRES_STR) + ","
                  + resultSet.getString(VALUE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(cnt, 1);
      }
    }
  }

  private void prepareData() {
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root",
            "root");
        Statement statement = connection.createStatement()) {


      for (String sql : dataSet1) {
        statement.execute(sql);
      }
      for (String sql : dataSet2) {
        statement.execute(sql);
      }
      for (String sql : dataSet3) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
