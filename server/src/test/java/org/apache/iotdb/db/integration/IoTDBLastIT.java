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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class IoTDBLastIT {

  private static final String[] dataSet1 =
      new String[] {
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

  private static final String[] dataSet2 =
      new String[] {
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

  private static final String[] dataSet3 =
      new String[] {
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
  private static final String DATA_TYPE_STR = "dataType";

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
  public void lastWithEmptySeriesTest() throws Exception {
    String[] retArray =
        new String[] {
          "root.ln.wf02.status,true,BOOLEAN",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "CREATE TIMESERIES root.ln.wf02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.wf02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.wf02(timestamp, status) values(200, true)");
      statement.execute("select last temperature,status from root.ln.wf02");

      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        Assert.assertEquals(retArray[cnt], ans);
        cnt++;
      }

      // Last query resultSet is empty after deletion
      statement.execute("INSERT INTO root.ln.wf02(timestamp, temperature) values(300, 100.0)");
      statement.execute("delete from root.ln.wf02.status where time > 0");
      statement.execute("select last status from root.ln.wf02");

      resultSet = statement.getResultSet();
      Assert.assertFalse(resultSet.next());
    }
  }

  @Test
  public void lastDescTimeTest() throws Exception {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "500,root.ln.wf01.wt01.status,false,BOOLEAN",
                "500,root.ln.wf01.wt01.temperature,22.1,DOUBLE",
                "500,root.ln.wf01.wt01.id,5,INT32",
                "500,root.ln.wf01.wt02.status,false,BOOLEAN",
                "500,root.ln.wf01.wt02.temperature,15.7,DOUBLE",
                "500,root.ln.wf01.wt02.id,9,INT32",
                "300,root.ln.wf01.wt03.status,true,BOOLEAN",
                "300,root.ln.wf01.wt03.temperature,23.1,DOUBLE",
                "300,root.ln.wf01.wt03.id,8,INT32"));

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last * from root.** order by time desc");
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        Assert.assertTrue(retSet.contains(ans));
        cnt++;
      }
      Assert.assertEquals(retSet.size(), cnt);
    }
  }

  @Test
  public void lastCacheUpdateTest() throws SQLException, MetadataException {
    String[] retArray =
        new String[] {
          "500,root.ln.wf01.wt01.temperature,22.1,DOUBLE",
          "500,root.ln.wf01.wt01.status,false,BOOLEAN",
          "500,root.ln.wf01.wt01.id,5,INT32",
          "700,root.ln.wf01.wt01.temperature,33.1,DOUBLE",
          "700,root.ln.wf01.wt01.status,false,BOOLEAN",
          "700,root.ln.wf01.wt01.id,3,INT32",
          "700,root.ln.wf01.wt01.temperature,33.1,DOUBLE",
          "700,root.ln.wf01.wt01.status,false,BOOLEAN",
          "700,root.ln.wf01.wt01.id,3,INT32"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select last temperature,status,id from root.ln.wf01.wt01");

      assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      PartialPath path = new PartialPath("root.ln.wf01.wt01.temperature");
      IoTDB.metaManager.resetLastCache(path);

      statement.execute(
          "insert into root.ln.wf01.wt01(time, temperature, status, id) values(700, 33.1, false, 3)");

      // Last cache is updated with above insert sql
      long time = IoTDB.metaManager.getLastCache(path).getTimestamp();
      Assert.assertEquals(700, time);

      hasResultSet = statement.execute("select last temperature,status,id from root.ln.wf01.wt01");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute(
          "insert into root.ln.wf01.wt01(time, temperature, status, id) values(600, 19.1, false, 1)");

      // Last cache is not updated with above insert sql
      time = IoTDB.metaManager.getLastCache(path).getTimestamp();
      Assert.assertEquals(700, time);

      hasResultSet = statement.execute("select last temperature,status,id from root.ln.wf01.wt01");
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
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
          "500,root.ln.wf01.wt02.temperature,15.7,DOUBLE",
          "500,root.ln.wf01.wt02.status,false,BOOLEAN",
          "500,root.ln.wf01.wt02.id,9,INT32",
          "600,root.ln.wf01.wt02.temperature,10.2,DOUBLE",
          "600,root.ln.wf01.wt02.status,false,BOOLEAN",
          "600,root.ln.wf01.wt02.id,6,INT32"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      PartialPath path = new PartialPath("root.ln.wf01.wt02.temperature");
      IoTDB.metaManager.resetLastCache(path);

      boolean hasResultSet =
          statement.execute("select last temperature,status,id from root.ln.wf01.wt02");

      assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      // Inject unsequential data
      statement.execute(
          "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) values(600, 10.2, false, 6)");
      statement.execute(
          "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) values(450, 20.1, false, 5)");
      statement.execute("flush");
      hasResultSet = statement.execute("select last temperature,status,id from root.ln.wf01.wt02");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
      Assert.assertEquals(cnt, retArray.length);

      IoTDB.metaManager.resetLastCache(path);
      String[] retArray3 =
          new String[] {
            "900,root.ln.wf01.wt01.temperature,10.2,DOUBLE",
            "900,root.ln.wf01.wt01.status,false,BOOLEAN",
            "900,root.ln.wf01.wt01.id,6,INT32",
            "800,root.ln.wf01.wt02.temperature,20.1,DOUBLE",
            "800,root.ln.wf01.wt02.status,false,BOOLEAN",
            "800,root.ln.wf01.wt02.id,5,INT32"
          };
      statement.execute(
          "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) values(900, 10.2, false, 6)");
      statement.execute(
          "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) values(800, 20.1, false, 5)");
      statement.execute("flush");
      hasResultSet =
          statement.execute(
              "select last temperature,status,id from root.ln.wf01.wt01,root.ln.wf01.wt02 order by time desc");
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void lastWithEmptyChunkMetadataTest() throws SQLException, MetadataException {
    String[] retArray = new String[] {"300,root.ln.wf01.wt03.temperature,23.1,DOUBLE"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDB.metaManager.resetLastCache(new PartialPath("root.ln.wf01.wt03.temperature"));

      statement.execute(
          "INSERT INTO root.ln.wf01.wt03(timestamp,status, id) values(500, false, 9)");
      statement.execute("flush");
      statement.execute(
          "INSERT INTO root.ln.wf01.wt03(timestamp,status, id) values(400, false, 11)");
      statement.execute("flush");
      boolean hasResultSet = statement.execute("select last temperature from root.ln.wf01.wt03");

      assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void lastWithUnseqTimeLargerThanSeqTimeTest() throws SQLException, MetadataException {
    String[] retArray = new String[] {"150,root.ln.wf01.wt04.temperature,31.2,DOUBLE"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt04.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt04.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.wf01.wt04(timestamp,temperature) values(100,22.1)");
      statement.execute("flush");
      statement.execute("INSERT INTO root.ln.wf01.wt04(timestamp,status) values(200,true)");
      statement.execute("flush");
      statement.execute("INSERT INTO root.ln.wf01.wt04(timestamp,temperature) values(150,31.2)");
      statement.execute("flush");

      IoTDB.metaManager.resetLastCache(new PartialPath("root.ln.wf01.wt04.temperature"));

      boolean hasResultSet = statement.execute("select last temperature from root.ln.wf01.wt04");

      assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void lastAfterDeletionTest() throws SQLException {
    String[] retArray =
        new String[] {
          "350,root.ln.wf01.wt05.temperature,31.2,DOUBLE",
          "200,root.ln.wf01.wt05.temperature,78.2,DOUBLE"
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt05.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.wf01.wt05.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
      statement.execute("INSERT INTO root.ln.wf01.wt05(timestamp,temperature) values(100,22.1)");
      statement.execute(
          "INSERT INTO root.ln.wf01.wt05(timestamp,temperature, status) values(200, 78.2, true)");
      statement.execute("INSERT INTO root.ln.wf01.wt05(timestamp,temperature) values(350,31.2)");
      statement.execute("flush");

      boolean hasResultSet = statement.execute("select last temperature from root.ln.wf01.wt05");
      assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      statement.execute(
          "delete from root.ln.wf01.wt05.temperature where time > 200 and time < 400");
      statement.execute("select last temperature from root.ln.wf01.wt05");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
    }
  }

  @Test
  public void lastWithFilterTest() throws SQLException {
    String[] retArray = new String[] {"500,root.ln.wf01.wt01.temperature,22.1,DOUBLE"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("select last temperature from root.ln.wf01.wt01");
      statement.execute("select last temperature from root.ln.wf01.wt01 where time >= 300");
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }

      statement.execute("select last temperature from root.ln.wf01.wt01 where time > 600");
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(TIMESEIRES_STR)
                  + ","
                  + resultSet.getString(VALUE_STR)
                  + ","
                  + resultSet.getString(DATA_TYPE_STR);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
      }
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
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
