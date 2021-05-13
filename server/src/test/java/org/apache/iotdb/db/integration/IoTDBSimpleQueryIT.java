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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBSimpleQueryIT {

  boolean autoCreateSchemaEnabled;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    autoCreateSchemaEnabled = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(autoCreateSchemaEnabled);
  }

  @Test
  public void testCreateTimeseries1() throws ClassNotFoundException, MetadataException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    MeasurementMNode mNode =
        (MeasurementMNode) MManager.getInstance().getNodeByPath(new PartialPath("root.sg1.d0.s1"));
    assertNull(mNode.getSchema().getProps());
  }

  @Test
  public void testCreateTimeseriesSDTProperties() throws ClassNotFoundException, MetadataException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    MeasurementMNode mNode =
        (MeasurementMNode) MManager.getInstance().getNodeByPath(new PartialPath("root.sg1.d0.s1"));

    // check if SDT property is set
    assertEquals(2, mNode.getSchema().getProps().size());
  }

  @Test
  public void testCreateTimeseriesWithSDTProperties2()
      throws ClassNotFoundException, MetadataException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN,"
              + "LOSS=SDT,COMPDEV=2,COMPMINTIME=2,COMPMAXTIME=10");
    } catch (SQLException e) {
      e.printStackTrace();
    }

    MeasurementMNode mNode =
        (MeasurementMNode) MManager.getInstance().getNodeByPath(new PartialPath("root.sg1.d0.s1"));

    // check if SDT property is set
    assertEquals(4, mNode.getSchema().getProps().size());
  }

  @Test
  public void testFailedToCreateTimeseriesSDTProperties() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      try {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=-2");
      } catch (Exception e) {
        assertEquals(
            "318: SDT compression deviation cannot be negative. Failed to create timeseries for path root.sg1.d0.s1",
            e.getMessage());
      }

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        while (resultSet.next()) {
          count++;
        }
      }
      assertEquals(0, count);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLastQueryNonCached() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine.d1.s1 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine.d1.s2 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine.d2.s1 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute("insert into root.turbine.d1(timestamp,s1,s2) values(1,1,2)");

      String[] results = {"root.turbine.d1.s1", "root.turbine.d1.s2"};

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("select last * from root")) {
        while (resultSet.next()) {
          String path = resultSet.getString("timeseries");
          assertEquals(results[count], path);
          count++;
        }
      }

      assertEquals(2, count);

      try (ResultSet resultSet = statement.executeQuery("select last * from root")) {
        while (resultSet.next()) {
          count++;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingSeq() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=DOUBLE,ENCODING=PLAIN,LOSS=SDT,COMPDEV=0.01");

      int degree = 0;
      for (int time = 0; time < 100; time++) {
        // generate data in sine wave pattern
        double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + "," + value + ")";
        statement.execute(sql);
      }

      // before SDT encoding
      ResultSet resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(count, 100);

      // after flush and SDT encoding
      statement.execute("flush");
      resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(15, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingCompDev() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2");

      for (int time = 1; time < 8; time++) {
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + ",1)";
        statement.execute(sql);
      }
      statement.execute("flush");

      String sql = "insert into root.sg1.d0(timestamp,s0) values(15,10)";
      statement.execute(sql);
      sql = "insert into root.sg1.d0(timestamp,s0) values(16,20)";
      statement.execute(sql);
      sql = "insert into root.sg1.d0(timestamp,s0) values(17,1)";
      statement.execute(sql);
      sql = "insert into root.sg1.d0(timestamp,s0) values(18,30)";
      statement.execute(sql);
      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("select * from root");
      int count = 0;

      String[] timestamps = {"1", "7", "15", "16", "17", "18"};
      String[] values = {"1", "1", "10", "20", "1", "30"};

      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString("Time"));
        assertEquals(values[count], resultSet.getString("root.sg1.d0.s0"));
        count++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingSelectFill() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      double compDev = 2;
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV="
              + compDev);

      int[] originalValues = new int[1000];

      Map<String, Integer> map = new HashMap<>();

      Random rand = new Random();
      for (int i = 1; i < originalValues.length; i++) {
        originalValues[i] = rand.nextInt(500);
        String sql =
            "insert into root.sg1.d0(timestamp,s0) values(" + i + "," + originalValues[i] + ")";
        statement.execute(sql);
        map.put(i + "", originalValues[i]);
      }
      statement.execute("flush");

      for (int i = 1; i < originalValues.length; i++) {
        String sql = "select * from root where time = " + i + " fill(int32 [linear, 20ms, 20ms])";
        ResultSet resultSet = statement.executeQuery(sql);

        while (resultSet.next()) {
          String time = resultSet.getString("Time");
          String value = resultSet.getString("root.sg1.d0.s0");
          // last value is not stored, cannot linear fill
          if (value == null) {
            continue;
          }
          // sdt parallelogram's height is 2 * compDev, so after linear fill, the values will fall
          // inside
          // the parallelogram of two stored points
          assertTrue(Math.abs(Integer.parseInt(value) - map.get(time)) <= 2 * compDev);
        }
      }
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testSDTEncodingCompMin() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2, COMPMINTIME=1");

      for (int time = 1; time < 8; time++) {
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + ",1)";
        statement.execute(sql);
      }
      statement.execute("flush");

      String sql = "insert into root.sg1.d0(timestamp,s0) values(15,10)";
      statement.execute(sql);
      sql = "insert into root.sg1.d0(timestamp,s0) values(16,20)";
      statement.execute(sql);
      sql = "insert into root.sg1.d0(timestamp,s0) values(17,1)";
      statement.execute(sql);
      sql = "insert into root.sg1.d0(timestamp,s0) values(18,30)";
      statement.execute(sql);
      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("select * from root");
      int count = 0;

      // will not store time = 16 since time distance to last stored time 15 is within compMinTime
      String[] timestamps = {"1", "7", "15", "17", "18"};
      String[] values = {"1", "1", "10", "1", "30"};

      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString("Time"));
        assertEquals(values[count], resultSet.getString("root.sg1.d0.s0"));
        count++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingCompMax() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2, COMPMAXTIME=20");

      for (int time = 1; time < 50; time++) {
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + ",1)";
        statement.execute(sql);
      }
      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("select * from root");
      int count = 0;

      String[] timestamps = {"1", "21", "41", "49"};
      String[] values = {"1", "1", "1", "1"};

      while (resultSet.next()) {
        assertEquals(timestamps[count], resultSet.getString("Time"));
        assertEquals(values[count], resultSet.getString("root.sg1.d0.s0"));
        count++;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingUnseq() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=DOUBLE,ENCODING=PLAIN,LOSS=SDT,COMPDEV=0.01");

      int degree = 0;
      for (int time = 0; time < 100; time++) {
        // generate data in sine wave pattern
        double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + "," + value + ")";
        statement.execute(sql);
      }

      // insert unseq
      String sql = "insert into root.sg1.d0(timestamp,s0) values(2,19)";
      statement.execute(sql);

      // before SDT encoding
      ResultSet resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(count, 100);

      // after flush and SDT encoding
      statement.execute("flush");
      resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(18, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingMergeSeq() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=DOUBLE,ENCODING=PLAIN,LOSS=SDT,COMPDEV=0.01");

      int degree = 0;
      for (int time = 0; time < 100; time++) {
        // generate data in sine wave pattern
        double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + "," + value + ")";
        statement.execute(sql);
      }

      // before SDT encoding
      ResultSet resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(100, count);

      // after flush and SDT encoding
      statement.execute("flush");
      resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(15, count);

      // no sdt encoding when merging
      statement.execute("merge");
      resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(15, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingMergeUnseq() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=DOUBLE,ENCODING=PLAIN,LOSS=SDT,COMPDEV=0.01");

      int degree = 0;
      for (int time = 0; time < 100; time++) {
        // generate data in sine wave pattern
        double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + "," + value + ")";
        statement.execute(sql);
      }

      // insert unseq
      String sql = "insert into root.sg1.d0(timestamp,s0) values(2,19)";
      statement.execute(sql);

      // before SDT encoding
      ResultSet resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      int count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(100, count);

      // after flush and SDT encoding
      statement.execute("flush");
      resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(18, count);

      // no sdt encoding when merging
      statement.execute("merge");
      resultSet = statement.executeQuery("select s0 from root.sg1.d0");
      count = 0;
      while (resultSet.next()) {
        count++;
      }
      assertEquals(18, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testEmptyDataSet() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("select * from root");
      // has an empty time column
      Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
      try {
        while (resultSet.next()) {
          fail();
        }

        resultSet =
            statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 group by ([0, 100), 20ms, 20ms)");
        // has an empty time column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root");
        // has no column
        Assert.assertEquals(0, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select * from root align by device");
        // has time and device columns
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select count(*) from root align by device");
        // has device column
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet =
            statement.executeQuery(
                "select count(*) from root where time >= 1 and time <= 100 "
                    + "group by ([0, 100), 20ms, 20ms) align by device");
        // has time and device columns
        Assert.assertEquals(2, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }
      } finally {
        resultSet.close();
      }

      resultSet.close();
    }
  }

  @Test
  public void testOrderByTimeDesc() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1, 1)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (2, 2)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (3, 3)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (4, 4)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (3, 3)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (1, 1)");
      statement.execute("flush");

      String[] ret =
          new String[] {
            "4,4,null", "3,3,3", "2,2,null", "1,1,1",
          };

      int cur = 0;
      try (ResultSet resultSet = statement.executeQuery("select * from root order by time desc")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("Time")
                  + ","
                  + resultSet.getString("root.sg1.d0.s0")
                  + ","
                  + resultSet.getString("root.sg1.d0.s1");
          assertEquals(ret[cur], ans);
          cur++;
        }
      }
    }
  }

  @Test
  public void testShowTimeseriesDataSet1() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(10, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesDataSet2() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(10);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(10, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesDataSet3() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(15);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(10, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesDataSet4() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s2 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s3 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s4 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s5 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s6 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s7 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s8 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s9 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s10 WITH DATATYPE=INT32,ENCODING=PLAIN");

      statement.execute("flush");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries limit 8")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(8, count);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testShowTimeseriesWithLimitOffset() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      List<String> exps =
          Arrays.asList("root.sg1.d0.s1", "root.sg1.d0.s2", "root.sg1.d0.s3", "root.sg1.d0.s4");

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s4) VALUES (5, 5)");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show timeseries limit 2 offset 1")) {
        while (resultSet.next()) {
          Assert.assertTrue(exps.contains(resultSet.getString(1)));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testShowDevicesWithLimitOffset() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      List<String> exps = Arrays.asList("root.sg1.d1", "root.sg1.d2");

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d3(timestamp, s4) VALUES (5, 5)");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show devices limit 2 offset 1")) {
        while (resultSet.next()) {
          Assert.assertEquals(exps.get(count), resultSet.getString(1));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testShowDevicesWithLimit() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      List<String> exps = Arrays.asList("root.sg1.d0", "root.sg1.d1");

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d3(timestamp, s4) VALUES (5, 5)");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show devices limit 2")) {
        while (resultSet.next()) {
          Assert.assertEquals(exps.get(count), resultSet.getString(1));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testFirstOverlappedPageFiltered() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq chunk : [1,10]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1, 1)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (10, 10)");

      statement.execute("flush");

      // seq chunk : [13,20]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (13, 13)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (20, 20)");

      statement.execute("flush");

      // unseq chunk : [5,15]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (15, 15)");

      statement.execute("flush");

      long count = 0;
      try (ResultSet resultSet =
          statement.executeQuery("select s0 from root.sg1.d0 where s0 > 18")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(1, count);
    }
  }

  @Test
  public void testPartialInsertion() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      try {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s0, s1) VALUES (1, 1, 2.2)");
        fail();
      } catch (IoTDBSQLException e) {
        assertTrue(e.getMessage().contains("s1"));
      }

      try (ResultSet resultSet = statement.executeQuery("select s0, s1 from root.sg1.d0")) {
        while (resultSet.next()) {
          assertEquals(1, resultSet.getInt("root.sg1.d0.s0"));
          assertEquals(null, resultSet.getString("root.sg1.d0.s1"));
        }
      }
    }
  }

  @Test
  public void testPartialInsertionAllFailed() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);

    boolean autoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    boolean enablePartialInsert = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
      IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);

      statement.execute("SET STORAGE GROUP TO root.sg1");

      try {
        statement.execute("INSERT INTO root.sg1(timestamp, s0) VALUES (1, 1)");
        fail();
      } catch (IoTDBSQLException e) {
        assertTrue(e.getMessage().contains("s0"));
      }
    }

    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(enablePartialInsert);
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(autoCreateSchemaEnabled);
  }

  @Test
  public void testOverlappedPagesMerge() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq chunk : start-end [1000, 1000]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1000, 0)");

      statement.execute("flush");

      // unseq chunk : [1,10]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1, 1)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (10, 10)");

      statement.execute("flush");

      // usneq chunk : [5,15]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (15, 15)");

      statement.execute("flush");

      // unseq chunk : [15,15]
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (15, 150)");

      statement.execute("flush");

      long count = 0;

      try (ResultSet resultSet =
          statement.executeQuery("select s0 from root.sg1.d0 where s0 < 100")) {
        while (resultSet.next()) {
          count++;
        }
      }

      Assert.assertEquals(4, count);
    }
  }

  @Test
  public void testUnseqUnsealedDeleteQuery() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");

      // seq data
      statement.execute("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (1000, 1)");
      statement.execute("flush");

      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (%d, %d)", i, i));
      }

      statement.execute("flush");

      // unseq data
      for (int i = 11; i <= 20; i++) {
        statement.execute(
            String.format("INSERT INTO root.sg1.d0(timestamp, s0) VALUES (%d, %d)", i, i));
      }

      statement.execute("delete from root.sg1.d0.s0 where time <= 15");

      long count = 0;

      try (ResultSet resultSet = statement.executeQuery("select * from root")) {
        while (resultSet.next()) {
          count++;
        }
      }

      System.out.println(count);
    }
  }

  @Test
  public void testTimeseriesMetadataCache() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      for (int i = 0; i < 10000; i++) {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d0.s" + i + " WITH DATATYPE=INT32,ENCODING=PLAIN");
      }
      for (int i = 1; i < 10000; i++) {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s" + i + ") VALUES (1000, 1)");
      }
      statement.execute("flush");
      statement.executeQuery("select s0 from root.sg1.d0");
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testInvalidSchema() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      try {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d1.s1 with datatype=BOOLEAN, encoding=TS_2DIFF");
      } catch (Exception e) {
        Assert.assertEquals("303: encoding TS_2DIFF does not support BOOLEAN", e.getMessage());
      }

      try {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d1.s3 with datatype=DOUBLE, encoding=REGULAR");
      } catch (Exception e) {
        Assert.assertEquals("303: encoding REGULAR does not support DOUBLE", e.getMessage());
      }

      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s4 with datatype=TEXT, encoding=TS_2DIFF");
      } catch (Exception e) {
        Assert.assertEquals("303: encoding TS_2DIFF does not support TEXT", e.getMessage());
      }

    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testUseSameStatement() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s0 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");

      statement.execute("insert into root.sg1.d0(timestamp,s0,s1) values(1,1,1)");
      statement.execute("insert into root.sg1.d1(timestamp,s0,s1) values(1000,1000,1000)");
      statement.execute("insert into root.sg1.d0(timestamp,s0,s1) values(10,10,10)");

      List<ResultSet> resultSetList = new ArrayList<>();

      ResultSet r1 = statement.executeQuery("select * from root.sg1.d0 where time <= 1");
      resultSetList.add(r1);

      ResultSet r2 = statement.executeQuery("select * from root.sg1.d1 where s0 == 1000");
      resultSetList.add(r2);

      ResultSet r3 = statement.executeQuery("select * from root.sg1.d0 where s1 == 10");
      resultSetList.add(r3);

      r1.next();
      Assert.assertEquals(r1.getLong(1), 1L);
      Assert.assertEquals(r1.getLong(2), 1L);
      Assert.assertEquals(r1.getLong(3), 1L);

      r2.next();
      Assert.assertEquals(r2.getLong(1), 1000L);
      Assert.assertEquals(r2.getLong(2), 1000L);
      Assert.assertEquals(r2.getLong(3), 1000L);

      r3.next();
      Assert.assertEquals(r3.getLong(1), 10L);
      Assert.assertEquals(r3.getLong(2), 10L);
      Assert.assertEquals(r3.getLong(3), 10L);
    }
  }

  @Test
  public void testInvalidMaxPointNumber() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s1 with datatype=FLOAT, encoding=TS_2DIFF, "
              + "max_point_number=4");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s2 with datatype=FLOAT, encoding=TS_2DIFF, "
              + "max_point_number=2.5");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s3 with datatype=FLOAT, encoding=RLE, "
              + "max_point_number=q");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s4 with datatype=FLOAT, encoding=RLE, "
              + "max_point_number=-1");
      statement.execute(
          "insert into root.sg1.d1(timestamp,s1,s2,s3,s4) values(1,1.1234,1.1234,1.1234,1.1234)");

      try (ResultSet r1 = statement.executeQuery("select s1 from root.sg1.d1")) {
        r1.next();
        Assert.assertEquals(1.1234f, r1.getFloat(2), 0);
      }

      try (ResultSet r2 = statement.executeQuery("select s3 from root.sg1.d1")) {
        r2.next();
        Assert.assertEquals(1.12f, r2.getFloat(2), 0);
      }

      try (ResultSet r3 = statement.executeQuery("select s3 from root.sg1.d1")) {
        r3.next();
        Assert.assertEquals(1.12f, r3.getFloat(2), 0);
      }

      try (ResultSet r4 = statement.executeQuery("select s4 from root.sg1.d1")) {
        r4.next();
        Assert.assertEquals(1.12f, r4.getFloat(2), 0);
      }

    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testStorageGroupWithHyphenInName() throws ClassNotFoundException, MetadataException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.group-with-hyphen");
    } catch (SQLException e) {
      fail();
    }

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("SHOW STORAGE GROUP");
      if (hasResultSet) {
        try (ResultSet resultSet = statement.getResultSet()) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          while (resultSet.next()) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
              builder.append(resultSet.getString(i));
            }
            Assert.assertEquals(builder.toString(), "root.group-with-hyphen");
          }
        }
      }
    } catch (SQLException e) {
      fail();
    }
  }
}
