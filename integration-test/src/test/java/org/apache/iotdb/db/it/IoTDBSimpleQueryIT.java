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
package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBSimpleQueryIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCreateTimeseries1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      try (ResultSet resultSet = statement.executeQuery("show timeseries root.sg1.d0.s1")) {
        if (resultSet.next()) {
          assertEquals("PLAIN", resultSet.getString(ColumnHeaderConstant.ENCODING).toUpperCase());
        }
      }

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFailedToCreateTimeseriesSDTProperties() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
      try {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN,'LOSS'='SDT','COMPDEV'='-2'");
        fail();
      } catch (Exception e) {
        assertEquals(
            TSStatusCode.ILLEGAL_PARAMETER.getStatusCode()
                + ": SDT compression deviation cannot be negative. Failed to create timeseries for path root.sg1.d0.s1",
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
  public void testLastQueryNonCached() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine.d1.s1 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine.d1.s2 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine.d2.s1 with datatype=FLOAT, encoding=GORILLA, compression=SNAPPY");
      statement.execute("insert into root.turbine.d1(timestamp,s1,s2) values(1,1,2)");

      List<String> expected = Arrays.asList("root.turbine.d1.s1", "root.turbine.d1.s2");
      List<String> actual = new ArrayList<>();

      try (ResultSet resultSet = statement.executeQuery("select last ** from root")) {
        while (resultSet.next()) {
          actual.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
      }

      assertEquals(expected, actual);

      actual.clear();
      try (ResultSet resultSet = statement.executeQuery("select last * from root")) {
        while (resultSet.next()) {
          actual.add(resultSet.getString(ColumnHeaderConstant.TIMESERIES));
        }
      }

      assertEquals(Collections.emptyList(), actual);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingSeq() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testSDTEncodingCompDev() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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

      ResultSet resultSet = statement.executeQuery("select * from root.**");
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
  public void testSDTEncodingCompMin() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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

      ResultSet resultSet = statement.executeQuery("select * from root.**");
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
  public void testSDTEncodingCompMax() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
      // test set sdt property
      statement.execute(
          "CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN,LOSS=SDT,COMPDEV=2, COMPMAXTIME=20");

      for (int time = 1; time < 50; time++) {
        String sql = "insert into root.sg1.d0(timestamp,s0) values(" + time + ",1)";
        statement.execute(sql);
      }
      statement.execute("flush");

      ResultSet resultSet = statement.executeQuery("select * from root.**");
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
  public void testSDTEncodingUnseq() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testSDTEncodingMergeSeq() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSDTEncodingMergeUnseq() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Ignore
  @Test
  public void testEmptyDataSet() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("select * from root.**");
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
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
        while (resultSet.next()) {
          fail();
        }

        resultSet = statement.executeQuery("select * from root.** align by device");
        // has time and device columns
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
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
        Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
      try (ResultSet resultSet =
          statement.executeQuery("select * from root.** order by time desc")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
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
  public void testShowTimeseriesDataSet1() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testShowTimeseriesDataSet2() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(10);
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testShowTimeseriesDataSet3() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(15);
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testShowTimeseriesDataSet4() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testShowTimeseriesWithLimitOffset() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
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
  public void testShowDevicesWithLimitOffset() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      List<String> exps = Arrays.asList("root.sg1.d1,false", "root.sg1.d2,false");

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d3(timestamp, s4) VALUES (5, 5)");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show devices limit 2 offset 1")) {
        while (resultSet.next()) {
          Assert.assertEquals(
              exps.get(count), resultSet.getString(1) + "," + resultSet.getString(2));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testShowDevicesWithLimit() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      List<String> exps = Arrays.asList("root.sg1.d0,false", "root.sg1.d1,false");

      statement.execute("INSERT INTO root.sg1.d0(timestamp, s1) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(timestamp, s2) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d2(timestamp, s3) VALUES (5, 5)");
      statement.execute("INSERT INTO root.sg1.d3(timestamp, s4) VALUES (5, 5)");

      int count = 0;
      try (ResultSet resultSet = statement.executeQuery("show devices limit 2")) {
        while (resultSet.next()) {
          Assert.assertEquals(
              exps.get(count), resultSet.getString(1) + "," + resultSet.getString(2));
          ++count;
        }
      }
      Assert.assertEquals(2, count);
    }
  }

  @Test
  public void testFirstOverlappedPageFiltered() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testPartialInsertion() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s0 WITH DATATYPE=INT32,ENCODING=PLAIN");
      statement.execute("CREATE TIMESERIES root.sg1.d0.s1 WITH DATATYPE=INT32,ENCODING=PLAIN");

      try {
        statement.execute("INSERT INTO root.sg1.d0(timestamp, s0, s1) VALUES (1, 1, 2.2)");
        fail();
      } catch (SQLException e) {
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
  public void testOverlappedPagesMerge() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testUnseqUnsealedDeleteQuery() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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

      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        while (resultSet.next()) {
          count++;
        }
      }

      System.out.println(count);
    }
  }

  @Test
  public void testTimeseriesMetadataCache() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testInvalidSchema() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      try {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d1.s1 with datatype=BOOLEAN, encoding=TS_2DIFF");
        fail();
      } catch (Exception e) {
        Assert.assertEquals(
            TSStatusCode.METADATA_ERROR.getStatusCode()
                + ": encoding TS_2DIFF does not support BOOLEAN",
            e.getMessage());
      }

      try {
        statement.execute(
            "CREATE TIMESERIES root.sg1.d1.s3 with datatype=DOUBLE, encoding=REGULAR");
        fail();
      } catch (Exception e) {
        Assert.assertEquals(
            TSStatusCode.METADATA_ERROR.getStatusCode()
                + ": encoding REGULAR does not support DOUBLE",
            e.getMessage());
      }

      try {
        statement.execute("CREATE TIMESERIES root.sg1.d1.s4 with datatype=TEXT, encoding=TS_2DIFF");
        fail();
      } catch (Exception e) {
        Assert.assertEquals(
            TSStatusCode.METADATA_ERROR.getStatusCode()
                + ": encoding TS_2DIFF does not support TEXT",
            e.getMessage());
      }

    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testUseSameStatement() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
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
  public void testInvalidMaxPointNumber() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s1 with datatype=FLOAT, encoding=TS_2DIFF, "
              + "'max_point_number'='4'");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s2 with datatype=FLOAT, encoding=TS_2DIFF, "
              + "'max_point_number'='2.5'");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s3 with datatype=FLOAT, encoding=RLE, "
              + "'max_point_number'='q'");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s4 with datatype=FLOAT, encoding=RLE, "
              + "'max_point_number'='-1'");
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
  public void testStorageGroupWithHyphenInName() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("CREATE DATABASE root.group_with_hyphen");
    } catch (final SQLException e) {
      fail();
    }

    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES DETAILS")) {
        while (resultSet.next()) {
          Assert.assertEquals("root.group_with_hyphen", resultSet.getString(1));
          Assert.assertEquals("TREE", resultSet.getString(12));
        }
      }
    } catch (final SQLException e) {
      fail();
    }
  }

  @Test
  @Ignore // disable align is not supported yet
  public void testDisableAlign() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=BOOLEAN");
      ResultSet resultSet = statement.executeQuery("select s1, s2 from root.sg1.d1 disable align");
      ResultSetMetaData metaData = resultSet.getMetaData();
      int[] types = {Types.TIMESTAMP, Types.INTEGER, Types.BIGINT, Types.BOOLEAN};
      int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        Assert.assertEquals(types[i], metaData.getColumnType(i + 1));
      }
    }
  }

  @Test
  public void testEnableAlign() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32");
      statement.execute("CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=BOOLEAN");
      ResultSet resultSet = statement.executeQuery("select s1, s2 from root.sg1.d1");
      ResultSetMetaData metaData = resultSet.getMetaData();
      int[] types = {Types.TIMESTAMP, Types.INTEGER, Types.BOOLEAN};
      int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        Assert.assertEquals(types[i], metaData.getColumnType(i + 1));
      }
    }
  }

  @Test
  public void testFromFuzzyMatching() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s1 with datatype=FLOAT, encoding=TS_2DIFF, "
              + "'max_point_number'='4'");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s2 with datatype=FLOAT, encoding=TS_2DIFF, "
              + "'max_point_number'='2.5'");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s3 with datatype=FLOAT, encoding=RLE, "
              + "'max_point_number'='q'");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s4 with datatype=FLOAT, encoding=RLE, "
              + "'max_point_number'='-1'");
      statement.execute(
          "insert into root.sg1.da1cb(timestamp,s1,s2,s3,s4) values(1,1.1234,1.1234,1.1234,1.1234)");
      statement.execute(
          "insert into root.sg1.da1ce(timestamp,s1,s2,s3,s4) values(1,1.1234,1.1234,1.1234,1.1234)");

      try (ResultSet r1 = statement.executeQuery("select s1 from root.sg1.*a*")) {
        while (r1.next()) {
          Assert.assertEquals(1.1234f, r1.getDouble(2), 0.001);
        }
        Assert.assertEquals(3, r1.getMetaData().getColumnCount());
      }

    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testNewDataType() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE DATABASE root.sg1");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s4 WITH DATATYPE=DATE, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s5 WITH DATATYPE=TIMESTAMP, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s6 WITH DATATYPE=BLOB, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      statement.execute(
          "CREATE TIMESERIES root.sg1.d1.s7 WITH DATATYPE=STRING, ENCODING=PLAIN, COMPRESSOR=SNAPPY");
      for (int i = 1; i <= 10; i++) {
        statement.execute(
            String.format(
                "insert into root.sg1.d1(timestamp, s4, s5, s6, s7) values(%d, \"%s\", %d, %s, \"%s\")",
                i, LocalDate.of(2024, 5, i % 31 + 1), i, "X'cafebabe'", i));
      }

      try (ResultSet resultSet = statement.executeQuery("select * from root.**")) {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        assertEquals(5, columnCount);
        HashMap<Integer, TSDataType> columnType = new HashMap<>();
        for (int i = 1; i < columnCount; i++) {
          if (metaData.getColumnLabel(i).endsWith("s4")) {
            columnType.put(i, TSDataType.DATE);
          } else if (metaData.getColumnLabel(i).endsWith("s5")) {
            columnType.put(i, TSDataType.TIMESTAMP);
          } else if (metaData.getColumnLabel(i).endsWith("s6")) {
            columnType.put(i, TSDataType.BLOB);
          } else if (metaData.getColumnLabel(i).endsWith("s7")) {
            columnType.put(i, TSDataType.TEXT);
          }
        }
        byte[] byteArray = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
        while (resultSet.next()) {
          long time = resultSet.getLong(1);
          Date date = resultSet.getDate(2);
          long timestamp = resultSet.getLong(3);
          byte[] blob = resultSet.getBytes(4);
          String text = resultSet.getString(5);
          assertEquals(2024 - 1900, date.getYear());
          assertEquals(5 - 1, date.getMonth());
          assertEquals(time % 31 + 1, date.getDate());
          assertEquals(time, timestamp);
          assertArrayEquals(byteArray, blob);
          assertEquals(String.valueOf(time), text);
        }
      }

    } catch (SQLException e) {
      fail();
    }
  }
}
