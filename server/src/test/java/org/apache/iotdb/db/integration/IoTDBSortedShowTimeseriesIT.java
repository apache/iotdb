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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBSortedShowTimeseriesIT {

  private static String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.turbine",
        "create timeseries root.turbine.d0.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=f, description='turbine this is a test1') attributes(H_Alarm=100, M_Alarm=50)",
        "create timeseries root.turbine.d0.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=kw, description='turbine this is a test2') attributes(H_Alarm=99.9, M_Alarm=44.4)",
        "create timeseries root.turbine.d0.s2(cpu) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=cores, description='turbine this is a cpu') attributes(H_Alarm=99.9, M_Alarm=44.4)",
        "create timeseries root.turbine.d0.s3(gpu0) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=cores, description='turbine this is a gpu') attributes(H_Alarm=99.9, M_Alarm=44.4)",
        "create timeseries root.turbine.d0.s4(tpu0) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=cores, description='turbine this is a tpu') attributes(H_Alarm=99.9, M_Alarm=44.4)",
        "create timeseries root.turbine.d1.s0(status) with datatype=INT32, encoding=RLE "
            + "tags(description='turbine this is a test3') attributes(H_Alarm=9, M_Alarm=5)",
        "create timeseries root.turbine.d2.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=f, description='turbine d2 this is a test1') attributes(MaxValue=100, MinValue=1)",
        "create timeseries root.turbine.d2.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=kw, description='turbine d2 this is a test2') attributes(MaxValue=99.9, MinValue=44.4)",
        "create timeseries root.turbine.d2.s3(status) with datatype=INT32, encoding=RLE "
            + "tags(description='turbine d2 this is a test3') attributes(MaxValue=9, MinValue=5)",
        "create timeseries root.ln.d0.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=c, description='ln this is a test1') attributes(H_Alarm=1000, M_Alarm=500)",
        "create timeseries root.ln.d0.s1(power) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(unit=w, description='ln this is a test2') attributes(H_Alarm=9.9, M_Alarm=4.4)",
        "create timeseries root.ln.d1.s0(status) with datatype=INT32, encoding=RLE "
            + "tags(description='ln this is a test3') attributes(H_Alarm=90, M_Alarm=50)",
        "insert into root.turbine.d0(timestamp,s0) values(1, 1)",
        "insert into root.turbine.d0(timestamp,s1) values(2, 2)",
        "insert into root.turbine.d0(timestamp,s2) values(3, 3)",
        "insert into root.turbine.d0(timestamp,s3) values(4, 4)",
        "insert into root.turbine.d0(timestamp,s4) values(5, 5)",
        "insert into root.turbine.d1(timestamp,s0) values(1, 11)",
        "insert into root.turbine.d2(timestamp,s0,s1,s3) values(6,6,6,6)"
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    createSchema();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void showTimeseriesOrderByHeatTest1() throws ClassNotFoundException {

    List<String> retArray1 =
        Arrays.asList(
            "root.turbine.d0.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\""
                + "turbine this is a test1\",\"unit\":\"f\"},{\"H_Alarm\":\"100\",\"M_Alarm\":\"50\"}",
            "root.turbine.d0.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine "
                + "this is a test2\",\"unit\":\"kw\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s2,cpu,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a cpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d1.s0,status,root.turbine,INT32,RLE,SNAPPY,{\"description\":\"turbine this "
                + "is a test3\"},{\"H_Alarm\":\"9\",\"M_Alarm\":\"5\"}",
            "root.turbine.d2.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine"
                + " d2 this is a test1\",\"unit\":\"f\"},{\"MinValue\":\"1\",\"MaxValue\":\"100\"}",
            "root.turbine.d2.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 this"
                + " is a test2\",\"unit\":\"kw\"},{\"MinValue\":\"44.4\",\"MaxValue\":\"99.9\"}",
            "root.turbine.d2.s3,status,root.turbine,INT32,RLE,SNAPPY,{\"description\":\"turbine d2"
                + " this is a test3\"},{\"MinValue\":\"5\",\"MaxValue\":\"9\"}",
            "root.ln.d0.s0,temperature,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a "
                + "test1\",\"unit\":\"c\"},{\"H_Alarm\":\"1000\",\"M_Alarm\":\"500\"}",
            "root.ln.d0.s1,power,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a test2\",\""
                + "unit\":\"w\"},{\"H_Alarm\":\"9.9\",\"M_Alarm\":\"4.4\"}",
            "root.ln.d1.s0,status,root.ln,INT32,RLE,SNAPPY,{\"description\":\"ln this is a test3\"},"
                + "{\"H_Alarm\":\"90\",\"M_Alarm\":\"50\"}");

    List<String> retArray2 =
        Arrays.asList(
            "root.turbine.d2.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 "
                + "this is a test1\",\"unit\":\"f\"},{\"MinValue\":\"1\",\"MaxValue\":\"100\"}",
            "root.turbine.d2.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 this "
                + "is a test2\",\"unit\":\"kw\"},{\"MinValue\":\"44.4\",\"MaxValue\":\"99.9\"}",
            "root.turbine.d2.s3,status,root.turbine,INT32,RLE,SNAPPY,{\"description\":\"turbine d2 this "
                + "is a test3\"},{\"MinValue\":\"5\",\"MaxValue\":\"9\"}",
            "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
                + " tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
                + " gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s2,cpu,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
                + "cpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
                + "test2\",\"unit\":\"kw\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
            "root.turbine.d0.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine"
                + " this is a test1\",\"unit\":\"f\"},{\"H_Alarm\":\"100\",\"M_Alarm\":\"50\"}",
            "root.turbine.d1.s0,status,root.turbine,INT32,RLE,SNAPPY,{\"description\":\"turbine this is a "
                + "test3\"},{\"H_Alarm\":\"9\",\"M_Alarm\":\"5\"}",
            "root.ln.d0.s0,temperature,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a test1\""
                + ",\"unit\":\"c\"},{\"H_Alarm\":\"1000\",\"M_Alarm\":\"500\"}",
            "root.ln.d0.s1,power,root.ln,FLOAT,RLE,SNAPPY,{\"description\":\"ln this is a test2\",\""
                + "unit\":\"w\"},{\"H_Alarm\":\"9.9\",\"M_Alarm\":\"4.4\"}",
            "root.ln.d1.s0,status,root.ln,INT32,RLE,SNAPPY,{\"description\":\"ln this is a test3\"},"
                + "{\"H_Alarm\":\"90\",\"M_Alarm\":\"50\"}");

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("show timeseries");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("alias")
                + ","
                + resultSet.getString("storage group")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression")
                + ","
                + resultSet.getString("tags")
                + ","
                + resultSet.getString("attributes");

        assertTrue(retArray1.contains(ans));
        count++;
      }
      assertEquals(retArray1.size(), count);

      hasResultSet = statement.execute("show LATEST timeseries");
      Assert.assertTrue(hasResultSet);
      resultSet = statement.getResultSet();
      count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("alias")
                + ","
                + resultSet.getString("storage group")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression")
                + ","
                + resultSet.getString("tags")
                + ","
                + resultSet.getString("attributes");
        System.out.println("\"" + ans + "\",");
        assertTrue(retArray2.contains(ans));
        count++;
      }
      assertEquals(retArray2.size(), count);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void showTimeseriesOrderByHeatWithLimitTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "root.turbine.d2.s0,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2"
              + " this is a test1\",\"unit\":\"f\"},{\"MinValue\":\"1\",\"MaxValue\":\"100\"}",
          "root.turbine.d2.s1,power,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine d2 this "
              + "is a test2\",\"unit\":\"kw\"},{\"MinValue\":\"44.4\",\"MaxValue\":\"99.9\"}",
          "root.turbine.d2.s3,status,root.turbine,INT32,RLE,SNAPPY,{\"description\":\"turbine d2 this "
              + "is a test3\"},{\"MinValue\":\"5\",\"MaxValue\":\"9\"}",
          "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
              + "tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
          "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
              + "gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("show LATEST timeseries limit 5");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("alias")
                + ","
                + resultSet.getString("storage group")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression")
                + ","
                + resultSet.getString("tags")
                + ","
                + resultSet.getString("attributes");

        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void showTimeseriesOrderByHeatWithWhereTest() throws ClassNotFoundException {

    String[] retArray =
        new String[] {
          "root.turbine.d0.s4,tpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
              + " tpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
          "root.turbine.d0.s3,gpu0,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a "
              + "gpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
          "root.turbine.d0.s2,cpu,root.turbine,FLOAT,RLE,SNAPPY,{\"description\":\"turbine this is a"
              + " cpu\",\"unit\":\"cores\"},{\"H_Alarm\":\"99.9\",\"M_Alarm\":\"44.4\"}",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("show LATEST timeseries where unit=cores");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString("timeseries")
                + ","
                + resultSet.getString("alias")
                + ","
                + resultSet.getString("storage group")
                + ","
                + resultSet.getString("dataType")
                + ","
                + resultSet.getString("encoding")
                + ","
                + resultSet.getString("compression")
                + ","
                + resultSet.getString("tags")
                + ","
                + resultSet.getString("attributes");

        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createSchema() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
