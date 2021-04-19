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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 *
 * <p>This test stores NaN Values and retrieves them via SQL Interface.
 */
public class IoTDBInsertNaNIT {

  private static final String CREATE_TEMPLATE_SQL =
      "CREATE TIMESERIES root.vehicle.%s.%s WITH DATATYPE=%s, ENCODING=%s, MAX_POINT_NUMBER=%d";
  private static final String INSERT_TEMPLATE_SQL =
      "insert into root.vehicle.%s(timestamp,%s) values(%d,%s)";
  private static final String INSERT_BRAND_NEW_TEMPLATE_SQL =
      "insert into root.cycle.%s(timestamp,%s) values(%d,%s)";
  private static List<String> sqls = new ArrayList<>();
  private static final int TIMESTAMP = 10;
  private static final String VALUE = "NaN";
  private static final float DELTA_FLOAT = 0.0000001f;
  private static final double DELTA_DOUBLE = 0.0000001d;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    initCreateSQLStatement();
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void initCreateSQLStatement() {
    sqls.add("SET STORAGE GROUP TO root.vehicle.f0");
    sqls.add("SET STORAGE GROUP TO root.vehicle.d0");
    for (int i = 0; i < 10; i++) {
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "f0", "s" + i + "rle", "FLOAT", "RLE", i));
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "f0", "s" + i + "2f", "FLOAT", "TS_2DIFF", i));
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "d0", "s" + i + "rle", "DOUBLE", "RLE", i));
      sqls.add(String.format(CREATE_TEMPLATE_SQL, "d0", "s" + i + "2f", "DOUBLE", "TS_2DIFF", i));
    }
    for (int i = 0; i < 10; i++) {
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "f0", "s" + i + "rle", TIMESTAMP, VALUE));
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "f0", "s" + i + "2f", TIMESTAMP, VALUE));
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "d0", "s" + i + "rle", TIMESTAMP, VALUE));
      sqls.add(String.format(INSERT_TEMPLATE_SQL, "d0", "s" + i + "2f", TIMESTAMP, VALUE));
    }
  }

  private static void insertData() throws ClassNotFoundException {
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

  @Test
  public void selectAllSQLTest() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select * from root.vehicle.*");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          assertEquals(TIMESTAMP + "", resultSet.getString(TIMESTAMP_STR));
          for (int i = 0; i < 10; i++) {
            Assert.assertEquals(
                Float.parseFloat(VALUE),
                resultSet.getFloat(String.format("root.vehicle.%s.%s", "f0", "s" + i + "rle")),
                DELTA_FLOAT);
            Assert.assertEquals(
                Float.parseFloat(VALUE),
                resultSet.getFloat(String.format("root.vehicle.%s.%s", "f0", "s" + i + "2f")),
                DELTA_FLOAT);
            Assert.assertEquals(
                Double.parseDouble(VALUE),
                resultSet.getDouble(String.format("root.vehicle.%s.%s", "d0", "s" + i + "rle")),
                DELTA_DOUBLE);
            Assert.assertEquals(
                Double.parseDouble(VALUE),
                resultSet.getDouble(String.format("root.vehicle.%s.%s", "d0", "s" + i + "2f")),
                DELTA_DOUBLE);
          }
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectTest() throws ClassNotFoundException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TIMESERIES root.happy.device1.sensor1.temperature WITH DATATYPE=DOUBLE, ENCODING=RLE");
      statement.execute(
          "INSERT INTO root.happy.device1.sensor1(timestamp,temperature) values(7925, NaN)");
      boolean hasResultSet = statement.execute("select * from root.happy.device1.sensor1");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          assertEquals(7925 + "", resultSet.getString(TIMESTAMP_STR));
          assertEquals(
              Double.parseDouble(VALUE),
              resultSet.getDouble("root.happy.device1.sensor1.temperature"),
              DELTA_DOUBLE);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testNaNValue() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          String.format(INSERT_BRAND_NEW_TEMPLATE_SQL, "d0", "s0" + "2f", TIMESTAMP, VALUE));
      boolean hasResultSet = statement.execute("show timeseries");
      Assert.assertTrue(hasResultSet);
      boolean exist = false;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          if ((resultSet.getString("timeseries")).contains("root.cycle.d0.s0")) {
            exist = true;
          }
        }
      }
      assertTrue(exist);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
