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
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBWithoutAnyNullIT {

  private static final String[] dataSet =
      new String[] {
        "SET STORAGE GROUP TO root.testWithoutAnyNull",
        "CREATE TIMESERIES root.testWithoutAnyNull.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.testWithoutAnyNull.d1.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.testWithoutAnyNull.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) "
            + "values(1, 21, false, 11.1)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2) " + "values(2, 22, true)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) "
            + "values(3, 23, false, 33.3)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s3) " + "values(4, 24, 44.4)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s2,s3) " + "values(5, true, 55.5)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1) " + "values(6, 26)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s2) " + "values(7, false)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s3) " + "values(8, 88.8)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) " + "values(9, 29, true, 99.9)",
        "flush",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) "
            + "values(10, 20, true, 10.0)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) "
            + "values(11, 21, false, 11.1)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2) " + "values(12, 22, true)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) "
            + "values(13, 23, false, 33.3)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s3) " + "values(14, 24, 44.4)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s2,s3) " + "values(15, true, 55.5)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1) " + "values(16, 26)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s2) " + "values(17, false)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s3) " + "values(18, 88.8)",
        "INSERT INTO root.testWithoutAnyNull.d1(timestamp,s1,s2,s3) " + "values(19, 29, true, 99.9)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(86400);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void withoutAnyNullTest1() {
    String[] retArray1 =
        new String[] {
          "1,21,false,11.1",
          "3,23,false,33.3",
          "9,29,true,99.9",
          "10,20,true,10.0",
          "11,21,false,11.1",
          "13,23,false,33.3",
          "19,29,true,99.9"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select * from root.testWithoutAnyNull.d1 WITHOUT NULL ANY");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s1")
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s2")
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s3");
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void withoutAnyNullTest2() {
    String[] retArray =
        new String[] {"10,20,true,10.0", "11,21,false,11.1", "13,23,false,33.3", "19,29,true,99.9"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.testWithoutAnyNull.d1 WHERE time >= 10 WITHOUT NULL ANY");

      int cnt;
      assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s1")
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s2")
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s3");
          assertEquals(retArray[cnt], ans);
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void withoutAnyNullTest3() {
    String[] retArray1 =
        new String[] {
          "3,23,false,33.3",
          "9,29,true,99.9",
          "10,20,true,10.0",
          "11,21,false,11.1",
          "13,23,false,33.3"
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.testWithoutAnyNull.d1 WITHOUT NULL ANY limit 5 offset 1");

      assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s1")
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s2")
                  + ","
                  + resultSet.getString("root.testWithoutAnyNull.d1.s3");
          assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
