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

package org.apache.iotdb.db.it.withoutNull;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBWithoutAllNullIT {

  private static final String[] dataSet =
      new String[] {
        "CREATE DATABASE root.testWithoutAllNull",
        "CREATE TIMESERIES root.testWithoutAllNull.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.testWithoutAllNull.d1.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.testWithoutAllNull.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1) " + "values(6, 26)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s2) " + "values(7, false)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2) " + "values(9, 29, true)",
        "flush",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2) " + "values(10, 20, true)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2,s3) "
            + "values(11, 21, false, 11.1)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2) " + "values(12, 22, true)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s2,s3) "
            + "values(13, 23, false, 33.3)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s1,s3) " + "values(14, 24, 44.4)",
        "INSERT INTO root.testWithoutAllNull.d1(timestamp,s2,s3) " + "values(15, true, 55.5)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void withoutAllNullTest1() {
    String[] retArray1 = new String[] {"6,20,true,null", "11,24,true,55.5"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.testWithoutAllNull.d1 GROUP BY([1, 21), 5ms) having last_value(s1) is not null || last_value(s2) is not null || last_value(s3) is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s1)")
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s2)")
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s3)");
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
  public void withoutAllNullTest2() {
    String[] retArray1 = new String[] {"11,24,true,55.5"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.testWithoutAllNull.d1 GROUP BY([1, 21), 5ms) having last_value(s1) is not null || last_value(s2) is not null || last_value(s3) is not null limit 1 offset 1")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s1)")
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s2)")
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s3)");
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
  public void withoutAllNullTest3() {
    String[] retArray1 = new String[] {"11,24,true,55.5"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.testWithoutAllNull.d1 GROUP BY([1, 21), 5ms) having last_value(s1) is not null and last_value(s2) is not null and last_value(s3) is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s1)")
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s2)")
                  + ","
                  + resultSet.getString("last_value(root.testWithoutAllNull.d1.s3)");
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
  public void withoutAllNullTest4() {
    String[] retArray1 = new String[] {"11,root.testWithoutAllNull.d1,24,true,55.5"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.testWithoutAllNull.d1 GROUP BY([1, 21), 5ms) having last_value(s1) is not null || last_value(s2) is not null || last_value(s3) is not null LIMIT 1 OFFSET 1 ALIGN BY DEVICE")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString("last_value(s1)")
                  + ","
                  + resultSet.getString("last_value(s2)")
                  + ","
                  + resultSet.getString("last_value(s3)");
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
  public void withoutAllNullTest5() {
    String[] retArray1 = new String[] {"6,root.testWithoutAllNull.d1,20,true,null"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.testWithoutAllNull.d1 GROUP BY([1, 21), 5ms) having last_value(s1) is not null || last_value(s2) is not null || last_value(s3) is not null "
                  + "ORDER BY TIME DESC LIMIT 1 OFFSET 1 ALIGN BY DEVICE")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("Device")
                  + ","
                  + resultSet.getString("last_value(s1)")
                  + ","
                  + resultSet.getString("last_value(s2)")
                  + ","
                  + resultSet.getString("last_value(s3)");
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
}
