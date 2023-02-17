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
public class IoTDBWithoutAnyNullIT {

  private static final String[] dataSet =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.d1.s2 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.d1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(1, 21, false, 11.1)",
        "INSERT INTO root.test.d1(timestamp,s1,s2) " + "values(2, 22, true)",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(3, 23, false, 33.3)",
        "INSERT INTO root.test.d1(timestamp,s1,s3) " + "values(4, 24, 44.4)",
        "INSERT INTO root.test.d1(timestamp,s2,s3) " + "values(5, true, 55.5)",
        "INSERT INTO root.test.d1(timestamp,s1) " + "values(6, 26)",
        "INSERT INTO root.test.d1(timestamp,s2) " + "values(7, false)",
        "INSERT INTO root.test.d1(timestamp,s3) " + "values(8, 88.8)",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(9, 29, true, 99.9)",
        "flush",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(10, 20, true, 10.0)",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(11, 21, false, 11.1)",
        "INSERT INTO root.test.d1(timestamp,s1,s2) " + "values(12, 22, true)",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(13, 23, false, 33.3)",
        "INSERT INTO root.test.d1(timestamp,s1,s3) " + "values(14, 24, 44.4)",
        "INSERT INTO root.test.d1(timestamp,s2,s3) " + "values(15, true, 55.5)",
        "INSERT INTO root.test.d1(timestamp,s1) " + "values(16, 26)",
        "INSERT INTO root.test.d1(timestamp,s2) " + "values(17, false)",
        "INSERT INTO root.test.d1(timestamp,s3) " + "values(18, 88.8)",
        "INSERT INTO root.test.d1(timestamp,s1,s2,s3) " + "values(19, 29, true, 99.9)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.d1 where s1 is not null && s2 is not null && s3 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.d1.s1")
                  + ","
                  + resultSet.getString("root.test.d1.s2")
                  + ","
                  + resultSet.getString("root.test.d1.s3");
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.d1 WHERE time >= 10 && s1 is not null && s2 is not null && s3 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.d1.s1")
                  + ","
                  + resultSet.getString("root.test.d1.s2")
                  + ","
                  + resultSet.getString("root.test.d1.s3");
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.d1 where s1 is not null && s2 is not null && s3 is not null limit 5 offset 1")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.d1.s1")
                  + ","
                  + resultSet.getString("root.test.d1.s2")
                  + ","
                  + resultSet.getString("root.test.d1.s3");
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
  public void withoutAnyNull_withValueFilterQueryTest() {
    String[] retArray1 = new String[] {"9,29,true,99.9", "10,20,true,10.0", "19,29,true,99.9"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.d1 where s2 = true && s1 is not null && s2 is not null && s3 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.d1.s1")
                  + ","
                  + resultSet.getString("root.test.d1.s2")
                  + ","
                  + resultSet.getString("root.test.d1.s3");
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
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
