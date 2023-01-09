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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBWithoutNullAnyFilterIT {

  private static String[] dataSet1 =
      new String[] {
        "CREATE DATABASE root.test",
        "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg1.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg2.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE ALIGNED TIMESERIES root.test.sg3(s5 INT32, s6 BOOLEAN, s7 DOUBLE, s8 INT32)",
        "CREATE TIMESERIES root.test.sg5.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.test.sg5.s9 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(1, true, 1, 1.0, 1)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(1, false, 1, 1.0, 1)",
        "INSERT INTO root.test.sg1(timestamp, s2) " + "values(2, 2)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(2, 2.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) " + "values(2, 2)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(2, true, 2, 2.0, 2)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp, s1) " + "values(3, false)",
        "INSERT INTO root.test.sg1(timestamp, s2) " + "values(5, 5)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(5, 5.0)",
        "INSERT INTO root.test.sg1(timestamp, s4) " + "values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s2) " + "values(5, 5)",
        "INSERT INTO root.test.sg2(timestamp, s3) " + "values(5, 5.0)",
        "INSERT INTO root.test.sg2(timestamp, s4) " + "values(5, 5)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(6, true, 6, 6.0, 6)",
        "INSERT INTO root.test.sg1(timestamp, s1) " + "values(7, true)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(7, 7.0)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) " + "values(7, true, 7, 7.0)",
        "flush",
        "INSERT INTO root.test.sg1(timestamp, s1) " + "values(8, true)",
        "INSERT INTO root.test.sg1(timestamp, s2) " + "values(8, 8)",
        "INSERT INTO root.test.sg1(timestamp, s3) " + "values(8, 8.0)",
        "INSERT INTO root.test.sg2(timestamp, s3) " + "values(8, 8.0)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(9, false, 9, 9.0, 9)",
        "INSERT INTO root.test.sg2(timestamp, s1) " + "values(9, true)",
        "flush",
        "INSERT INTO root.test.sg2(timestamp, s2) " + "values(9, 9)",
        "INSERT INTO root.test.sg2(timestamp, s4) " + "values(9, 9)",
        "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) " + "values(10, true, 10, 10.0, 10)",
        "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) " + "values(10, true, 10, 10.0, 10)",
        "flush",
        "INSERT INTO root.test.sg3(time, s5, s6, s7, s8) aligned values(1, 1, true, 1.0, 1)",
        "INSERT INTO root.test.sg3(time, s6, s7, s8) aligned values(2, false, 2.0, 2)",
        "INSERT INTO root.test.sg3(time, s5, s7, s8) aligned values(3, 3, 3.0, 3)",
        "INSERT INTO root.test.sg3(time, s5) aligned values(5, 5)",
        "INSERT INTO root.test.sg3(time, s5, s8) aligned values(6, 6, 6)",
        "INSERT INTO root.test.sg3(time, s6) aligned values(7, true)",
        "INSERT INTO root.test.sg3(time, s5, s7) aligned values(8, 8, 8.0)",
        "INSERT INTO root.test.sg3(time, s5, s7, s8) aligned values(9, 9, 9.0, 9)",
        "INSERT INTO root.test.sg3(time, s7, s8) aligned values(10, 10.0, 10)",
        "INSERT INTO root.test.sg5(timestamp, s1, s9) " + "values(1, true, 1)",
        "flush",
      };

  private static final String TIMESTAMP_STR = "Time";

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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
  public void rawDataWithoutValueFilterQueryTest() {
    System.out.println("rawDataWithoutValueFilterQueryTest");
    String[] retArray1 =
        new String[] {
          "1,true,1,1.0,1",
          "2,null,2,2.0,2",
          "5,null,5,5.0,5",
          "6,true,6,6.0,6",
          "8,true,8,8.0,null",
          "9,false,9,9.0,9",
          "10,true,10,10.0,10"
        };
    String[] retArray2 =
        new String[] {
          "1,true,1,1.0,1",
          "3,false,null,null,null",
          "6,true,6,6.0,6",
          "7,true,null,7.0,null",
          "8,true,8,8.0,null",
          "9,false,9,9.0,9",
          "10,true,10,10.0,10"
        };
    String[] retArray3 =
        new String[] {"1,true,1,1.0,1", "6,true,6,6.0,6", "9,false,9,9.0,9", "10,true,10,10.0,10"};
    String[] retArray4 =
        new String[] {"1,true,1,1.0,1", "6,true,6,6.0,6", "9,false,9,9.0,9", "10,true,10,10.0,10"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.sg1 where s2 is not null && s3 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.test.sg1 where s1 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.test.sg1 where * is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.sg1 where s1 is not null && s2 is not null && s3 is not null && s4 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void rawDataWithValueFilterQueryTest() {
    System.out.println("rawDataWithValueFilterQueryTest");
    String[] retArray1 = new String[] {"9,false,9,9.0,9"};
    String[] retArray2 = new String[] {"1,true,1,1.0,1"};
    String[] retArray3 = new String[] {"1,true,1,1.0,1", "6,true,6,6.0,6", "10,true,10,10.0,10"};
    String[] retArray4 = new String[] {"1,true,1,1.0,1", "6,true,6,6.0,6", "10,true,10,10.0,10"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.sg1 where s1 = false && s2 is not null && s3 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.test.sg1 where s2 = 1 && s1 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      Assert.assertFalse(
          statement
              .executeQuery("select * from root.test.sg1 where s2 = 2 && s1 is not null")
              .next());

      try (ResultSet resultSet =
          statement.executeQuery("select * from root.test.sg1 where s1 = true && * is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select * from root.test.sg1 where s1 = true && s1 is not null && s2 is not null && s3 is not null && s4 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s3")
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void withExpressionQueryTest() {
    System.out.println("withExpressionQueryTest");
    String[] retArray1 =
        new String[] {
          "1,1,-1,1,1,2.0,0.0,1.0,1.0,0.0",
          "2,2,-2,2,2,4.0,0.0,4.0,1.0,0.0",
          "5,5,-5,5,5,10.0,0.0,25.0,1.0,0.0",
          "6,6,-6,6,6,12.0,0.0,36.0,1.0,0.0",
          "9,9,-9,9,9,18.0,0.0,81.0,1.0,0.0",
          "10,10,-10,10,10,20.0,0.0,100.0,1.0,0.0"
        };
    String[] retArray2 =
        new String[] {
          "1,1,-1,1,1,2.0,0.0,1.0,1.0,0.0",
          "2,2,-2,2,2,4.0,0.0,4.0,1.0,0.0",
          "5,5,-5,5,5,10.0,0.0,25.0,1.0,0.0",
          "6,6,-6,6,6,12.0,0.0,36.0,1.0,0.0",
          "9,9,-9,9,9,18.0,0.0,81.0,1.0,0.0",
          "10,10,-10,10,10,20.0,0.0,100.0,1.0,0.0"
        };
    String[] retArray3 =
        new String[] {
          "1,1,-1,1,1,2.0,0.0,1.0,1.0,0.0",
          "2,2,-2,2,2,4.0,0.0,4.0,1.0,0.0",
          "5,5,-5,5,5,10.0,0.0,25.0,1.0,0.0",
          "6,6,-6,6,6,12.0,0.0,36.0,1.0,0.0",
          "9,9,-9,9,9,18.0,0.0,81.0,1.0,0.0",
          "10,10,-10,10,10,20.0,0.0,100.0,1.0,0.0"
        };
    String[] retArray4 =
        new String[] {
          "1,1,-1,1,1,2.0,0.0,1.0,1.0,0.0",
          "2,2,-2,2,2,4.0,0.0,4.0,1.0,0.0",
          "5,5,-5,5,5,10.0,0.0,25.0,1.0,0.0",
          "6,6,-6,6,6,12.0,0.0,36.0,1.0,0.0",
          "9,9,-9,9,9,18.0,0.0,81.0,1.0,0.0",
          "10,10,-10,10,10,20.0,0.0,100.0,1.0,0.0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 where (s2+s4) is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("-root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 + root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 - root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 * root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 / root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 % root.test.sg1.s4");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 where s2+s4 is not null && s2 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("-root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 + root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 - root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 * root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 / root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 % root.test.sg1.s4");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 "
                  + "where s2 is not null && s4 is not null && s2 / s4 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("-root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 + root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 - root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 * root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 / root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 % root.test.sg1.s4");
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 "
                  + "where s2 is not null && s4 is not null && s2 / s4 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("-root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 + root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 - root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 * root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 / root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s2 % root.test.sg1.s4");
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void withUDFQueryTest() {
    System.out.println("withUDFQueryTest");
    String[] retArray1 =
        new String[] {
          "1,true,0.84,0.54,1.56",
          "6,true,-0.28,0.96,-0.29",
          "8,true,0.99,-0.15,-6.80",
          "9,false,0.41,-0.91,-0.45",
          "10,true,-0.54,-0.84,0.65"
        };
    String[] retArray2 =
        new String[] {
          "1,true,1.38,-0.33",
          "2,null,0.49,0.32",
          "5,null,-0.68,-0.25",
          "6,true,0.68,0.68",
          "9,false,-0.50,-0.38",
          "10,true,-1.38,-0.08"
        };
    String[] retArray3 =
        new String[] {
          "1,true,-0.33",
          "2,null,0.32",
          "5,null,-0.25",
          "6,true,0.68",
          "9,false,-0.38",
          "10,true,-0.08"
        };
    String[] retArray4 =
        new String[] {
          "1,true,1.38,-0.33", "6,true,0.68,0.68", "9,false,-0.50,-0.38", "10,true,-1.38,-0.08"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1, sin(s2), cos(s2), tan(s2) from root.test.sg1 where sin(s2) is not null && s1 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + (resultSet.getString("sin(root.test.sg1.s2)") == null
                          || resultSet.getString("sin(root.test.sg1.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("sin(root.test.sg1.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("cos(root.test.sg1.s2)") == null
                          || resultSet.getString("cos(root.test.sg1.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("cos(root.test.sg1.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("tan(root.test.sg1.s2)") == null
                          || resultSet.getString("tan(root.test.sg1.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("tan(root.test.sg1.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      String[] columns =
          new String[] {
            "root.test.sg1.s1",
            "sin(root.test.sg1.s2) + cos(root.test.sg1.s2)",
            "cos(sin(root.test.sg1.s2 + root.test.sg1.s4) + root.test.sg1.s2)"
          };

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1, sin(s2) + cos(s2), cos(sin(s2 + s4) + s2) from root.test.sg1 where sin(s2) + cos(s2) is not null && cos(sin(s2 + s4) + s2) is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[1]) == null
                          || resultSet.getString(columns[1]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[1]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[2]) == null
                          || resultSet.getString(columns[2]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[2]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1, cos(sin(s2 + s4) + s2) from root.test.sg1 where cos(sin(s2 + s4) + s2) is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[2]) == null
                          || resultSet.getString(columns[2]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[2]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1, sin(s2) + cos(s2), cos(sin(s2 + s4) + s2) from root.test.sg1 where s1 is not null && sin(s2) + cos(s2) is not null && cos(sin(s2 + s4) + s2) is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[1]) == null
                          || resultSet.getString(columns[1]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[1]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[2]) == null
                          || resultSet.getString(columns[2]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[2]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void withGroupByTimeQueryTest() {
    System.out.println("withGroupByTimeQueryTest");
    String[] retArray1 = new String[] {"1,1.50,3.00", "5,5.50,11.00", "7,null,8.00", "9,9.00,9.00"};
    String[] retArray2 = new String[] {"1,1.50,3.00,2", "5,5.50,11.00,2", "9,9.00,9.00,1"};
    String[] retArray3 =
        new String[] {"1,1.00,1.00", "2,2.00,2.00", "5,5.00,5.00", "6,6.00,6.00", "9,9.00,9.00"};
    String[] retArray4 =
        new String[] {
          "1,1.50,3.00,1.50", "5,5.50,11.00,5.50", "7,null,15.00,8.00", "9,9.00,9.00,9.00"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s4), sum(s2) from root.test.sg1 group by ([1,10), 2ms) without null any(sum(s2))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString("avg(root.test.sg1.s4)") == null
                          || resultSet.getString("avg(root.test.sg1.s4)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("avg(root.test.sg1.s4)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("sum(root.test.sg1.s2)") == null
                          || resultSet.getString("sum(root.test.sg1.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("sum(root.test.sg1.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      String[] columns =
          new String[] {
            "avg(root.test.sg1.s4)", "sum(root.test.sg1.s2)", "count(root.test.sg1.s3)"
          };

      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s4), sum(s2), count(s3) from root.test.sg1 group by ([1,10), 2ms) without null any(avg(s4), sum(s2))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString(columns[0]) == null
                          || resultSet.getString(columns[0]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[0]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[1]) == null
                          || resultSet.getString(columns[1]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[1]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + resultSet.getString(columns[2]);
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s4) as t, avg(s2) from root.test.sg1 group by ([1,10), 1ms) without null any(t, avg(s2))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString("t") == null || resultSet.getString("t").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("t"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("avg(root.test.sg1.s2)") == null
                          || resultSet.getString("avg(root.test.sg1.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("avg(root.test.sg1.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s4), sum(s3) as t, avg(s2) from root.test.sg1 group by ([1,10), 2ms) without null any(t, avg(s2))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString("avg(root.test.sg1.s4)") == null
                          || resultSet.getString("avg(root.test.sg1.s4)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("avg(root.test.sg1.s4)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("t") == null || resultSet.getString("t").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("t"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("avg(root.test.sg1.s2)") == null
                          || resultSet.getString("avg(root.test.sg1.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("avg(root.test.sg1.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void withGroupByLevelQueryTest() {
    System.out.println("withGroupByLevelQueryTest");
    String[] retArray1 =
        new String[] {
          "1,1.50,1.50,3.00,3.00",
          "5,5.50,5.50,11.00,11.00",
          "7,8.00,7.00,null,null",
          "9,9.00,9.00,9.00,9.00"
        };
    String[] retArray2 =
        new String[] {"1,1.50,1.50,3.00,3.00", "5,5.50,5.50,11.00,11.00", "9,9.00,9.00,9.00,9.00"};

    String[] retArray3 = new String[] {"1,1.50,3.00", "5,5.50,11.00", "9,9.00,9.00"};
    String[] retArray4 = new String[] {"5.79"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] columns =
          new String[] {
            "avg(root.*.sg1.s2)", "avg(root.*.sg2.s2)", "sum(root.*.sg1.s4)", "sum(root.*.sg2.s4)"
          };

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s2), sum(s4) from root.test.** group by ([1, 10), 2ms), level = 2 without null any(avg(s2))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString(columns[0]) == null
                          || resultSet.getString(columns[0]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[0]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[1]) == null
                          || resultSet.getString(columns[1]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[1]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[2]) == null
                          || resultSet.getString(columns[2]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[2]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[3]) == null
                          || resultSet.getString(columns[3]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[3]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s2), sum(s4) from root.test.** group by ([1, 10), 2ms), level = 2 without null any(avg(s2), sum(s4))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString(columns[0]) == null
                          || resultSet.getString(columns[0]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[0]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[1]) == null
                          || resultSet.getString(columns[1]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[1]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[2]) == null
                          || resultSet.getString(columns[2]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[2]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString(columns[3]) == null
                          || resultSet.getString(columns[3]).equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString(columns[3]))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(s2), sum(s4) as t from root.*.sg1 group by ([1, 10), 2ms), level = 1 without null any (avg(s2), t)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + (resultSet.getString("avg(root.test.*.s2)") == null
                          || resultSet.getString("avg(root.test.*.s2)").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("avg(root.test.*.s2)"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString())
                  + ","
                  + (resultSet.getString("t") == null || resultSet.getString("t").equals("null")
                      ? "null"
                      : new BigDecimal(resultSet.getString("t"))
                          .setScale(2, RoundingMode.HALF_UP)
                          .toPlainString());
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select avg(sg1.s2), avg(sg2.s2) from root.test group by level = 1 without null any(avg(sg1.s2), avg(sg2.s2))")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              new BigDecimal(resultSet.getString("avg(root.test.*.s2)"))
                  .setScale(2, RoundingMode.HALF_UP)
                  .toPlainString();
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void withoutNullColumnsIsFullPathQueryTest() {
    System.out.println("withoutNullColumnsIsFullPathQueryTest");
    String[] retArray1 =
        new String[] {
          "1,1,1.0,1,1.0",
          "2,2,2.0,2,2.0",
          "5,5,5.0,5,5.0",
          "6,6,6.0,6,6.0",
          "8,8,8.0,null,8.0",
          "10,10,10.0,10,10.0"
        };
    String[] retArray2 =
        new String[] {
          "1,1,1.0,1,1.0",
          "2,2,2.0,2,2.0",
          "5,5,5.0,5,5.0",
          "6,6,6.0,6,6.0",
          "8,8,8.0,null,8.0",
          "9,9,9.0,9,null",
          "10,10,10.0,10,10.0"
        };
    String[] retArray3 =
        new String[] {
          "1,1,1.0,1,1.0",
          "2,2,2.0,2,2.0",
          "5,5,5.0,5,5.0",
          "6,6,6.0,6,6.0",
          "8,8,8.0,null,8.0",
          "10,10,10.0,10,10.0"
        };
    String[] retArray4 =
        new String[] {
          "1,1,1.0,1,1.0",
          "2,2,2.0,2,2.0",
          "5,5,5.0,5,5.0",
          "6,6,6.0,6,6.0",
          "9,9,9.0,9,null",
          "10,10,10.0,10,10.0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] columns =
          new String[] {
            "root.test.sg1.s2", "root.test.sg1.s3", "root.test.sg2.s2", "root.test.sg2.s3"
          };

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, s3 from root.test.** without null any(root.test.sg1.s2, root.test.sg2.s3)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, s3 from root.test.sg1, root.test.sg2 without null any(root.test.sg1.s2)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, s3 from root.test.sg1, root.test.sg2 without null any(root.test.sg1.s2, s3)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s2, s3 from root.test.sg1, root.test.sg2 without null any(root.test.*.s2)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // meta check whether the specified exception occur
  @Test
  public void withoutNullColumnsMisMatchSelectedQueryTest() {
    System.out.println("withoutNullColumnsMisMatchSelectedQueryTest");
    String[] expectedHeader =
        new String[] {
          TIMESTAMP_STR,
          "root.test.sg1.s1",
          "root.test.sg1.s2",
          "root.test.sg1.s3",
          "root.test.sg1.s4",
        };
    String[] retArray =
        new String[] {
          "1,true,1,1.0,1,",
          "3,false,null,null,null,",
          "6,true,6,6.0,6,",
          "7,true,null,7.0,null,",
          "8,true,8,8.0,null,",
          "9,false,9,9.0,9,",
          "10,true,10,10.0,10,"
        };
    resultSetEqualTest(
        "select s1, s2, s3, s4 from root.test.sg1 where s1 is not null && usage is null",
        expectedHeader,
        retArray);
  }

  @Ignore
  @Test
  public void alignByDeviceQueryTest() {
    System.out.println("alignByDeviceQueryTest");
    String[] retArray1 =
        new String[] {"1,true,2,2.0,2", "5,true,6,6.0,6", "7,true,8,8.0,null", "9,false,9,9.0,9"};
    String[] retArray2 =
        new String[] {"1,true,2,2.0,2", "5,true,6,6.0,6", "7,true,8,8.0,null", "9,false,9,9.0,9"};
    String[] retArray3 =
        new String[] {
          "1,true,2,2.0,2",
          "5,true,6,6.0,6",
          "7,true,8,8.0,null",
          "9,false,9,9.0,9",
          "1,true,2,2.0,2",
          "5,true,6,6.0,6",
          "7,true,7,8.0,null",
          "9,true,9,null,9"
        };
    String[] retArray4 =
        new String[] {
          "1,true,2,2.0,2",
          "5,true,6,6.0,6",
          "7,true,8,8.0,null",
          "9,false,9,9.0,9",
          "1,true,2,2.0,2",
          "5,true,6,6.0,6",
          "7,true,7,8.0,null"
        };
    String[] retArray5 = new String[] {"1,2,2.0,2", "5,6,6.0,6", "7,8,8.0,null", "9,9,9.0,9"};
    String[] retArray6 =
        new String[] {
          "true,1,null",
          "null,2,null",
          "null,5,null",
          "true,6,null",
          "true,8,null",
          "false,9,null",
          "true,10,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      String[] columns =
          new String[] {"last_value(s1)", "last_value(s2)", "last_value(s3)", "last_value(s4)"};

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null any(last_value(s2), last_value(s3)) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null any(last_value(s2)) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.* group by([1,10), 2ms) without null any(last_value(s2)) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.* group by([1,10), 2ms) without null any(last_value(s2), last_value(s3)) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s2) as t, last_value(s3), last_value(s4) from root.test.sg1 group by([1,10), 2ms) without null any(t, last_value(s3)) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("t")
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray5[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray5.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s1,s2,s9 from root.test.sg1,root.test.sg5 without null any(s2) align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString("s1")
                  + ","
                  + resultSet.getString("s2")
                  + ","
                  + resultSet.getString("s9");
          Assert.assertEquals(retArray6[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray6.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void alignTimeSeriesQueryTest() {
    System.out.println("alignTimeSeriesQueryTest");
    String[] retArray1 = new String[] {"1,true,1,1.0,1,true,1.0,1"};
    String[] retArray2 = new String[] {"1,true,1,1.0,1,true,1.0,1"};
    String[] retArray3 = new String[] {"1,true,1,1.0,1,true,1.0,1"};
    String[] retArray4 = new String[] {"1,true,1,1.0,1,true,1.0,1"};
    String[] retArray5 = new String[] {"1,true,1,1.0,1,true,1.0,1"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test where sg3.* is not null && sg1.s2 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg2.s3")
                  + ","
                  + resultSet.getString("root.test.sg3.s5")
                  + ","
                  + resultSet.getString("root.test.sg3.s6")
                  + ","
                  + resultSet.getString("root.test.sg3.s7")
                  + ","
                  + resultSet.getString("root.test.sg3.s8");
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test where sg3.* is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg2.s3")
                  + ","
                  + resultSet.getString("root.test.sg3.s5")
                  + ","
                  + resultSet.getString("root.test.sg3.s6")
                  + ","
                  + resultSet.getString("root.test.sg3.s7")
                  + ","
                  + resultSet.getString("root.test.sg3.s8");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test where sg3.s5 is not null && sg3.s6 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg2.s3")
                  + ","
                  + resultSet.getString("root.test.sg3.s5")
                  + ","
                  + resultSet.getString("root.test.sg3.s6")
                  + ","
                  + resultSet.getString("root.test.sg3.s7")
                  + ","
                  + resultSet.getString("root.test.sg3.s8");
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test where sg3.s5 is not null && sg3.s6 is not null && sg2.s3 is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg2.s3")
                  + ","
                  + resultSet.getString("root.test.sg3.s5")
                  + ","
                  + resultSet.getString("root.test.sg3.s6")
                  + ","
                  + resultSet.getString("root.test.sg3.s7")
                  + ","
                  + resultSet.getString("root.test.sg3.s8");
          Assert.assertEquals(retArray4[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray4.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select sg1.s1, sg1.s2, sg2.s3, sg3.* from root.test where sg1.s1 is not null && sg1.s2 is not null && sg2.s3 is not null && sg3.* is not null")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + resultSet.getString("root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg2.s3")
                  + ","
                  + resultSet.getString("root.test.sg3.s5")
                  + ","
                  + resultSet.getString("root.test.sg3.s6")
                  + ","
                  + resultSet.getString("root.test.sg3.s7")
                  + ","
                  + resultSet.getString("root.test.sg3.s8");
          Assert.assertEquals(retArray5[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray5.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void withLimitOffsetQueryTest() {
    // last_value(s3) | last_value(s4) | last_value(s1) | last_value(s2)
    System.out.println("withLimitOffsetQueryTest");
    String[] retArray1 = new String[] {"9,false,9,9.0,9"};
    String[] retArray2 = new String[] {"9,false,9.0,9"};
    String[] retArray3 = new String[] {"7,true,8,null", "9,false,9,9"};
    String[] columns =
        new String[] {"last_value(s1)", "last_value(s2)", "last_value(s3)", "last_value(s4)"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null any(last_value(s2), last_value(s3)) limit 5 offset 3 align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null any(last_value(s4), last_value(s3)) limit 5 offset 2 slimit 3 align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[2])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.test.sg1 group by([1,10), 2ms) without null any(last_value(s2)) limit 5 offset 2 slimit 3 soffset 1 align by device")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[3]);
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
