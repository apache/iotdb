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
package org.apache.iotdb.db.integration.without_null_filter;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBWithoutNullAllFilterIT {

  private static String[] dataSet1 =
      new String[] {
          "SET STORAGE GROUP TO root.test",
          "CREATE TIMESERIES root.test.sg1.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg1.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg1.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s1 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s3 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
          "CREATE TIMESERIES root.test.sg2.s4 WITH DATATYPE=INT32, ENCODING=PLAIN",

          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) "
              + "values(1, true, 1, 1.0, 1)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) "
              + "values(1, false, 1, 1.0, 1)",

          "INSERT INTO root.test.sg1(timestamp, s2) "
              + "values(2, 2)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(2, 2.0)",
          "INSERT INTO root.test.sg1(timestamp, s4) "
              + "values(2, 2)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) "
              + "values(2, true, 2, 2.0, 2)",
          "flush",

          "INSERT INTO root.test.sg1(timestamp, s1) "
              + "values(3, false)",
          "INSERT INTO root.test.sg1(timestamp, s2) "
              + "values(5, 5)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(5, 5.0)",
          "INSERT INTO root.test.sg1(timestamp, s4) "
              + "values(5, 5)",
          "INSERT INTO root.test.sg2(timestamp, s2) "
              + "values(5, 5)",
          "INSERT INTO root.test.sg2(timestamp, s3) "
              + "values(5, 5.0)",
          "INSERT INTO root.test.sg2(timestamp, s4) "
              + "values(5, 5)",
          "flush",

          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) "
              + "values(6, true, 6, 6.0, 6)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) "
              + "values(6, true, 6, 6.0, 6)",
          "INSERT INTO root.test.sg1(timestamp, s1) "
              + "values(7, true)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(7, 7.0)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3) "
              + "values(7, true, 7, 7.0)",
          "flush",

          "INSERT INTO root.test.sg1(timestamp, s1) "
              + "values(8, true)",
          "INSERT INTO root.test.sg1(timestamp, s2) "
              + "values(8, 8)",
          "INSERT INTO root.test.sg1(timestamp, s3) "
              + "values(8, 8.0)",
          "INSERT INTO root.test.sg2(timestamp, s3) "
              + "values(8, 8.0)",
          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) "
              + "values(9, false, 9, 9.0, 9)",
          "INSERT INTO root.test.sg2(timestamp, s1) "
              + "values(9, true)",
          "flush",

          "INSERT INTO root.test.sg2(timestamp, s2) "
              + "values(9, 9)",
          "INSERT INTO root.test.sg2(timestamp, s4) "
              + "values(9, 9)",
          "INSERT INTO root.test.sg1(timestamp,s1,s2, s3, s4) "
              + "values(10, true, 10, 10.0, 10)",
          "INSERT INTO root.test.sg2(timestamp,s1,s2, s3, s4) "
              + "values(10, true, 10, 10.0, 10)",
          "flush",
      };

  private static final String TIMESTAMP_STR = "Time";
  private long prevPartitionInterval;

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setUp() throws Exception {
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initBeforeTest();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
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
            "7,true,null,7.0,null",
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
        new String[] {
            "1,true,1,1.0,1",
            "2,null,2,2.0,2",
            "3,false,null,null,null",
            "5,null,5,5.0,5",
            "6,true,6,6.0,6",
            "7,true,null,7.0,null",
            "8,true,8,8.0,null",
            "9,false,9,9.0,9",
            "10,true,10,10.0,10"
        };
    String[] retArray4 =
        new String[] {
            "1,true,1,1.0,1",
            "2,null,2,2.0,2",
            "3,false,null,null,null",
            "5,null,5,5.0,5",
            "6,true,6,6.0,6",
            "7,true,null,7.0,null",
            "8,true,8,8.0,null",
            "9,false,9,9.0,9",
            "10,true,10,10.0,10"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all (s2, s3)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all (s1)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"))
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"))
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 without null all(s1, s2, s3, s4)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"))
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
    String[] retArray1 =
        new String[] {
            "9,false,9,9.0,9"
        };
    String[] retArray2 =
        new String[] {
            "1,true,1,1.0,1"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s1 = false without null all (s2, s3)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s2 = 1 without null all (s1)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.test.sg1.s1"))
                  + ","
                  + resultSet.getString(sum("root.test.sg1.s2"))
                  + ","
                  + resultSet.getString(avg("root.test.sg1.s3"))
                  + ","
                  + resultSet.getString("root.test.sg1.s4");
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s2 = 2 without null all (s1)");

      Assert.assertFalse(hasResultSet);

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s1 = true without null all");

      Assert.assertFalse(hasResultSet);

      hasResultSet =
          statement.execute(
              "select * from root.test.sg1 where s1 = true without null all(s1,s2,s3,s4)");

      Assert.assertFalse(hasResultSet);
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
            "1,1,-1,1,1,2,0,1,1,0",
            "2,2,-2,2,2,4,0,4,1,0",
            "5,5,-5,5,5,10,0,25,1,0",
            "6,6,-6,6,6,12,0,36,1,0",
            "9,9,-9,9,9,18,0,81,1,0",
            "10,10,-10,10,10,20,0,100,1,0"
        };
    String[] retArray2 =
        new String[] {
            "1,1,-1,1,1,2,0,1,1,0",
            "2,2,-2,2,2,4,0,4,1,0",
            "5,5,-5,5,5,10,0,25,1,0",
            "6,6,-6,6,6,12,0,36,1,0",
            "8,8,-8,null,null,null,null,null,null,null",
            "9,9,-9,9,9,18,0,81,1,0",
            "10,10,-10,10,10,20,0,100,1,0"
        };
    String[] retArray3 =
        new String[] {
            "1,1,-1,1,1,2,0,1,1,0",
            "2,2,-2,2,2,4,0,4,1,0",
            "5,5,-5,5,5,10,0,25,1,0",
            "6,6,-6,6,6,12,0,36,1,0",
            "8,8,-8,null,null,null,null,null,null,null",
            "9,9,-9,9,9,18,0,81,1,0",
            "10,10,-10,10,10,20,0,100,1,0"
        };
    String[] retArray4 =
        new String[] {
            "1,1,-1,1,1,2,0,1,1,0",
            "2,2,-2,2,2,4,0,4,1,0",
            "5,5,-5,5,5,10,0,25,1,0",
            "6,6,-6,6,6,12,0,36,1,0",
            "8,8,-8,null,null,null,null,null,null,null",
            "9,9,-9,9,9,18,0,81,1,0",
            "10,10,-10,10,10,20,0,100,1,0"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (s2+s4)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (s2+s4, s2)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without "
                  + "null all(s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select s2, - s2, s4, + s4, s2 + s4, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without "
                  + "null all");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
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
  public void withAliasQueryTest() {
    System.out.println("withAliasQueryTest");
    String[] retArray1 =
        new String[] {
            "1,1,-1,1,1,2,0,1,1,0",
            "2,2,-2,2,2,4,0,4,1,0",
            "5,5,-5,5,5,10,0,25,1,0",
            "6,6,-6,6,6,12,0,36,1,0",
            "9,9,-9,9,9,18,0,81,1,0",
            "10,10,-10,10,10,20,0,100,1,0"
        };
    String[] retArray2 =
        new String[] {
            "1,1,-1,1,1,2,0,1,1,0",
            "2,2,-2,2,2,4,0,4,1,0",
            "5,5,-5,5,5,10,0,25,1,0",
            "6,6,-6,6,6,12,0,36,1,0",
            "8,8,-8,null,null,null,null,null,null,null",
            "9,9,-9,9,9,18,0,81,1,0",
            "10,10,-10,10,10,20,0,100,1,0"
        };
    String[] retArray3 =
        new String[] {
            "1,true,1.38,-0.33",
            "2,null,0.49,0.32",
            "5,null,-0.68,-0.25",
            "6,true,0.68,0.68",
            "8,true,0.84,null",
            "9,false,-0.50,-0.38",
            "10,true,-1.38,-0.08"
        };
    String[] retArray4 =
        new String[] {
            "1,true,-0.33",
            "2,null,0.32",
            "5,null,-0.25",
            "6,true,0.68",
            "9,false,-0.38",
            "10,true,-0.08"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s2, - s2, s4, + s4, s2 + s4 as t, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (t)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
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
                  + resultSet.getString("t")
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

      hasResultSet =
          statement.execute(
              "select s2 as t1, - s2, s4, + s4, s2 + s4 as t2, s2 - s4, s2 * s4, s2 / s4, s2 % s4 from root.test.sg1 without null all (t2, t1)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("t1")
                  + ","
                  + resultSet.getString("-root.test.sg1.s2")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("root.test.sg1.s4")
                  + ","
                  + resultSet.getString("t2")
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

      hasResultSet =
          statement.execute(
              "select s1, sin(s2) + cos(s2) as t1, cos(sin(s2 + s4) + s2) as t2 from root.test.sg1 without null all (t1, t2)");

      String[] columns = new String[]{"root.test.sg1.s1", "t1", "t2"};
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[1]).equals("null") ? "null" : new BigDecimal(resultSet.getString(columns[1])).setScale(2, RoundingMode.HALF_UP).toPlainString())
                  + ","
                  + (resultSet.getString(columns[2]).equals("null") ? "null" : new BigDecimal(resultSet.getString(columns[2])).setScale(2, RoundingMode.HALF_UP).toPlainString())
              ;
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select s1, cos(sin(s2 + s4) + s2) as t2 from root.test.sg1 without null all (t2)");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[2]).equals("null") ? "null" : new BigDecimal(resultSet.getString(columns[1])).setScale(2, RoundingMode.HALF_UP).toPlainString())
              ;
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
            "2,null,0.91,-0.42,-2.19",
            "3,false,null,null,null",
            "5,null,-0.96,0.28,-3.38",
            "6,true,-0.28,0.96,-0.29",
            "7,true,null,null,null",
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
            "8,true,0.84,null",
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
            "1,true,1.38,-0.33",
            "2,null,0.49,0.32",
            "3,false,null,null",
            "5,null,-0.68,-0.25",
            "6,true,0.68,0.68",
            "7,true,null,null",
            "8,true,0.84,null",
            "9,false,-0.50,-0.38",
            "10,true,-1.38,-0.08"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s1, sin(s2), cos(s2), tan(s2) from root.test.sg1 without null all(sin(s2), s1)");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("root.test.sg1.s1")
                  + ","
                  + (resultSet.getString("sin(root.test.sg1.s2)").equals("null") ? "null" : new BigDecimal(resultSet.getString("sin(root.test.sg1.s2)")).setScale(2, RoundingMode.HALF_UP).toPlainString())
                  + ","
                  + (resultSet.getString("cos(root.test.sg1.s2)").equals("null") ? "null" : new BigDecimal(resultSet.getString("cos(root.test.sg1.s2)")).setScale(2, RoundingMode.HALF_UP).toPlainString())
                  + ","
                  + (resultSet.getString("tan(root.test.sg1.s2)").equals("null") ? "null" : new BigDecimal(resultSet.getString("tan(root.test.sg1.s2)")).setScale(2, RoundingMode.HALF_UP).toPlainString());
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select s1, sin(s2) + cos(s2), cos(sin(s2 + s4) + s2) from root.test.sg1 without null all (sin(s2) + cos(s2), cos(sin(s2 + s4) + s2))");

      String[] columns = new String[]{"root.test.sg1.s1", "sin(root.test.sg1.s2) + cos(root.test.sg1.s2)", "cos(sin(root.test.sg1.s2 + root.test.sg1.s4) + root.test.sg1.s2)"};
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[1]).equals("null") ? "null" : new BigDecimal(resultSet.getString(columns[1])).setScale(2, RoundingMode.HALF_UP).toPlainString())
                  + ","
                  + (resultSet.getString(columns[2]).equals("null") ? "null" : new BigDecimal(resultSet.getString(columns[2])).setScale(2, RoundingMode.HALF_UP).toPlainString())
                  ;
          Assert.assertEquals(retArray2[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray2.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select s1, cos(sin(s2 + s4) + s2) from root.test.sg1 without null all (cos(sin(s2 + s4) + s2))");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + (resultSet.getString(columns[2]).equals("null") ? "null" : new BigDecimal(resultSet.getString(columns[1])).setScale(2, RoundingMode.HALF_UP).toPlainString())
                  ;
          Assert.assertEquals(retArray3[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray3.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select s1, sin(s2) + cos(s2), cos(sin(s2 + s4) + s2) from root.test.sg1 without null all");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(columns[0])
                  + ","
                  + resultSet.getString(columns[1])
                  + ","
                  + resultSet.getString(columns[2]);
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
  public void withGroupByTimeQueryTest() {
    // select avg(s4) as t, avg(s2) from root.test.sg1 group by ([1,10), 1ms) without null all(t, avg(s2))
    // select avg(s4) as t, sum(t3), avg(s2) from root.test.sg1 group by ([1,10), 1ms) without null all(t, avg(s2))
  }

  @Test
  public void withGroupByLevelQueryTest() {
    // select avg(s2), sum(s4) from root.test.** group by ([1, 10), 1ms), level = 2 without null all(avg(s2))
    // select avg(s2), sum(s4) from root.test.** group by ([1, 10), 1ms), level = 2 without null all(avg(s2), sum(s4))
  }

  @Test
  public void withoutNullColumnsIsFullPathQueryTest() {
    // select s2, s3 from root.test.** without null all(`root.test.sg1.s2`, `root.test.sg2.s3`)
    // select s2, s3 from root.test.sg1, root.test.sg2 without null all(`root.test.sg1.s2`)
    // select s2, s3 from root.test.sg1, root.test.sg2 without null all(`root.test.sg1.s2`, s3)
  }

  @Test
  public void withoutNullColumnsMisMatchSelectedQueryTest() {
    // select * from root.test.sg1 without null all(s1, usag)
    // select s1 + s2, s1 - s2, s1 * s2, s1 / s2, s1 % s2 from root.test.sg1 without null all (s1+s2, s2)
    // select s1 as d, sin(s1), cos(s1), tan(s1) as t, s2 from root.test.sg1 without null any(d,  tan(s1), t) limit 5
  }

  @Test
  public void alignByDeviceQueryTest() {
    // select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) align by device
  }

  @Test
  public void withLimitOffsetQueryTest() {
    // select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all limit 5 offset 3 align by device
    // select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) limit 5 offset 3 align by device
    // select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) limit 5 offset 2 align by device
    // select last_value(*) from root.sg1.* group by([1,10), 2ms) without null all(last_value(s2), last_value(s3)) limit 5 offset 2 slimit 1 align by device
    // select last_value(*) from root.sg1.* group by([1,10), 2ms) without null any(last_value(s1)) limit 5 offset 2 slimit 2 soffset 1 align by device
  }
}
