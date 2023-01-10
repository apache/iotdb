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
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.constant.TestConstant.minTime;
import static org.apache.iotdb.db.constant.TestConstant.minValue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByQueryWithoutValueFilterWithDeletionIT {

  private static final String TIMESTAMP_STR = "Time";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initClusterEnvironment();
    AlignedWriteUtil.insertData();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete from root.sg1.d1.s1 where time <= 15");
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s3 where time > 25");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void countSumAvgTest1() throws SQLException {
    String[] retArray = new String[] {"1,0,8.0", "11,5,13014.2", "21,1,46018.4", "31,0,null"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s3) from root.sg1.d1 "
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void countSumAvgTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null", "6,0,8.0", "11,0,26010.4", "16,5,18.0",
          "21,1,46018.4", "26,0,null", "31,0,null", "36,0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s3) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s3) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void countSumAvgWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null", "7,0,8.5", "13,1,32511.25", "19,2,20.5", "25,0,25.0", "31,0,null", "37,0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(s1), sum(s2), avg(s3) from root.sg1.d1 "
                  + " where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void maxMinValueTimeTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,10,null,6", "11,130000,16.0,11", "21,230000,230000.0,21", "31,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void maxMinValueTimeTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null",
          "6,10,null,6",
          "11,130000,null,11",
          "16,20,16.0,16",
          "21,230000,230000.0,21",
          "26,null,null,null",
          "31,null,null,null",
          "36,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void maxMinValueTimeWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null",
          "7,10,null,7",
          "13,130000,16.0,13",
          "19,22,19.0,19",
          "25,25,null,25",
          "31,null,null,null",
          "37,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + " where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void firstLastTest1() throws SQLException {
    String[] retArray = new String[] {"1,null,6", "11,20.0,11", "21,230000.0,21", "31,null,null"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s1), first_value(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s1), first_value(s3) from root.sg1.d1 "
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void firstLastTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null",
          "6,null,6",
          "11,null,11",
          "16,20.0,16",
          "21,230000.0,21",
          "26,null,null",
          "31,null,null",
          "36,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s1), first_value(s3) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s1), first_value(s3) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void firstLastWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null",
          "7,null,7",
          "13,16.0,130000",
          "19,20.0,19",
          "25,null,25",
          "31,null,null",
          "37,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s1), first_value(s3) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(s1), first_value(s3) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void groupByWithWildcardTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,8,8,9,null,10,true,aligned_test10",
          "11,5,10,1,1,20.0,20,true,aligned_unseq_test13",
          "21,1,5,10,0,230000.0,25,false,null",
          "31,0,0,0,10,null,null,null,aligned_test40"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*), last_value(*) from root.sg1.d1 GROUP BY ([1, 41), 10ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s5"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*), last_value(*) from root.sg1.d1 "
                  + "GROUP BY ([1, 41), 10ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s5"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void groupByWithWildcardTest2() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,0,0,0",
          "5,0,2,2,1",
          "9,0,3,2,2",
          "13,0,3,1,1",
          "17,3,3,0,0",
          "21,1,3,3,0",
          "25,0,1,3,0",
          "29,0,0,2,1",
          "33,0,0,0,3",
          "37,0,0,0,1"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select count(*) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void groupByWithWildcardTest3() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null",
          "5,null,7,false,aligned_test7",
          "9,null,11,true,aligned_test10",
          "13,null,15,true,aligned_unseq_test13",
          "17,19.0,19,null,null",
          "21,230000.0,230000,false,null",
          "25,null,25,false,null",
          "29,null,null,false,aligned_test31",
          "33,null,null,null,aligned_test35",
          "37,null,null,null,aligned_test37"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int cnt;
      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms)")) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last_value(*) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms) order by time desc")) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }
}
