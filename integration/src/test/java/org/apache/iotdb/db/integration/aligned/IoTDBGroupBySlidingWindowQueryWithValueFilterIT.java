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
package org.apache.iotdb.db.integration.aligned;

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.constant.TestConstant.maxTime;
import static org.apache.iotdb.db.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.constant.TestConstant.minTime;
import static org.apache.iotdb.db.constant.TestConstant.minValue;
import static org.apache.iotdb.db.constant.TestConstant.sum;

@Category({LocalStandaloneTest.class})
public class IoTDBGroupBySlidingWindowQueryWithValueFilterIT {

  private static final String TIMESTAMP_STR = "Time";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void countSumAvgTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,5,30.0,6006.0",
          "6,9,130082.0,14453.555555555555",
          "11,10,130142.0,13014.2",
          "16,6,90.0,38348.333333333336",
          "21,1,null,230000.0",
          "26,0,null,null",
          "31,0,null,null",
          "36,0,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where s1 > 5 and time < 35 GROUP BY ([1, 41), 10ms, 5ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + " where s1 > 5 and time < 35 GROUP BY ([1, 41), 10ms, 5ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
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
          "1,1,null,30000.0",
          "4,3,21.0,7.0",
          "7,4,45.0,8.75",
          "10,4,130047.0,32509.25",
          "13,5,130062.0,26012.4",
          "16,5,90.0,18.0",
          "19,3,39.0,76679.66666666667",
          "22,1,null,230000.0",
          "25,0,null,null",
          "28,0,null,null",
          "31,0,null,null",
          "34,0,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where s3 > 5 and time < 30 GROUP BY ([1, 36), 5ms, 3ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + " where s3 > 5 and time < 30 GROUP BY ([1, 36), 5ms, 3ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void countSumAvgTest3() throws SQLException {
    String[] retArray =
        new String[] {
          "1,1,null,30000.0",
          "3,2,6.0,15003.0",
          "5,3,21.0,7.0",
          "7,3,34.0,8.0",
          "9,3,42.0,10.666666666666666",
          "11,4,130037.0,32509.25",
          "13,4,130045.0,32511.25",
          "15,4,66.0,16.5",
          "17,4,74.0,18.5",
          "19,2,39.0,19.5",
          "21,1,null,230000.0",
          "23,1,null,230000.0",
          "25,0,null,null",
          "27,0,null,null",
          "29,0,null,null",
          "31,0,null,null",
          "33,0,null,null",
          "35,0,null,null",
          "37,0,null,null",
          "39,0,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where s3 > 5 GROUP BY ([1, 41), 4ms, 2ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + " where s3 > 5 GROUP BY ([1, 41), 4ms, 2ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d1.s1"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void countSumAvgWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null,null,0,null,null",
          "3,2,13.0,6.5,2,13.0,6.5",
          "5,4,30.0,7.5,4,30.0,7.5",
          "7,4,45.0,8.75,5,45.0,9.0",
          "9,3,42.0,10.666666666666666,4,42.0,10.5",
          "11,4,52.0,13.0,4,52.0,13.0",
          "13,4,62.0,15.5,4,62.0,15.5",
          "15,5,85.0,17.0,5,85.0,17.0",
          "17,4,74.0,18.5,5,74.0,19.0",
          "19,2,39.0,19.5,4,39.0,20.5",
          "21,0,null,null,3,null,22.333333333333332",
          "23,0,null,null,1,null,24.0",
          "25,0,null,null,0,null,null",
          "27,0,null,null,0,null,null",
          "29,0,null,null,0,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(d1.s1), sum(d2.s2), avg(d2.s1), count(d1.s3), sum(d1.s2), avg(d2.s3) "
                  + "from root.sg1 where d2.s3 > 5 and d1.s3 < 25 GROUP BY ([1, 31), 5ms, 2ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d2.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d2.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(d1.s1), sum(d2.s2), avg(d2.s1), count(d1.s3), sum(d1.s2), avg(d2.s3) "
                  + "from root.sg1 where d2.s3 > 5 and d1.s3 < 25 GROUP BY ([1, 31), 5ms, 2ms) "
                  + "order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d2.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d2.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(sum("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(avg("root.sg1.d2.s3"));
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
          "1,30000,6.0,9,3",
          "6,130000,6.0,15,6",
          "11,130000,11.0,20,11",
          "16,230000,16.0,20,16",
          "21,230000,230000.0,null,23",
          "26,null,null,null,null",
          "31,null,null,null,null",
          "36,null,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where s1 > 5 and time < 35 GROUP BY ([1, 41), 10ms, 5ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + " where s1 > 5 and time < 35 GROUP BY ([1, 41), 10ms, 5ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
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
          "1,30000,30000.0,null,3",
          "4,8,6.0,8,6",
          "7,11,7.0,11,7",
          "10,130000,11.0,14,10",
          "13,130000,14.0,17,13",
          "16,20,16.0,20,16",
          "19,230000,19.0,20,19",
          "22,230000,230000.0,null,22",
          "25,29,null,null,25",
          "28,29,null,null,28"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where s3 > 5 and time < 30 GROUP BY ([1, 31), 5ms, 3ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + " where s3 > 5 and time < 30 GROUP BY ([1, 31), 5ms, 3ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
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
  public void maxMinValueTimeTest3() throws SQLException {
    String[] retArray =
        new String[] {
          "1,30000,30000.0,null,3",
          "3,30000,6.0,6,3",
          "5,8,6.0,8,6",
          "7,10,7.0,10,7",
          "9,12,9.0,12,9",
          "11,130000,11.0,14,11",
          "13,130000,14.0,16,13",
          "15,18,15.0,18,15",
          "17,20,17.0,20,17",
          "19,22,19.0,20,19",
          "21,230000,230000.0,null,21",
          "23,230000,230000.0,null,23",
          "25,28,null,null,25",
          "27,29,null,null,27",
          "29,29,null,null,29",
          "31,null,null,null,null",
          "33,null,null,null,null",
          "35,null,null,null,null",
          "37,null,null,null,null",
          "39,null,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where s3 > 5 and time < 30 GROUP BY ([1, 41), 4ms, 2ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + " where s3 > 5 and time < 30 GROUP BY ([1, 41), 4ms, 2ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
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
  public void maxMinValueTimeWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null,null,null,null,null",
          "3,7,6.0,7,6,7,6.0,7,6",
          "5,9,6.0,9,6,9,6.0,9,6",
          "7,11,7.0,11,7,11,7.0,11,7",
          "9,12,9.0,12,9,12,9.0,12,9",
          "11,15,11.0,15,11,15,11.0,15,11",
          "13,17,14.0,17,14,17,14.0,17,14",
          "15,19,15.0,19,15,19,15.0,19,15",
          "17,21,17.0,20,17,21,17.0,20,17",
          "19,22,19.0,20,19,22,19.0,20,19",
          "21,24,null,null,21,24,null,null,21",
          "23,24,null,null,24,24,null,null,24",
          "25,null,null,null,null,null,null,null,null",
          "27,null,null,null,null,null,null,null,null",
          "29,null,null,null,null,null,null,null,null",
          "31,null,null,null,null,null,null,null,null",
          "33,null,null,null,null,null,null,null,null",
          "35,null,null,null,null,null,null,null,null",
          "37,null,null,null,null,null,null,null,null",
          "39,null,null,null,null,null,null,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(d2.s3), min_value(d1.s1), max_time(d2.s2), min_time(d1.s3), "
                  + "max_value(d1.s3), min_value(d2.s1), max_time(d1.s2), min_time(d2.s3) "
                  + "from root.sg1 where d2.s3 > 5 and d1.s3 < 25 GROUP BY ([1, 41), 5ms, 2ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d2.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d2.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d2.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d2.s3"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select max_value(d2.s3), min_value(d1.s1), max_time(d2.s2), min_time(d1.s3), "
                  + "max_value(d1.s3), min_value(d2.s1), max_time(d1.s2), min_time(d2.s3) "
                  + "from root.sg1 where d2.s3 > 5 and d1.s3 < 25 GROUP BY ([1, 41), 5ms, 2ms) "
                  + " order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d2.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d2.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(maxValue("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(minValue("root.sg1.d2.s1"))
                  + ","
                  + resultSet.getString(maxTime("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(minTime("root.sg1.d2.s3"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void firstLastTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,true,aligned_test1",
          "6,true,aligned_test10",
          "11,true,aligned_unseq_test13",
          "16,true,null",
          "21,true,null",
          "26,null,null",
          "31,null,null",
          "36,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where s4 = true GROUP BY ([1, 41), 10ms, 5ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + " where s4 = true GROUP BY ([1, 41), 10ms, 5ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s5"));
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
          "4,false,aligned_test7",
          "7,false,aligned_test7",
          "10,null,null",
          "13,null,null",
          "16,null,null",
          "19,false,null",
          "22,false,null",
          "25,false,null",
          "28,false,null",
          "31,null,null",
          "34,null,null",
          "37,null,null",
          "40,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where s4 = false GROUP BY ([1, 41), 5ms, 3ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + " where s4 = false GROUP BY ([1, 41), 5ms, 3ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void firstLastTest3() throws SQLException {
    String[] retArray =
        new String[] {
          "1,true,aligned_test1",
          "3,true,aligned_unseq_test3",
          "5,true,aligned_test5",
          "7,true,aligned_test10",
          "9,true,aligned_test10",
          "11,true,aligned_unseq_test13",
          "13,true,aligned_unseq_test13",
          "15,null,null",
          "17,null,null",
          "19,true,null",
          "21,true,null",
          "23,true,null",
          "25,true,null",
          "27,null,null",
          "29,null,null",
          "31,null,null",
          "33,null,null",
          "35,null,null",
          "37,null,null",
          "39,null,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where s4 != false GROUP BY ([1, 41), 4ms, 2ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + " where s4 != false GROUP BY ([1, 41), 4ms, 2ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s5"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }

  @Test
  public void firstLastWithNonAlignedTimeseriesTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,non_aligned_test5,true,aligned_test5,true",
          "3,non_aligned_test7,true,aligned_test7,false",
          "5,non_aligned_test9,true,aligned_test9,true",
          "7,non_aligned_test10,false,aligned_test10,false",
          "9,non_aligned_test10,false,aligned_unseq_test13,false",
          "11,null,true,aligned_unseq_test13,null",
          "13,null,true,aligned_unseq_test13,null",
          "15,null,null,null,null",
          "17,null,null,null,null",
          "19,null,null,null,null",
          "21,null,null,null,null",
          "23,null,null,null,null",
          "25,null,null,null,null",
          "27,non_aligned_test31,null,aligned_test31,null",
          "29,non_aligned_test33,null,aligned_test33,null",
          "31,non_aligned_test35,null,aligned_test35,null",
          "33,non_aligned_test37,null,aligned_test37,null",
          "35,non_aligned_test39,null,aligned_test39,null",
          "37,non_aligned_test40,null,aligned_test40,null",
          "39,non_aligned_test40,null,aligned_test40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(d2.s5), first_value(d1.s4), last_value(d1.s5), first_value(d2.s4) "
                  + "from root.sg1 where d1.s5 like 'aligned_unseq_test%' or d2.s5 like 'non_aligned_test%' "
                  + "GROUP BY ([1, 41), 5ms, 2ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d2.s5"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s5"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d2.s4"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray.length, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(d2.s5), first_value(d1.s4), last_value(d1.s5), first_value(d2.s4) "
                  + "from root.sg1 where d1.s5 like 'aligned_unseq_test%' or d2.s5 like 'non_aligned_test%' "
                  + "GROUP BY ([1, 41), 5ms, 2ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d2.s5"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s5"))
                  + ","
                  + resultSet.getString(firstValue("root.sg1.d2.s4"));
          Assert.assertEquals(retArray[cnt - 1], ans);
          cnt--;
        }
        Assert.assertEquals(0, cnt);
      }
    }
  }
}
