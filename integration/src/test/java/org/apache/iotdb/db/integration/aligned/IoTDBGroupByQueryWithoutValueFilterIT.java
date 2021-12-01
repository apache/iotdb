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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.crud.AggregationQueryOperator;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.IoTDBSQLException;

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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({LocalStandaloneTest.class})
public class IoTDBGroupByQueryWithoutValueFilterIT {

  protected static boolean enableSeqSpaceCompaction;
  protected static boolean enableUnseqSpaceCompaction;
  protected static boolean enableCrossSpaceCompaction;
  protected static long prevPartitionInterval;

  private static final String TIMESTAMP_STR = "Time";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    // TODO When the aligned time series support compaction, we need to set compaction to true
    enableSeqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction();
    enableUnseqSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction();
    enableCrossSpaceCompaction =
        IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction();
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(false);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(false);
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ConfigFactory.getConfig().setEnableSeqSpaceCompaction(enableSeqSpaceCompaction);
    ConfigFactory.getConfig().setEnableUnseqSpaceCompaction(enableUnseqSpaceCompaction);
    ConfigFactory.getConfig().setEnableCrossSpaceCompaction(enableCrossSpaceCompaction);
    ConfigFactory.getConfig().setPartitionInterval(prevPartitionInterval);
    EnvFactory.getEnv().cleanAfterClass();
  }

  @Test
  public void countSumAvgTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,4,40.0,7.5", "11,10,130142.0,13014.2", "21,1,null,230000.0", "31,0,355.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)");
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
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc");
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
          "1,0,null,null", "6,4,40.0,7.5", "11,5,130052.0,26010.4", "16,5,90.0,18.0",
          "21,1,null,230000.0", "26,0,null,null", "31,0,165.0,null", "36,0,73.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)");
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
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc");
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
  public void countSumAvgWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,0,null,null",
          "7,3,34.0,8.0",
          "13,4,130045.0,32511.25",
          "19,2,39.0,19.5",
          "25,0,null,null",
          "31,0,130.0,null",
          "37,0,154.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms)");
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
                  + " where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc");
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
          "7,3,34.0,8.0,4,34.0,8.5",
          "13,4,58.0,14.5,4,130045.0,14.5",
          "19,2,39.0,19.5,4,39.0,20.5",
          "25,0,null,null,4,null,26.5",
          "31,0,130.0,null,0,130.0,null",
          "37,0,154.0,null,0,154.0,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(d1.s1), sum(d2.s2), avg(d2.s1), count(d1.s3), sum(d1.s2), avg(d2.s3) "
                  + "from root.sg1 where time > 5 GROUP BY ([1, 41), 4ms, 6ms)");
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
                  + "from root.sg1 where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc");
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
          "1,10,6.0,10,6",
          "11,130000,11.0,20,11",
          "21,230000,230000.0,null,21",
          "31,null,null,40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)");
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
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc");
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
          "1,null,null,null,null", "6,10,6.0,10,6", "11,130000,11.0,15,11", "16,20,16.0,20,16",
          "21,230000,230000.0,null,21", "26,30,null,null,26", "31,null,null,35,null",
              "36,null,null,37,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)");
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
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc");
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
  public void maxMinValueTimeWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null,null,null",
          "7,10,7.0,10,7",
          "13,130000,14.0,16,13",
          "19,22,19.0,20,19",
          "25,28,null,null,25",
          "31,null,null,34,null",
          "37,null,null,40,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(s3), min_value(s1), max_time(s2), min_time(s3) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 4ms, 6ms)");
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
                  + " where time > 5 GROUP BY ([1, 41), 4ms, 6ms) order by time desc");
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
          "7,10,7.0,10,7,10,7.0,10,7",
          "13,16,14.0,16,13,130000,13.0,16,13",
          "19,22,19.0,20,19,22,19.0,20,19",
          "25,28,null,null,25,28,null,null,25",
          "31,null,null,34,null,null,null,34,null",
          "37,null,null,37,null,null,null,37,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select max_value(d2.s3), min_value(d1.s1), max_time(d2.s2), min_time(d1.s3), "
                  + "max_value(d1.s3), min_value(d2.s1), max_time(d1.s2), min_time(d2.s3) "
                  + "from root.sg1 where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)");
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
                  + "from root.sg1 where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms) order by time desc");
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
          "1,true,aligned_test7",
          "11,true,aligned_unseq_test13",
          "21,false,null",
          "31,null,aligned_test31"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 10ms)");
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
                  + " where time > 5 GROUP BY ([1, 41), 10ms) order by time desc");
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
          "1,null,null", "6,true,aligned_test7", "11,true,aligned_unseq_test13", "16,null,null",
          "21,true,null", "26,false,null", "31,null,aligned_test31", "36,null,aligned_test36"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 5ms)");
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
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 5ms) order by time desc");
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
  public void firstLastWithSlidingStepTest() throws SQLException {
    String[] retArray =
        new String[] {
          "1,null,null",
          "7,true,aligned_test7",
          "13,true,aligned_unseq_test13",
          "19,true,null",
          "25,false,null",
          "31,null,aligned_test31",
          "37,null,aligned_test37"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s4), first_value(s5) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)");
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
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms) order by time desc");
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
          "1,null,null,null,null",
          "7,non_aligned_test10,false,aligned_test10,false",
          "13,null,true,aligned_unseq_test13,null",
          "19,null,true,null,true",
          "25,null,true,null,true",
          "31,non_aligned_test34,null,aligned_test34,null",
          "37,non_aligned_test37,null,aligned_test37,null"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(d2.s5), first_value(d1.s4), last_value(d1.s5), first_value(d2.s4) "
                  + "from root.sg1 where time > 5 and time < 38 GROUP BY ([1, 41), 4ms, 6ms)");
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
                  + "from root.sg1 where time > 5 and time < 38 "
                  + "GROUP BY ([1, 41), 4ms, 6ms) order by time desc");
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

  @Test
  public void groupByWithWildcardTest1() throws SQLException {
    String[] retArray =
        new String[] {
          "1,9,9,8,8,9,9.0,10,10,true,aligned_test10",
          "11,10,10,10,1,1,20.0,20,20,true,aligned_unseq_test13",
          "21,1,0,10,10,0,230000.0,null,30,false,null",
          "31,0,10,0,0,10,null,40,null,null,aligned_test40"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(*), last_value(*) from root.sg1.d1 GROUP BY ([1, 41), 10ms)");
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
                  + resultSet.getString(count("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s5"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s2"))
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

      hasResultSet =
          statement.execute(
              "select count(*), last_value(*) from root.sg1.d1 "
                  + "GROUP BY ([1, 41), 10ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s2"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s3"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s4"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s5"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s2"))
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
          "1,0,0,0,0,0",
          "5,2,2,2,2,1",
          "9,2,3,3,2,2",
          "13,3,3,3,1,1",
          "17,3,3,3,0,0",
          "21,1,0,3,3,0",
          "25,0,0,3,3,0",
          "29,0,1,2,2,1",
          "33,0,3,0,0,3",
          "37,0,1,0,0,1"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(*) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms)");
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
                  + resultSet.getString(count("root.sg1.d1.s2"))
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

      hasResultSet =
          statement.execute(
              "select count(*) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s2"))
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
          "1,null,null,null,null,null",
          "5,7.0,7,7,false,aligned_test7",
          "9,11.0,11,11,true,aligned_test10",
          "13,15.0,15,15,true,aligned_unseq_test13",
          "17,19.0,19,19,null,null",
          "21,230000.0,null,230000,false,null",
          "25,null,null,27,false,null",
          "29,null,31,30,false,aligned_test31",
          "33,null,35,null,null,aligned_test35",
          "37,null,37,null,null,aligned_test37"
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(*) from root.sg1.d1 "
                  + "where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms)");
      Assert.assertTrue(hasResultSet);

      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s2"))
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

      hasResultSet =
          statement.execute(
              "select last_value(*) from root.sg1.d1 "
                  + " where time > 5 and time < 38 GROUP BY ([1, 41), 3ms, 4ms) order by time desc");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = retArray.length;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s1"))
                  + ","
                  + resultSet.getString(lastValue("root.sg1.d1.s2"))
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
  public void groupByWithoutAggregationFuncTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      statement.execute("select s1 from root.sg1.d1 group by ([0, 100), 5ms)");

      fail("No expected exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains(AggregationQueryOperator.ERROR_MESSAGE1));
    }
  }

  @Test
  public void negativeOrZeroTimeIntervalTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from  root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 0ms)");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof IoTDBSQLException);
    }

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from  root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), -1ms)");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof IoTDBSQLException);
    }
  }

  @Test
  public void slidingStepLessThanTimeIntervalTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s1), sum(s2), avg(s1) from  root.sg1.d1 "
                  + "where time > 5 GROUP BY ([1, 41), 5ms, 3ms)");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof IoTDBSQLException);
    }
  }
}
