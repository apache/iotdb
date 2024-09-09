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

package org.apache.iotdb.db.it.aggregation;

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
import java.sql.Statement;

import static org.apache.iotdb.db.utils.constant.TestConstant.avg;
import static org.apache.iotdb.db.utils.constant.TestConstant.count;
import static org.apache.iotdb.db.utils.constant.TestConstant.extreme;
import static org.apache.iotdb.db.utils.constant.TestConstant.firstValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.lastValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxTime;
import static org.apache.iotdb.db.utils.constant.TestConstant.maxValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.minTime;
import static org.apache.iotdb.db.utils.constant.TestConstant.minValue;
import static org.apache.iotdb.db.utils.constant.TestConstant.sum;
import static org.junit.Assert.fail;

/** Multiple aggregation with filter test. */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAggregationSmallDataIT {

  private static final String TIMESTAMP_STR = "Time";
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private final String d0s4 = "root.vehicle.d0.s4";
  private final String d1s0 = "root.vehicle.d1.s0";
  private final String d1s1 = "root.vehicle.d1.s1";

  private static String[] sqls =
      new String[] {
        "CREATE DATABASE root.vehicle",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,199)",
        "DELETE FROM root.vehicle.d0.s0 WHERE time < 104",
        "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
        "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
        "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
        "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
        "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
        "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
        "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
        "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
        "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertSQL();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void countWithTimeFilterTest() {
    String[] retArray = new String[] {"3,7,4,5,1"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s0),count(s1),count(s2),count(s3),count(s4) "
                  + "FROM root.vehicle.d0 WHERE time >= 3 AND time <= 106")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3))
                  + ","
                  + resultSet.getString(count(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void countWithoutFilterTest() {
    String[] retArray = new String[] {"4,0,6,1"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // select count(d0.s0),count(d1.s1),count(d0.s3),count(d0.s4) from root.vehicle
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(d0.s0),count(d1.s1),count(d0.s3),count(d0.s4) FROM root.vehicle")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d1s1))
                  + ","
                  + resultSet.getString(count(d0s3))
                  + ","
                  + resultSet.getString(count(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void maxValueWithoutFilterTest() {
    String[] retArray = new String[] {"22222,null"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try {
        statement.executeQuery(
            "SELECT max_value(d0.s0),max_value(d1.s1),max_value(d0.s3) FROM root.vehicle");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.toString()
                .contains(
                    "Aggregate functions [MIN_VALUE, MAX_VALUE] only support data types [INT32, INT64, FLOAT, DOUBLE, STRING, DATE, TIMESTAMP]"));
      }

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT max_value(d0.s0),max_value(d1.s1) FROM root.vehicle")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(maxValue(d0s0)) + "," + resultSet.getString(maxValue(d1s1));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void extremeWithoutFilterTest() {
    String[] retArray = new String[] {"22222,null"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try {
        statement.executeQuery(
            "SELECT extreme(d0.s0),extreme(d1.s1),extreme(d0.s3) FROM root.vehicle");
        fail();
      } catch (Exception e) {
        Assert.assertTrue(
            e.toString()
                .contains(
                    "Aggregate functions [AVG, SUM, EXTREME, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP] only support numeric data types [INT32, INT64, FLOAT, DOUBLE]"));
      }

      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT extreme(d0.s0),extreme(d1.s1) FROM root.vehicle")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(extreme(d0s0)) + "," + resultSet.getString(extreme(d1s1));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void firstValueWithoutFilterTest() {
    String[] retArray = new String[] {"90,null,aaaaa"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // select first_value(d0.s0),first_value(d1.s1),first_value(d0.s3) from root.vehicle
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT first_value(d0.s0),first_value(d1.s1),first_value(d0.s3) FROM root.vehicle")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(firstValue(d0s0))
                  + ","
                  + resultSet.getString(firstValue(d1s1))
                  + ","
                  + resultSet.getString(firstValue(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void lastValueWithoutFilterTest() {
    String[] retArray = new String[] {"22222,null,good"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // select last_value(d0.s0),last_value(d1.s1),last_value(d0.s3) from root.vehicle
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(d0.s0),last_value(d1.s1),last_value(d0.s3) FROM root.vehicle")) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(lastValue(d0s0))
                  + ","
                  + resultSet.getString(lastValue(d1s1))
                  + ","
                  + resultSet.getString(lastValue(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void sumWithoutFilterTest() {
    String[] retArray = new String[] {"22610.0,null"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // select sum(d0.s0),sum(d1.s1),sum(d0.s3) from root.vehicle
      int cnt = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT sum(d0.s0),sum(d1.s1) FROM root.vehicle")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(sum(d0s0)) + "," + resultSet.getString(sum(d1s1));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void lastValueWithSingleValueFilterTest() {
    String[] retArray = new String[] {"22222,55555"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT last_value(s0),last_value(s1) FROM root.vehicle.d0 WHERE s2 >= 3.33")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(lastValue(d0s0)) + "," + resultSet.getString(lastValue(d0s1));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void firstValueWithSingleValueFilterTest() {
    String[] retArray = new String[] {"99,180"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT first_value(s0),first_value(s1) FROM root.vehicle.d0 WHERE s2 >= 3.33")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(firstValue(d0s0)) + "," + resultSet.getString(firstValue(d0s1));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void sumWithSingleValueFilterTest() {
    String[] retArray = new String[] {"22321.0,55934.0,1029"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT sum(s0),sum(s1),sum(s2) FROM root.vehicle.d0 WHERE s2 >= 3.33")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(sum(d0s0))
                  + ","
                  + resultSet.getString(sum(d0s1))
                  + ","
                  + Math.round(resultSet.getDouble(sum(d0s2)));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void avgWithSingleValueFilterTest() {
    String[] retArray = new String[] {"11160.5,18645,206"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT avg(s0),avg(s1),avg(s2) FROM root.vehicle.d0 WHERE s2 >= 3.33")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(avg(d0s0))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s1)))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s2)));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void countWithSingleValueFilterTest() {
    String[] retArray = new String[] {"2,3,5,1,0"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s0),count(s1),count(s2),count(s3),"
                  + "count(s4) FROM root.vehicle.d0 WHERE s2 >= 3.33")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3))
                  + ","
                  + resultSet.getString(count(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void minTimeWithMultiValueFiltersTest() {
    String[] retArray = new String[] {"104,1,2,101,100"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT min_time(s0),min_time(s1),min_time(s2)"
                  + ",min_time(s3),min_time(s4) FROM root.vehicle.d0 WHERE s1 < 50000 AND s1 != 100")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(minTime(d0s0))
                  + ","
                  + resultSet.getString(minTime(d0s1))
                  + ","
                  + resultSet.getString(minTime(d0s2))
                  + ","
                  + resultSet.getString(minTime(d0s3))
                  + ","
                  + resultSet.getString(minTime(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
          Assert.assertEquals(1, cnt);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void maxTimeWithMultiValueFiltersTest() {
    String[] retArray = new String[] {"105,105,105,102,100"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_time(s0),max_time(s1),max_time(s2)"
                  + ",max_time(s3),max_time(s4) FROM root.vehicle.d0 WHERE s1 < 50000 AND s1 != 100")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(maxTime(d0s0))
                  + ","
                  + resultSet.getString(maxTime(d0s1))
                  + ","
                  + resultSet.getString(maxTime(d0s2))
                  + ","
                  + resultSet.getString(maxTime(d0s3))
                  + ","
                  + resultSet.getString(maxTime(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void minValueWithMultiValueFiltersTest() {
    String[] retArray = new String[] {"90,180,2.22"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT min_value(s0),min_value(s1),min_value(s2) FROM root.vehicle.d0 "
                  + "WHERE s1 < 50000 AND s1 != 100")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(minValue(d0s0))
                  + ","
                  + resultSet.getString(minValue(d0s1))
                  + ","
                  + resultSet.getString(minValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void maxValueWithMultiValueFiltersTest() {
    String[] retArray = new String[] {"99,40000,11.11"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT max_value(s0),max_value(s1),max_value(s2) FROM root.vehicle.d0 "
                  + "WHERE s1 < 50000 AND s1 != 100")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(maxValue(d0s0))
                  + ","
                  + resultSet.getString(maxValue(d0s1))
                  + ","
                  + resultSet.getString(maxValue(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void extremeWithMultiValueFiltersTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"99,40000,11.11"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT extreme(s0),extreme(s1),extreme(s2)"
                  + " FROM root.vehicle.d0 "
                  + "WHERE s1 < 50000 AND s1 != 100")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(extreme(d0s0))
                  + ","
                  + resultSet.getString(extreme(d0s1))
                  + ","
                  + resultSet.getString(extreme(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
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
  public void selectAllSQLTest() {
    String[] retArray =
        new String[] {
          "1,null,1101,null,null,999",
          "2,null,40000,2.22,null,null",
          "3,null,null,3.33,null,null",
          "4,null,null,4.44,null,null",
          "50,null,50000,null,null,null",
          "60,null,null,null,aaaaa,null",
          "70,null,null,null,bbbbb,null",
          "80,null,null,null,ccccc,null",
          "100,null,199,null,null,null",
          "101,null,199,null,ddddd,null",
          "102,null,180,10.0,fffff,null",
          "103,null,199,null,null,null",
          "104,90,190,null,null,null",
          "105,99,199,11.11,null,null",
          "106,199,null,null,null,null",
          "1000,22222,55555,1000.11,null,888",
          "946684800000,null,100,null,good,null"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.**")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(d0s0)
                  + ","
                  + resultSet.getString(d0s1)
                  + ","
                  + resultSet.getString(d0s2)
                  + ","
                  + resultSet.getString(d0s3)
                  + ","
                  + resultSet.getString(d1s0);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(17, cnt);
      }

      retArray = new String[] {"100,true"};
      try (ResultSet resultSet = statement.executeQuery("SELECT s4 FROM root.vehicle.d0")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s4);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void insertSQL() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
