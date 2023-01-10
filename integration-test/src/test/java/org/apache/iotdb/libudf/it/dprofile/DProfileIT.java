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

package org.apache.iotdb.libudf.it.dprofile;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class DProfileIT {
  protected static final int ITERATION_TIMES = 10_000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database root.vehicle");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s1 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s2 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    double x = -100d, y = 100d; // borders of random value
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 1; i <= ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2) values(%d,%d,%d)",
                i * 1000,
                (int) Math.floor(x + Math.random() * y % (y - x + 1)),
                (int) Math.floor(x + Math.random() * y % (y - x + 1))));
        statement.execute(
            (String.format(
                "insert into root.vehicle.d2(timestamp,s1,s2) values(%d,%f,%f)",
                i * 1000,
                x + Math.random() * y % (y - x + 1),
                x + Math.random() * y % (y - x + 1))));
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function distinct as 'org.apache.iotdb.library.dprofile.UDTFDistinct'");
      statement.execute(
          "create function histogram as 'org.apache.iotdb.library.dprofile.UDTFHistogram'");
      statement.execute(
          "create function integral as 'org.apache.iotdb.library.dprofile.UDAFIntegral'");
      statement.execute(
          "create function integralavg as 'org.apache.iotdb.library.dprofile.UDAFIntegralAvg'");
      statement.execute("create function mad as 'org.apache.iotdb.library.dprofile.UDAFMad'");
      statement.execute("create function median as 'org.apache.iotdb.library.dprofile.UDAFMedian'");
      statement.execute("create function mode as 'org.apache.iotdb.library.dprofile.UDAFMode'");
      statement.execute(
          "create function percentile as 'org.apache.iotdb.library.dprofile.UDAFPercentile'");
      statement.execute("create function period as 'org.apache.iotdb.library.dprofile.UDAFPeriod'");
      statement.execute("create function qlb as 'org.apache.iotdb.library.dprofile.UDTFQLB'");
      statement.execute(
          "create function resample as 'org.apache.iotdb.library.dprofile.UDTFResample'");
      statement.execute("create function sample as 'org.apache.iotdb.library.dprofile.UDTFSample'");
      statement.execute(
          "create function segment as 'org.apache.iotdb.library.dprofile.UDTFSegment'");
      statement.execute("create function skew as 'org.apache.iotdb.library.dprofile.UDAFSkew'");
      statement.execute("create function spread as 'org.apache.iotdb.library.dprofile.UDAFSpread'");
      statement.execute("create function stddev as 'org.apache.iotdb.library.dprofile.UDAFStddev'");
      statement.execute("create function minmax as 'org.apache.iotdb.library.dprofile.UDTFMinMax'");
      statement.execute("create function zscore as 'org.apache.iotdb.library.dprofile.UDTFZScore'");
      statement.execute("create function spline as 'org.apache.iotdb.library.dprofile.UDTFSpline'");
      statement.execute("create function mvavg as 'org.apache.iotdb.library.dprofile.UDTFMvAvg'");
      statement.execute("create function acf as 'org.apache.iotdb.library.dprofile.UDTFACF'");
      statement.execute("create function pacf as 'org.apache.iotdb.library.dprofile.UDTFPACF'");
      statement.execute(
          "create function consistency as 'org.apache.iotdb.library.dquality.UDTFConsistency'");
      statement.execute(
          "create function timeliness as 'org.apache.iotdb.library.dquality.UDTFTimeliness'");
      statement.execute(
          "create function completeness as 'org.apache.iotdb.library.dquality.UDTFCompleteness'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testIntegral1() {
    String sqlStr = "select integral(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIntegral2() {
    String sqlStr = "select completeness(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIntegralAvg1() {
    String sqlStr = "select integralavg(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIntegralAvg2() {
    String sqlStr = "select integralavg(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testMad1() {
    String sqlStr = "select mad(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMad2() {
    String sqlStr = "select timeliness(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMedian1() {
    String sqlStr = "select median(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMedian2() {
    String sqlStr = "select median(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMode1() {
    String sqlStr = "select consistency(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPercentile1() {
    String sqlStr = "select percentile(d1.s2,'rank'='0.7') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPercentile2() {
    String sqlStr = "select percentile(d2.s2,'rank'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPeriod1() {
    String sqlStr = "select period(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSkew1() {
    String sqlStr = "select skew(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSkew2() {
    String sqlStr = "select skew(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSpread1() {
    String sqlStr = "select spread(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSpread2() {
    String sqlStr = "select spread(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSddev1() {
    String sqlStr = "select stddev(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testStddev2() {
    String sqlStr = "select stddev(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      Object result = resultSet.getObject(2);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testACF1() {
    String sqlStr = "select acf(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDistinct1() {
    String sqlStr = "select distinct(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testHistogram1() {
    String sqlStr =
        String.format(
            "select histogram(d1.s1,'min'='%f','max'='%f','count'='20') from root.vehicle",
            -100d, 100d);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMinMax1() {
    String sqlStr =
        String.format("select minmax(d2.s2,'min'='%f','max'='%f') from root.vehicle", -100d, 100d);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMvAvg1() {
    String sqlStr = "select mvavg(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMvAvg2() {
    String sqlStr = "select mvavg(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testPACF1() {
    String sqlStr = "select pacf(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testQLB1() {
    String sqlStr = "select qlb(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testResample1() {
    String sqlStr = "select resample(d2.s1, 'every'='5s') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testResample2() {
    String sqlStr = "select resample(d2.s2, 'every'='10s', 'aggr'='median') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testSample1() {
    String sqlStr = "select resample(d2.s1, 'method'='reservoir','k'='5') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testsample2() {
    String sqlStr = "select resample(d1.s2, 'method'='isometric','k'='5') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testsample3() {
    String sqlStr = "select sample(d1.s2, 'method'='triangle','k'='5') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSegment1() {
    String sqlStr = "select segment(d2.s2,'error'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testSpline1() {
    String sqlStr = "select spline(d2.s1, 'points'='100') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testZScore1() {
    String sqlStr = "select zscore(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testZScore2() {
    String sqlStr = "select zscore(d2.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
