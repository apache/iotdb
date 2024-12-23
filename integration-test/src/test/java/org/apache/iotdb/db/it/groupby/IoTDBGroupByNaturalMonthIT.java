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
package org.apache.iotdb.db.it.groupby;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.utils.constant.TestConstant.sum;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBGroupByNaturalMonthIT {

  protected static final List<String> dataSet = new ArrayList<>();
  protected static TimeUnit currPrecision;

  static {
    for (long i = 1604102400000L /*  2020-10-31 00:00:00 */;
        i <= 1617148800000L /* 2021-03-31 00:00:00 */;
        i += 86400_000L) {
      dataSet.add("insert into root.sg1.d1(timestamp, temperature) values (" + i + ", 1)");
    }

    // TimeRange: [2023-01-01 00:00:00, 2027-01-01 00:00:00]
    // insert a record each first day of month
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeZone(TimeZone.getTimeZone("+00:00"));
    calendar.setTimeInMillis(1672531200000L);
    for (long i = calendar.getTimeInMillis();
        i <= 1798761600000L;
        calendar.add(Calendar.MONTH, 1), i = calendar.getTimeInMillis()) {
      dataSet.add("insert into root.test.d1(timestamp, s1) values (" + i + ", 1)");
    }

    dataSet.add("insert into root.testTimeZone.d1(timestamp, s1) values (1, 1)");
  }

  protected static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");

  @BeforeClass
  public static void setUp() throws Exception {
    df.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    EnvFactory.getEnv().initClusterEnvironment();
    currPrecision = EnvFactory.getEnv().getConfig().getCommonConfig().getTimestampPrecision();
    prepareData(dataSet.toArray(new String[0]));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * Test when interval = slidingStep = 1 month. StartTime: 2020-10-31 00:00:00, EndTime: 2021-03-01
   * 00:00:00
   */
  @Test
  public void groupByNaturalMonthTest1() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray =
        new String[] {
          "10/31/2020:00:00:00,30.0,",
          "11/30/2020:00:00:00,31.0,",
          "12/31/2020:00:00:00,31.0,",
          "01/31/2021:00:00:00,28.0,",
          "02/28/2021:00:00:00,1.0,"
        };
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([2020-10-31, 2021-03-01), 1mo, 1mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  /**
   * Test when interval = 10 days < slidingStep = 1 month. StartTime: 2020-10-31 00:00:00, EndTime:
   * 2021-03-01 00:00:00
   */
  @Test
  public void groupByNaturalMonthTest2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {
      "10/31/2020:00:00:00,10.0,",
      "11/30/2020:00:00:00,10.0,",
      "12/31/2020:00:00:00,10.0,",
      "01/31/2021:00:00:00,10.0,",
      "02/28/2021:00:00:00,1.0,"
    };
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([2020-10-31, 2021-03-01), 10d, 1mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  /**
   * Test when endTime - startTime = interval StartTime: 2020-10-31 00:00:00, EndTime: 2020-11-30
   * 00:00:00
   */
  @Test
  public void groupByNaturalMonthTest3() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {"10/31/2020:00:00:00,30.0,"};
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 " + "GROUP BY ([2020-10-31, 2020-11-30), 1mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  /**
   * StartTime: 2021-01-31 00:00:00, EndTime: 2021-03-31 00:00:00. First Month with 28 days, Second
   * month with 31 days
   */
  @Test
  public void groupByNaturalMonthTest4() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {"01/31/2021:00:00:00,28.0,", "02/28/2021:00:00:00,31.0,"};
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 GROUP BY ([2021-01-31, 2021-03-31), 1mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalMonthTest5() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {
      "01/30/2021:00:00:00,29.0,", "02/28/2021:00:00:00,30.0,", "03/30/2021:00:00:00,1.0,"
    };
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 GROUP BY ([2021-01-30, 2021-03-31), 1mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  /** Test group by month with order by time desc. */
  @Test
  public void groupByNaturalMonthFailTest() {
    assertTestFail(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([2021-01-31, 2021-03-31), 1mo) order by time desc",
        "doesn't support order by time desc now.");

    assertTestFail(
        "select sum(temperature) from root.sg1.d1 GROUP BY ([1970-01-01, 2970-01-01), 40d, 1mo)",
        "The time windows may exceed 10000, please ensure your input.");
  }

  /** StartTime: now() - 1mo, EndTime: now(). */
  @Test
  @Ignore // TODO add it back after we can query with no DataRegion
  @Category(LocalStandaloneIT.class) // datasets are inconsistent in cluster
  public void groupByNaturalMonthWithNowTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      connection.setClientInfo("time_zone", "+00:00");

      int cnt = 0;
      List<String> times = new ArrayList<>();
      try (ResultSet resultSet =
          statement.executeQuery(
              "select sum(temperature) from root.sg1.d1 GROUP BY ([now() - 1mo, now()), 1d)")) {
        while (resultSet.next()) {
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          times.add(resultSet.getString("Time"));
          if (ans == null) {
            cnt++;
          }
        }
        Assert.assertTrue(cnt >= 28);
        Assert.assertTrue(cnt <= 31);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupBySlingWindowNaturalMonth1() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {
      "10/31/2020:00:00:00,61.0,",
      "11/30/2020:00:00:00,62.0,",
      "12/31/2020:00:00:00,59.0,",
      "01/31/2021:00:00:00,29.0,",
      "02/28/2021:00:00:00,1.0,"
    };
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([2020-10-31, 2021-03-01), 2mo, 1mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupBySlingWindowNaturalMonth2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {
      "10/31/2020:00:00:00,30.0,",
      "11/10/2020:00:00:00,30.0,",
      "11/20/2020:00:00:00,30.0,",
      "11/30/2020:00:00:00,30.0,",
      "12/10/2020:00:00:00,30.0,",
      "12/20/2020:00:00:00,30.0,",
      "12/30/2020:00:00:00,30.0,",
      "01/09/2021:00:00:00,30.0,",
      "01/19/2021:00:00:00,30.0,",
      "01/29/2021:00:00:00,30.0,",
      "02/08/2021:00:00:00,21.0,",
      "02/18/2021:00:00:00,11.0,",
      "02/28/2021:00:00:00,1.0,"
    };
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([2020-10-31, 2021-03-01), 1mo, 10d)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalYearTest1() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          "01/01/2023:00:00:00,12,",
          "01/01/2024:00:00:00,12,",
          "01/01/2025:00:00:00,12,",
          "01/01/2026:00:00:00,12,"
        };
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-01, 2027-01-01), 1y)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalYearTest2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          "01/01/2024:00:00:00,12,",
          "01/01/2025:00:00:00,12,",
          "01/01/2026:00:00:00,12,",
          "01/01/2027:00:00:00,12,"
        };
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ((2023-01-01, 2027-01-01], 1y)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalYearWithSlidingWindowTest1() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          "01/01/2023:00:00:00,12,",
          "07/01/2023:00:00:00,12,",
          "01/01/2024:00:00:00,12,",
          "07/01/2024:00:00:00,12,",
          "01/01/2025:00:00:00,12,",
          "07/01/2025:00:00:00,12,",
          "01/01/2026:00:00:00,12,",
          "07/01/2026:00:00:00,6,"
        };
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-01, 2027-01-01), 1y, 6mo)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalYearWithSlidingWindowTest2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          "01/01/2023:00:00:00,24,",
          "01/01/2024:00:00:00,24,",
          "01/01/2025:00:00:00,24,",
          "01/01/2026:00:00:00,12,"
        };
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-01, 2027-01-01), 2y, 1y)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalYearWithSlidingWindowTest3() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          "01/01/2023:00:00:00,6,",
          "01/01/2024:00:00:00,6,",
          "01/01/2025:00:00:00,6,",
          "01/01/2026:00:00:00,6,"
        };
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-01, 2027-01-01), 6mo, 1y)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalMonthWithMixedUnit1() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          // [01-28, 03-01)
          "01/28/2023:00:00:00,1,",
          // [03-01, 04-02)
          "03/01/2023:00:00:00,1,",
          // [04-02, 05-03)
          "03/30/2023:00:00:00,1,",
          // [05-03, 05-29)
          "05/01/2023:00:00:00,1,"
        };
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-28, 2023-05-29), 1mo1d)",
        expectedHeader,
        retArray,
        df,
        currPrecision);
  }

  @Test
  public void groupByNaturalMonthWithMixedUnit2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, count("root.test.d1.s1")};
    String[] retArray =
        new String[] {
          // [01-28, 03-01)
          "1674864000000,1,",
          // [03-01, 03-30)
          "1677628800000,1,",
          // [03-30, 05-01)
          "1680134400000,1,",
          // [05-01, 05-29)
          "1682899200000,1,"
        };
    // the part in timeDuration finer than current time precision will be discarded
    resultSetEqualTest(
        "select count(s1) from root.test.d1 " + "group by ([2023-01-28, 2023-05-29), 1mo1d1ns)",
        expectedHeader,
        retArray,
        null,
        currPrecision);
  }

  @Test
  public void groupByNaturalMonthWithNonSystemDefaultTimeZone() {
    try (ISession session =
        EnvFactory.getEnv().getSessionConnection(TimeZone.getTimeZone("UTC+09:00").toZoneId())) {

      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select count(s1) from root.testTimeZone.d1 group by([2024-07-01, 2024-08-01), 1mo)");

      int count = 0;
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        count++;
      }
      assertEquals(1, count);

      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
