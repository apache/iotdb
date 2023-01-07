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
import java.util.List;
import java.util.TimeZone;

import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
public class IoTDBGroupByNaturalMonthIT {

  private static final List<String> dataSet = new ArrayList<>();

  static {
    for (long i = 1604102400000L /*  2020-10-31 08:00:00 */;
        i <= 1617148800000L /* 2021-03-31 08:00:00 */;
        i += 86400_000L) {
      dataSet.add("insert into root.sg1.d1(timestamp, temperature) values (" + i + ", 1)");
    }
  }

  private static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");

  @BeforeClass
  public static void setUp() throws Exception {
    df.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    EnvFactory.getEnv().initClusterEnvironment();
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
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
            + "GROUP BY ([1604102400000, 1614556800000), 1mo, 1mo)",
        expectedHeader,
        retArray,
        df);
  }

  /**
   * Test when interval = 10 days < slidingStep = 1 month. StartTime: 2020-10-31 00:00:00, EndTime:
   * 2021-03-01 00:00:00
   */
  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
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
            + "GROUP BY ([1604102400000, 1614556800000), 10d, 1mo)",
        expectedHeader,
        retArray,
        df);
  }

  /**
   * Test when endTime - startTime = interval StartTime: 2020-10-31 00:00:00, EndTime: 2020-11-30
   * 00:00:00
   */
  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void groupByNaturalMonthTest3() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {"10/31/2020:00:00:00,30.0,"};
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([1604102400000, 1606694400000), 1mo)",
        expectedHeader,
        retArray,
        df);
  }

  /**
   * StartTime: 2021-01-31 00:00:00, EndTime: 2021-03-31 00:00:00. First Month with 28 days, Second
   * month with 31 days
   */
  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void groupByNaturalMonthTest4() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {"01/31/2021:00:00:00,28.0,", "02/28/2021:00:00:00,31.0,"};
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 GROUP BY ([1612051200000, 1617148800000), 1mo)",
        expectedHeader,
        retArray,
        df);
  }

  /** Test group by month with order by time desc. */
  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void groupByNaturalMonthFailTest() {
    assertTestFail(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([1612051200000, 1617148800000), 1mo) order by time desc",
        "doesn't support order by time desc now.");
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
  @Category({LocalStandaloneIT.class, ClusterIT.class})
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
            + "GROUP BY ([1604102400000, 1614556800000), 2mo, 1mo)",
        expectedHeader,
        retArray,
        df);
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void groupBySlingWindowNaturalMonth2() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, sum("root.sg1.d1.temperature")};
    String[] retArray = {
      "10/31/2020:00:00:00,30.0,",
      "11/10/2020:00:00:00,30.0,",
      "11/20/2020:00:00:00,30.0,",
      "11/30/2020:00:00:00,31.0,",
      "12/10/2020:00:00:00,31.0,",
      "12/20/2020:00:00:00,31.0,",
      "12/30/2020:00:00:00,31.0,",
      "01/09/2021:00:00:00,31.0,",
      "01/19/2021:00:00:00,31.0,",
      "01/29/2021:00:00:00,30.0,",
      "02/08/2021:00:00:00,21.0,",
      "02/18/2021:00:00:00,11.0,",
      "02/28/2021:00:00:00,1.0,"
    };
    resultSetEqualTest(
        "select sum(temperature) from root.sg1.d1 "
            + "GROUP BY ([1604102400000, 1614556800000), 1mo, 10d)",
        expectedHeader,
        retArray,
        df);
  }
}
