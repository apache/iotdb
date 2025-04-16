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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBWindowTVFIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table bid (stock_id string tag, price double field, s1 double field)",
        "insert into bid values (2021-01-01T09:05:00, 'AAPL', 100.0, 101)",
        "insert into bid values (2021-01-01T09:07:00, 'AAPL', 103.0, 101)",
        "insert into bid values (2021-01-01T09:09:00, 'AAPL', 102.0, 101)",
        "insert into bid values (2021-01-01T09:06:00, 'TESL', 200.0, 102)",
        "insert into bid values (2021-01-01T09:07:00, 'TESL', 202.0, 202)",
        "insert into bid values (2021-01-01T09:15:00, 'TESL', 195.0, 332)",
        "FLUSH",
        "CLEAR ATTRIBUTE CACHE",
      };

  private static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail("insertData failed.");
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testHopFunction() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "time", "stock_id", "price", "s1"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "2021-01-01T09:10:00.000Z,2021-01-01T09:20:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
          "2021-01-01T09:15:00.000Z,2021-01-01T09:25:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM HOP(DATA => bid, TIMECOL => 'time', SLIDE => 5m, SIZE => 10m) ORDER BY stock_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,AAPL,305.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,AAPL,305.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,TESL,402.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:15:00.000Z,TESL,402.0,",
          "2021-01-01T09:10:00.000Z,2021-01-01T09:20:00.000Z,TESL,195.0,",
          "2021-01-01T09:15:00.000Z,2021-01-01T09:25:00.000Z,TESL,195.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM HOP(DATA => bid, TIMECOL => 'time', SLIDE => 5m, SIZE => 10m) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T10:00:00.000Z,AAPL,305.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T10:00:00.000Z,TESL,597.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM HOP(DATA => bid, TIMECOL => 'time', SLIDE => 1h, SIZE => 1h) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testSessionFunction() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "time", "stock_id", "price", "s1"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,2021-01-01T09:09:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:09:00.000Z,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "2021-01-01T09:05:00.000Z,2021-01-01T09:09:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "2021-01-01T09:06:00.000Z,2021-01-01T09:07:00.000Z,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "2021-01-01T09:06:00.000Z,2021-01-01T09:07:00.000Z,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "2021-01-01T09:15:00.000Z,2021-01-01T09:15:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM SESSION(DATA => bid PARTITION BY stock_id ORDER BY time, TIMECOL => 'time', GAP => 2m) ORDER BY stock_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,2021-01-01T09:09:00.000Z,AAPL,305.0,",
          "2021-01-01T09:06:00.000Z,2021-01-01T09:07:00.000Z,TESL,402.0,",
          "2021-01-01T09:15:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM SESSION(DATA => bid PARTITION BY stock_id ORDER BY time, TIMECOL => 'time', GAP => 2m) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testVariationFunction() {
    String[] expectedHeader = new String[] {"window_index", "time", "stock_id", "price", "s1"};
    String[] retArray =
        new String[] {
          "0,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "1,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "1,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "0,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "0,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "1,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM VARIATION(DATA => bid PARTITION BY stock_id ORDER BY time, COL => 'price', DELTA => 2.0) ORDER BY stock_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    expectedHeader = new String[] {"start_time", "end_time", "stock_id", "avg"};
    retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,",
          "2021-01-01T09:07:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.5,",
          "2021-01-01T09:06:00.000Z,2021-01-01T09:07:00.000Z,TESL,201.0,",
          "2021-01-01T09:15:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,",
        };
    tableResultSetEqualTest(
        "SELECT first(time) as start_time, last(time) as end_time, stock_id, avg(price) as avg FROM VARIATION(DATA => bid PARTITION BY stock_id ORDER BY time, COL => 'price', DELTA => 2.0) GROUP BY window_index, stock_id ORDER BY stock_id, window_index",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testCapacityFunction() {
    String[] expectedHeader = new String[] {"window_index", "time", "stock_id", "price", "s1"};
    String[] retArray =
        new String[] {
          "0,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "0,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "1,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "0,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "0,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "1,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM CAPACITY(DATA => bid PARTITION BY stock_id ORDER BY time, SIZE => 2) ORDER BY stock_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    expectedHeader = new String[] {"start_time", "end_time", "stock_id", "avg"};
    retArray =
        new String[] {
          "2021-01-01T09:05:00.000Z,2021-01-01T09:07:00.000Z,AAPL,101.5,",
          "2021-01-01T09:09:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.0,",
          "2021-01-01T09:06:00.000Z,2021-01-01T09:07:00.000Z,TESL,201.0,",
          "2021-01-01T09:15:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,",
        };
    tableResultSetEqualTest(
        "SELECT first(time) as start_time, last(time) as end_time, stock_id, avg(price) as avg FROM CAPACITY(DATA => bid PARTITION BY stock_id ORDER BY time, SIZE => 2) GROUP BY window_index, stock_id ORDER BY stock_id, window_index",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testTumbleFunction() {
    // TUMBLE (10m)
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "time", "stock_id", "price", "s1"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "2021-01-01T09:10:00.000Z,2021-01-01T09:20:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM TUMBLE(DATA => bid, TIMECOL => 'time', SIZE => 10m) ORDER BY stock_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // TUMBLE (10m) + GROUP BY
    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,AAPL,305.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:10:00.000Z,TESL,402.0,",
          "2021-01-01T09:10:00.000Z,2021-01-01T09:20:00.000Z,TESL,195.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM TUMBLE(DATA => bid, TIMECOL => 'time', SIZE => 10m) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // TUMBLE (1h) + GROUP BY
    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T10:00:00.000Z,AAPL,305.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T10:00:00.000Z,TESL,597.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM TUMBLE(DATA => bid, TIMECOL => 'time', SIZE => 1h) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testCumulateFunction() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "time", "stock_id", "price", "s1"};
    String[] retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T09:06:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,2021-01-01T09:05:00.000Z,AAPL,100.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,2021-01-01T09:07:00.000Z,AAPL,103.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,2021-01-01T09:09:00.000Z,AAPL,102.0,101.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,2021-01-01T09:06:00.000Z,TESL,200.0,102.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,2021-01-01T09:07:00.000Z,TESL,202.0,202.0,",
          "2021-01-01T09:12:00.000Z,2021-01-01T09:18:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
          "2021-01-01T09:12:00.000Z,2021-01-01T09:24:00.000Z,2021-01-01T09:15:00.000Z,TESL,195.0,332.0,",
        };
    tableResultSetEqualTest(
        "SELECT * FROM CUMULATE(DATA => bid, TIMECOL => 'time', STEP => 6m, SIZE => 12m) ORDER BY stock_id, time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T09:06:00.000Z,AAPL,100.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,AAPL,305.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T09:12:00.000Z,TESL,402.0,",
          "2021-01-01T09:12:00.000Z,2021-01-01T09:18:00.000Z,TESL,195.0,",
          "2021-01-01T09:12:00.000Z,2021-01-01T09:24:00.000Z,TESL,195.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM CUMULATE(DATA => bid, TIMECOL => 'time', STEP => 6m, SIZE => 12m) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader = new String[] {"window_start", "window_end", "stock_id", "sum"};
    retArray =
        new String[] {
          "2021-01-01T09:00:00.000Z,2021-01-01T10:00:00.000Z,AAPL,305.0,",
          "2021-01-01T09:00:00.000Z,2021-01-01T10:00:00.000Z,TESL,597.0,",
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, stock_id, sum(price) as sum FROM CUMULATE(DATA => bid, TIMECOL => 'time', STEP => 1h, SIZE => 1h) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // test UDFException
    String errMsg = "Cumulative table function requires size must be an integral multiple of step.";
    tableAssertTestFail(
            "SELECT window_start, window_end, stock_id, sum(price) as sum FROM CUMULATE(DATA => bid, TIMECOL => 'time', STEP => 4m, SIZE => 10m) GROUP BY window_start, window_end, stock_id ORDER BY stock_id, window_start",
            errMsg,
            DATABASE_NAME);
  }
}
