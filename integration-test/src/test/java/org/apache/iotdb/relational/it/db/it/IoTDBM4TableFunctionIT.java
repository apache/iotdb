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

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBM4TableFunctionIT {

  private static final String DATABASE_NAME = "test";

  private static final String[] SQLS =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE vehicle(device_id STRING TAG, speed DOUBLE FIELD, status STRING FIELD)",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.001+08:00, 'car_01', 5.0, 'OK')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.003+08:00, 'car_01', 15.0, 'OK')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.006+08:00, 'car_01', 30.0, 'WARN')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.009+08:00, 'car_01', 10.0, 'OK')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.020+08:00, 'car_01', 40.0, 'CRIT')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.002+08:00, 'car_02', 8.0, 'OK')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.005+08:00, 'car_02', 25.0, 'WARN')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.008+08:00, 'car_02', 12.0, 'OK')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.011+08:00, 'car_02', 18.0, 'OK')",
        "INSERT INTO vehicle(time, device_id, speed, status) VALUES (1970-01-01T08:00:00.015+08:00, 'car_02', 6.0, 'WARN')",
        "FLUSH"
      };

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockLineNumber(2);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLS);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testTimeWindowMode() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "m4_time", "m4_value", "device_id", "status"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.001Z,5.0,car_01,OK,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.006Z,30.0,car_01,WARN,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.009Z,10.0,car_01,OK,",
          "1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.030Z,1970-01-01T00:00:00.020Z,40.0,car_01,CRIT,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.002Z,8.0,car_02,OK,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.005Z,25.0,car_02,WARN,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.008Z,12.0,car_02,OK,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.011Z,18.0,car_02,OK,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.015Z,6.0,car_02,WARN,"
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, m4_time, m4_value, device_id, status "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 10ms)) "
            + "ORDER BY device_id, window_start, m4_time",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    expectedHeader =
        new String[] {
          "window_start",
          "window_end",
          "m4_time",
          "m4_value",
          "time",
          "device_id",
          "speed",
          "status"
        };
    tableResultSetEqualTest(
        "SELECT * "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 10ms)) "
            + "ORDER BY device_id, window_start, m4_time",
        expectedHeader,
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.001Z,5.0,1970-01-01T00:00:00.001Z,car_01,5.0,OK,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.006Z,30.0,1970-01-01T00:00:00.006Z,car_01,30.0,WARN,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.009Z,10.0,1970-01-01T00:00:00.009Z,car_01,10.0,OK,",
          "1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.030Z,1970-01-01T00:00:00.020Z,40.0,1970-01-01T00:00:00.020Z,car_01,40.0,CRIT,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.002Z,8.0,1970-01-01T00:00:00.002Z,car_02,8.0,OK,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.005Z,25.0,1970-01-01T00:00:00.005Z,car_02,25.0,WARN,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.008Z,12.0,1970-01-01T00:00:00.008Z,car_02,12.0,OK,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.011Z,18.0,1970-01-01T00:00:00.011Z,car_02,18.0,OK,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.015Z,6.0,1970-01-01T00:00:00.015Z,car_02,6.0,WARN,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testTimeWindowModeByPosition() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "m4_time", "m4_value", "device_id", "status"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.001Z,5.0,car_01,OK,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.006Z,30.0,car_01,WARN,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.009Z,10.0,car_01,OK,",
          "1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.030Z,1970-01-01T00:00:00.020Z,40.0,car_01,CRIT,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.002Z,8.0,car_02,OK,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.005Z,25.0,car_02,WARN,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.008Z,12.0,car_02,OK,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.011Z,18.0,car_02,OK,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.015Z,6.0,car_02,WARN,"
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, m4_time, m4_value, device_id, status "
            + "FROM TABLE(M4(TABLE(vehicle) PARTITION BY device_id ORDER BY time, 'time', 'speed', 10ms)) "
            + "ORDER BY device_id, window_start, m4_time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testGroupBy() {
    String[] expectedHeader = new String[] {"device_id", "point_count"};
    String[] retArray = new String[] {"car_01,4,", "car_02,5,"};
    tableResultSetEqualTest(
        "SELECT device_id, COUNT(*) AS point_count "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 10ms)) "
            + "GROUP BY device_id ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testTimeWindowWithGap() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "m4_time", "m4_value", "device_id"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.005Z,1970-01-01T00:00:00.001Z,5.0,car_01,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.005Z,1970-01-01T00:00:00.003Z,15.0,car_01,",
          "1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.025Z,1970-01-01T00:00:00.020Z,40.0,car_01,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.005Z,1970-01-01T00:00:00.002Z,8.0,car_02,",
          "1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.015Z,1970-01-01T00:00:00.011Z,18.0,car_02,"
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 5ms, SLIDE => 10ms)) "
            + "ORDER BY device_id, window_start, m4_time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDisplayWindowRange() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "m4_time", "m4_value", "device_id"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.001Z,5.0,car_01,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.006Z,30.0,car_01,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.009Z,10.0,car_01,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.002Z,8.0,car_02,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.005Z,25.0,car_02,",
          "1970-01-01T00:00:00.000Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.008Z,12.0,car_02,"
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 10ms, "
            + "DISPLAYBEGIN => 1970-01-01T08:00:00.000+08:00, "
            + "DISPLAYEND => 1970-01-01T08:00:00.010+08:00)) "
            + "ORDER BY device_id, window_start, m4_time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testCountWindowMode() {
    String[] expectedHeader =
        new String[] {"window_start", "window_end", "m4_time", "m4_value", "device_id"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.004Z,1970-01-01T00:00:00.001Z,5.0,car_01,",
          "1970-01-01T00:00:00.001Z,1970-01-01T00:00:00.004Z,1970-01-01T00:00:00.003Z,15.0,car_01,",
          "1970-01-01T00:00:00.006Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.006Z,30.0,car_01,",
          "1970-01-01T00:00:00.006Z,1970-01-01T00:00:00.010Z,1970-01-01T00:00:00.009Z,10.0,car_01,",
          "1970-01-01T00:00:00.020Z,1970-01-01T00:00:00.021Z,1970-01-01T00:00:00.020Z,40.0,car_01,",
          "1970-01-01T00:00:00.002Z,1970-01-01T00:00:00.006Z,1970-01-01T00:00:00.002Z,8.0,car_02,",
          "1970-01-01T00:00:00.002Z,1970-01-01T00:00:00.006Z,1970-01-01T00:00:00.005Z,25.0,car_02,",
          "1970-01-01T00:00:00.008Z,1970-01-01T00:00:00.012Z,1970-01-01T00:00:00.008Z,12.0,car_02,",
          "1970-01-01T00:00:00.008Z,1970-01-01T00:00:00.012Z,1970-01-01T00:00:00.011Z,18.0,car_02,",
          "1970-01-01T00:00:00.015Z,1970-01-01T00:00:00.016Z,1970-01-01T00:00:00.015Z,6.0,car_02,"
        };
    tableResultSetEqualTest(
        "SELECT window_start, window_end, m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 2)) "
            + "ORDER BY device_id, window_start, m4_time",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testIllegalValueType() {
    tableAssertTestFail(
        "SELECT m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'status', SIZE => 10ms))",
        "701: The type of the column [status] is not as expected.",
        DATABASE_NAME);
  }

  @Test
  public void testValueColumnNotFound() {
    tableAssertTestFail(
        "SELECT m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'fake_speed', SIZE => 10ms))",
        "701: Required column [fake_speed] not found in the source table argument.",
        DATABASE_NAME);
  }

  @Test
  public void testMissingSize() {
    tableAssertTestFail(
        "SELECT m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id ORDER BY time, "
            + "TIMECOL => 'time', VALUECOL => 'speed'))",
        "701: Missing required argument: SIZE",
        DATABASE_NAME);
  }

  @Test
  public void testMissingOrderBy() {
    tableAssertTestFail(
        "SELECT m4_time, m4_value, device_id "
            + "FROM TABLE(M4(DATA => TABLE(vehicle) PARTITION BY device_id, "
            + "TIMECOL => 'time', VALUECOL => 'speed', SIZE => 10ms))",
        "701: Table argument with set semantics requires an ORDER BY clause.",
        DATABASE_NAME);
  }
}
