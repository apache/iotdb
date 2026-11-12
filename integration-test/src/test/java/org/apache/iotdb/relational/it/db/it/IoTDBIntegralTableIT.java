/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
public class IoTDBIntegralTableIT {

  private static final String DATABASE = "test";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE,
        "USE " + DATABASE,
        "CREATE TABLE integral_table(device_id STRING TAG, x DOUBLE FIELD, y DOUBLE FIELD)",
        "INSERT INTO integral_table(time, device_id, x, y) VALUES "
            + "(0, 'd1', 1.0, 100.0), (1000, 'd1', 3.0, 104.0), "
            + "(2000, 'd1', 2.0, 102.0), (3000, 'd1', 4.0, 108.0), "
            + "(0, 'd2', 10.0, 200.0), (1000, 'd2', 12.0, 204.0), "
            + "(2000, 'd2', 11.0, 202.0), (3000, 'd2', 13.0, 208.0)",
        "CREATE TABLE integral_sparse(device_id STRING TAG, v DOUBLE FIELD)",
        // device a skips the null point at time 1s, device b has a single point and device n only
        // contains negative values
        "INSERT INTO integral_sparse(time, device_id, v) VALUES "
            + "(0, 'a', 1.0), (1000, 'a', null), (2000, 'a', 2.0), (3000, 'a', 4.0), "
            + "(0, 'b', 5.0), "
            + "(0, 'n', -1.0), (1000, 'n', -3.0)",
        "CREATE TABLE integral_invalid(device_id STRING TAG, s STRING FIELD)",
        "INSERT INTO integral_invalid(time, device_id, s) VALUES (0, 'd1', 'a'), (1000, 'd1', 'b')",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testIntegralBasicAndMultiGroup() {
    String[] expectedHeader =
        new String[] {"device_id", "area", "area_default", "area_with_unit", "area_timecol"};
    String[] retArray =
        new String[] {
          // d1.x: (1+3)/2 + (3+2)/2 + (2+4)/2 = 7.5; unit 10s makes the result 1/10 of unit 1s
          "d1,7.5,7.5,0.75,7.5,",
          // d2.x: (10+12)/2 + (12+11)/2 + (11+13)/2 = 34.5
          "d2,34.5,34.5,3.45,34.5,"
        };
    // Groups are integrated independently; the unit defaults to 1s when omitted, and the third
    // argument accepts an explicit time column name.
    tableResultSetEqualTest(
        "SELECT device_id, integral(x, '1s') AS area, integral(x) AS area_default, "
            + "integral(x, '10s') AS area_with_unit, integral(x, '1s', time) AS area_timecol "
            + "FROM integral_table GROUP BY device_id ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testIntegralWithTimeWindow() {
    String[] expectedHeader = new String[] {"device_id", "window_start", "area"};
    String[] retArray =
        new String[] {
          // window [0s, 2s) only accumulates (0s, 1.0) and (1s, 3.0)
          "d1,1970-01-01T00:00:00.000Z,2.0,",
          "d1,1970-01-01T00:00:02.000Z,3.0,",
          "d2,1970-01-01T00:00:00.000Z,11.0,",
          "d2,1970-01-01T00:00:02.000Z,12.0,"
        };
    // Each (device, window) group is integrated independently: no interpolation at the window
    // boundary and no accumulation across windows.
    tableResultSetEqualTest(
        "SELECT device_id, date_bin(2s, time) AS window_start, integral(x, '1s') AS area "
            + "FROM integral_table GROUP BY device_id, date_bin(2s, time) "
            + "ORDER BY device_id, window_start",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testIntegralSkipsNullAndHandlesInsufficientPoints() {
    String[] expectedHeader = new String[] {"device_id", "area"};
    String[] retArray =
        new String[] {
          // the null point is skipped, so the remaining points are (0s,1),(2s,2),(3s,4)
          "a,6.0,",
          // fewer than two effective points have no time span, so the area is 0
          "b,0.0,",
          // negative values are allowed and the result may be negative
          "n,-2.0,"
        };
    tableResultSetEqualTest(
        "SELECT device_id, integral(v, '1s') AS area FROM integral_sparse "
            + "GROUP BY device_id ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testIntegralWithColumnRegex() {
    String[] expectedHeader = new String[] {"device_id", "_col1_x", "_col2_y"};
    String[] retArray = new String[] {"d1,7.5,310.0,", "d2,34.5,610.0,"};
    // COLUMNS('^[xy]$') is expanded into one integral per matched numeric column.
    tableResultSetEqualTest(
        "SELECT device_id, integral(COLUMNS('^[xy]$'), '1s') FROM integral_table "
            + "GROUP BY device_id ORDER BY device_id",
        expectedHeader,
        retArray,
        DATABASE);
  }

  @Test
  public void testIntegralInvalidArguments() {
    tableAssertTestFail(
        "SELECT integral(s, '1s') FROM integral_invalid",
        "only support numeric data types",
        DATABASE);
    tableAssertTestFail(
        "SELECT integral(x, 'abc') FROM integral_table",
        "should be a positive duration string",
        DATABASE);
    tableAssertTestFail(
        "SELECT integral(x, '0s') FROM integral_table",
        "should be a positive duration string",
        DATABASE);
    tableAssertTestFail(
        "SELECT integral(x, '1s', device_id) FROM integral_table",
        "must be actual time name",
        DATABASE);
  }
}
