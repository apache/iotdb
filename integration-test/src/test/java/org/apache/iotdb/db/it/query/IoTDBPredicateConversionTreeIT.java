/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPredicateConversionTreeIT {

  private static final String DATABASE_NAME = "root.test_pred";
  private static final String DEVICE_ID = DATABASE_NAME + ".d1";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "CREATE TIMESERIES " + DEVICE_ID + ".int32_col WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES " + DEVICE_ID + ".int64_col WITH DATATYPE=INT64, ENCODING=PLAIN",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(1, 20, 20)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(2, 29, 29)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(3, 30, 30)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(4, 31, 31)",
        "INSERT INTO " + DEVICE_ID + "(time, int64_col) VALUES(5, 35)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col) VALUES(6, 35)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(7, null, null)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(8, 1000, 1000)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(9, -29, -29)",
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(10, -30, -30)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();

    importData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void importData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testDoubleLiteralAndInt32Column() {
    // Common header definition for Tree Model
    String expectedHeader = "Time," + DEVICE_ID + ".int32_col,";

    // ----------------------------------------------------------------------
    // Group 1: Positive boundary 29.1
    // ----------------------------------------------------------------------

    // TC-1: int32_col >= 29.1
    // Logic: Ceil(29.1) = 30 -> int32_col >= 30
    // Expected result: Row 3(30), Row 4(31), Row 6(35), Row 8(1000)
    String[] retArrayGtEqPositive = {"3,30,", "4,31,", "6,35,", "8,1000,"};
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col >= 29.1",
        expectedHeader,
        retArrayGtEqPositive);

    // TC-2: int32_col > 29.1
    // Logic: Floor(29.1) = 29 -> int32_col > 29 -> equivalent to int32_col >= 30
    // Expected result: Same as above
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col > 29.1",
        expectedHeader,
        retArrayGtEqPositive);

    // TC-3: int32_col <= 29.1
    // Logic: Floor(29.1) = 29 -> int32_col <= 29
    // Expected result: Row 1(20), Row 2(29), Row 9(-29), Row 10(-30)
    String[] retArrayLtEqPositive = {"1,20,", "2,29,", "9,-29,", "10,-30,"};
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col <= 29.1",
        expectedHeader,
        retArrayLtEqPositive);

    // TC-4: int32_col < 29.1
    // Logic: Ceil(29.1) = 30 -> int32_col < 30 -> equivalent to int32_col <= 29
    // Expected result: Same as above
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col < 29.1",
        expectedHeader,
        retArrayLtEqPositive);

    // TC-5: int32_col = 29.1
    // Logic: Integer values can never equal a decimal fraction -> Empty result
    String[] retArrayEmpty = {};
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col = 29.1",
        expectedHeader,
        retArrayEmpty);

    // TC-6: int32_col != 29.1
    // Logic: Integer values are never equal to a decimal fraction -> Return all non-null rows
    // Expected result: All rows with int32 data (Row 5 and Row 7 are null, excluded)
    String[] retArrayNotEq = {
      "1,20,", "2,29,", "3,30,", "4,31,", "6,35,", "8,1000,", "9,-29,", "10,-30,"
    };
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col != 29.1",
        expectedHeader,
        retArrayNotEq);
  }

  @Test
  public void testDoubleLiteralAndInt64Column() {
    // Common header definition for Tree Model
    String expectedHeader = "Time," + DEVICE_ID + ".int64_col,";

    // ----------------------------------------------------------------------
    // Group 1: Positive boundary 29.1
    // ----------------------------------------------------------------------

    // TC-1: int64_col >= 29.1
    // Logic: Ceil(29.1) = 30 -> int64_col >= 30
    // Expected result: Row 3(30), Row 4(31), Row 5(35), Row 8(1000)
    // Note: Row 5 (35) is included here (it is int64). Row 6 is excluded (it is int32).
    String[] retArrayGtEqPositive = {"3,30,", "4,31,", "5,35,", "8,1000,"};
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col >= 29.1",
        expectedHeader,
        retArrayGtEqPositive);

    // TC-2: int64_col > 29.1
    // Logic: Floor(29.1) = 29 -> int64_col > 29 -> equivalent to int64_col >= 30
    // Expected result: Same as above
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col > 29.1",
        expectedHeader,
        retArrayGtEqPositive);

    // TC-3: int64_col <= 29.1
    // Logic: Floor(29.1) = 29 -> int64_col <= 29
    // Expected result: Row 1(20), Row 2(29), Row 9(-29), Row 10(-30)
    String[] retArrayLtEqPositive = {"1,20,", "2,29,", "9,-29,", "10,-30,"};
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col <= 29.1",
        expectedHeader,
        retArrayLtEqPositive);

    // TC-4: int64_col < 29.1
    // Logic: Ceil(29.1) = 30 -> int64_col < 30 -> equivalent to int64_col <= 29
    // Expected result: Same as above
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col < 29.1",
        expectedHeader,
        retArrayLtEqPositive);

    // TC-5: int64_col = 29.1
    // Logic: Long values can never equal a decimal fraction -> Empty result
    String[] retArrayEmpty = {};
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col = 29.1",
        expectedHeader,
        retArrayEmpty);

    // TC-6: int64_col != 29.1
    // Logic: Long values are never equal to a decimal fraction -> Return all non-null rows
    // Expected result: All rows with int64 data (Row 6 and Row 7 are null, excluded)
    String[] retArrayNotEq = {
      "1,20,", "2,29,", "3,30,", "4,31,", "5,35,", "8,1000,", "9,-29,", "10,-30,"
    };
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col != 29.1",
        expectedHeader,
        retArrayNotEq);

    // ----------------------------------------------------------------------
    // Group 2: Negative boundary -29.1
    // ----------------------------------------------------------------------

    // TC-7: int64_col <= -29.1
    // Logic: Floor(-29.1) = -30 -> int64_col <= -30
    // Expected result: Row 10(-30)
    String[] retArrayLtEqNegative = {"10,-30,"};
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col <= -29.1",
        expectedHeader,
        retArrayLtEqNegative);

    // TC-8: int64_col >= -29.1
    // Logic: Ceil(-29.1) = -29 -> int64_col >= -29
    // Expected result: Row 9(-29) and all larger positive numbers (20, 29, 30, 31, 35, 1000)
    String[] retArrayGtEqNegative = {
      "1,20,", "2,29,", "3,30,", "4,31,", "5,35,", "8,1000,", "9,-29,"
    };
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col >= -29.1",
        expectedHeader,
        retArrayGtEqNegative);
  }

  @Test
  public void testDoubleLiteralOverflow() {
    String expectedHeaderInt32 = "Time," + DEVICE_ID + ".int32_col,";
    String expectedHeaderInt64 = "Time," + DEVICE_ID + ".int64_col,";
    String[] emptyResult = {};

    // ----------------------------------------------------------------------
    // Part 1: INT32 Column vs Double > Integer.MAX_VALUE (~2.14E9)
    // Literal: 3.0E9
    // ----------------------------------------------------------------------

    // TC-1: int32 >= 3.0E9
    // Logic: Impossible for any INT32 value.
    // Expected: Empty result.
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col >= 3.0E9",
        expectedHeaderInt32,
        emptyResult);

    // TC-2: int32 <= 3.0E9
    // Logic: Always True for valid values. Must exclude NULLs.
    // Expected: All rows where int32_col is not null (Row 5 and 7 are null).
    String[] allInt32Rows = {
      "1,20,", "2,29,", "3,30,", "4,31,", "6,35,", "8,1000,", "9,-29,", "10,-30,"
    };
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE int32_col <= 3.0E9",
        expectedHeaderInt32,
        allInt32Rows);

    // ----------------------------------------------------------------------
    // Part 2: INT64 Column vs Double > Long.MAX_VALUE (~9.22E18)
    // Literal: 1.0E19
    // ----------------------------------------------------------------------

    // TC-3: int64 >= 1.0E19
    // Logic: Impossible for any INT64 value.
    // Expected: Empty result.
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col >= 1.0E19",
        expectedHeaderInt64,
        emptyResult);

    // TC-4: int64 <= 1.0E19
    // Logic: Always True for valid values. Must exclude NULLs.
    // Expected: All rows where int64_col is not null (Row 6 and 7 are null).
    String[] allInt64Rows = {
      "1,20,", "2,29,", "3,30,", "4,31,", "5,35,", "8,1000,", "9,-29,", "10,-30,"
    };
    resultSetEqualTest(
        "SELECT int64_col FROM " + DEVICE_ID + " WHERE int64_col <= 1.0E19",
        expectedHeaderInt64,
        allInt64Rows);
  }
}
