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

package org.apache.iotdb.relational.it.query.recent;

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
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBPredicateConversionTableIT {

  private static final String DATABASE_NAME = "test_predicate_conversion";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE PredicateTestTable(" + "int32_col INT32, " + "int64_col INT64" + ")",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(1, 20, 20)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(2, 29, 29)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(3, 30, 30)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(4, 31, 31)",
        "INSERT INTO PredicateTestTable(time, int64_col) VALUES(5, 35)",
        "INSERT INTO PredicateTestTable(time, int32_col) VALUES(6, 35)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(7, null, null)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(8, 1000, 1000)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(9, -29, -29)",
        "INSERT INTO PredicateTestTable(time, int32_col, int64_col) VALUES(10, -30, -30)"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDoubleLiteralAndInt32Column() {
    // Common header definition
    String[] expectedHeader = {"time", "int32_col"};

    // ----------------------------------------------------------------------
    // Group 1: Positive boundary 29.1
    // ----------------------------------------------------------------------

    // TC-1: int32_col >= 29.1
    // Logic: Ceil(29.1) = 30 -> int32_col >= 30
    // Expected result: Row 3(30), Row 4(31), Row 6(35), Row 8(1000)
    String[] retArrayGtEqPositive = {
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.006Z,35,",
      "1970-01-01T00:00:00.008Z,1000,"
    };
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col >= 29.1 ORDER BY time",
        expectedHeader,
        retArrayGtEqPositive,
        DATABASE_NAME);

    // TC-2: int32_col > 29.1
    // Logic: Floor(29.1) = 29 -> int32_col > 29 -> equivalent to int32_col >= 30
    // Expected result: Same as above
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col > 29.1 ORDER BY time",
        expectedHeader,
        retArrayGtEqPositive,
        DATABASE_NAME);

    // TC-3: int32_col <= 29.1
    // Logic: Floor(29.1) = 29 -> int32_col <= 29
    // Expected result: Row 1(20), Row 2(29), Row 9(-29), Row 10(-30)
    String[] retArrayLtEqPositive = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.009Z,-29,",
      "1970-01-01T00:00:00.010Z,-30,"
    };
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col <= 29.1 ORDER BY time",
        expectedHeader,
        retArrayLtEqPositive,
        DATABASE_NAME);

    // TC-4: int32_col < 29.1
    // Logic: Ceil(29.1) = 30 -> int32_col < 30 -> equivalent to int32_col <= 29
    // Expected result: Same as above
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col < 29.1 ORDER BY time",
        expectedHeader,
        retArrayLtEqPositive,
        DATABASE_NAME);

    // TC-5: int32_col = 29.1
    // Logic: Integer values can never equal a decimal fraction -> Empty result
    String[] retArrayEmpty = {};
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col = 29.1 ORDER BY time",
        expectedHeader,
        retArrayEmpty,
        DATABASE_NAME);

    // TC-6: int32_col != 29.1
    // Logic: Integer values are never equal to a decimal fraction -> Return all non-null rows
    // Expected result: All rows with int32 data (Row 5 and Row 7 are null, excluded)
    String[] retArrayNotEq = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.006Z,35,",
      "1970-01-01T00:00:00.008Z,1000,",
      "1970-01-01T00:00:00.009Z,-29,",
      "1970-01-01T00:00:00.010Z,-30,"
    };
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col != 29.1 ORDER BY time",
        expectedHeader,
        retArrayNotEq,
        DATABASE_NAME);

    // ----------------------------------------------------------------------
    // Group 2: Negative boundary -29.1
    // ----------------------------------------------------------------------

    // TC-7: int32_col <= -29.1
    // Logic: Floor(-29.1) = -30 -> int32_col <= -30
    // Expected result: Row 10(-30)
    String[] retArrayLtEqNegative = {"1970-01-01T00:00:00.010Z,-30,"};
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col <= -29.1 ORDER BY time",
        expectedHeader,
        retArrayLtEqNegative,
        DATABASE_NAME);

    // TC-8: int32_col >= -29.1
    // Logic: Ceil(-29.1) = -29 -> int32_col >= -29
    // Expected result: Row 9(-29) and all larger positive numbers (20, 29, 30, 31, 35, 1000)
    String[] retArrayGtEqNegative = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.006Z,35,",
      "1970-01-01T00:00:00.008Z,1000,",
      "1970-01-01T00:00:00.009Z,-29,"
    };
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col >= -29.1 ORDER BY time",
        expectedHeader,
        retArrayGtEqNegative,
        DATABASE_NAME);
  }

  @Test
  public void testDoubleLiteralAndInt64Column() {
    // Common header definition
    String[] expectedHeader = {"time", "int64_col"};

    // ----------------------------------------------------------------------
    // Group 1: Positive boundary 29.1
    // ----------------------------------------------------------------------

    // TC-1: int64_col >= 29.1
    // Logic: Ceil(29.1) = 30 -> int64_col >= 30
    // Expected result: Row 3(30), Row 4(31), Row 5(35), Row 8(1000)
    // Note: Row 5 (35) is included here.
    String[] retArrayGtEqPositive = {
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.005Z,35,",
      "1970-01-01T00:00:00.008Z,1000,"
    };
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col >= 29.1 ORDER BY time",
        expectedHeader,
        retArrayGtEqPositive,
        DATABASE_NAME);

    // TC-2: int64_col > 29.1
    // Logic: Floor(29.1) = 29 -> int64_col > 29 -> equivalent to int64_col >= 30
    // Expected result: Same as above
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col > 29.1 ORDER BY time",
        expectedHeader,
        retArrayGtEqPositive,
        DATABASE_NAME);

    // TC-3: int64_col <= 29.1
    // Logic: Floor(29.1) = 29 -> int64_col <= 29
    // Expected result: Row 1(20), Row 2(29), Row 9(-29), Row 10(-30)
    String[] retArrayLtEqPositive = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.009Z,-29,",
      "1970-01-01T00:00:00.010Z,-30,"
    };
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col <= 29.1 ORDER BY time",
        expectedHeader,
        retArrayLtEqPositive,
        DATABASE_NAME);

    // TC-4: int64_col < 29.1
    // Logic: Ceil(29.1) = 30 -> int64_col < 30 -> equivalent to int64_col <= 29
    // Expected result: Same as above
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col < 29.1 ORDER BY time",
        expectedHeader,
        retArrayLtEqPositive,
        DATABASE_NAME);

    // TC-5: int64_col = 29.1
    // Logic: Long values can never equal a decimal fraction -> Empty result
    String[] retArrayEmpty = {};
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col = 29.1 ORDER BY time",
        expectedHeader,
        retArrayEmpty,
        DATABASE_NAME);

    // TC-6: int64_col != 29.1
    // Logic: Long values are never equal to a decimal fraction -> Return all non-null rows
    // Expected result: All rows with int64 data (Row 6 and Row 7 are null, excluded)
    String[] retArrayNotEq = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.005Z,35,",
      "1970-01-01T00:00:00.008Z,1000,",
      "1970-01-01T00:00:00.009Z,-29,",
      "1970-01-01T00:00:00.010Z,-30,"
    };
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col != 29.1 ORDER BY time",
        expectedHeader,
        retArrayNotEq,
        DATABASE_NAME);

    // ----------------------------------------------------------------------
    // Group 2: Negative boundary -29.1
    // ----------------------------------------------------------------------

    // TC-7: int64_col <= -29.1
    // Logic: Floor(-29.1) = -30 -> int64_col <= -30
    // Expected result: Row 10(-30)
    String[] retArrayLtEqNegative = {"1970-01-01T00:00:00.010Z,-30,"};
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col <= -29.1 ORDER BY time",
        expectedHeader,
        retArrayLtEqNegative,
        DATABASE_NAME);

    // TC-8: int64_col >= -29.1
    // Logic: Ceil(-29.1) = -29 -> int64_col >= -29
    // Expected result: Row 9(-29) and all larger positive numbers (20, 29, 30, 31, 35, 1000)
    String[] retArrayGtEqNegative = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.005Z,35,",
      "1970-01-01T00:00:00.008Z,1000,",
      "1970-01-01T00:00:00.009Z,-29,"
    };
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col >= -29.1 ORDER BY time",
        expectedHeader,
        retArrayGtEqNegative,
        DATABASE_NAME);
  }

  @Test
  public void testDoubleLiteralOverflow() {
    String[] headerInt32 = {"time", "int32_col"};
    String[] headerInt64 = {"time", "int64_col"};
    String[] emptyResult = {};

    // ----------------------------------------------------------------------
    // Part 1: INT32 Column vs Double > Integer.MAX_VALUE (~2.14E9)
    // Literal: 3.0E9
    // ----------------------------------------------------------------------

    // TC-1: int32 >= 3.0E9
    // Logic: Impossible for any INT32 value.
    // Expected: Empty result.
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col >= 3.0E9 ORDER BY time",
        headerInt32,
        emptyResult,
        DATABASE_NAME);

    // TC-2: int32 <= 3.0E9
    // Logic: Always True for valid values. Must exclude NULLs (Row 5 and 7).
    // Expected: All rows where int32_col is not null.
    String[] allInt32Rows = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.006Z,35,", // Row 6 has int32
      "1970-01-01T00:00:00.008Z,1000,",
      "1970-01-01T00:00:00.009Z,-29,",
      "1970-01-01T00:00:00.010Z,-30,"
    };
    tableResultSetEqualTest(
        "SELECT time, int32_col FROM PredicateTestTable WHERE int32_col <= 3.0E9 ORDER BY time",
        headerInt32,
        allInt32Rows,
        DATABASE_NAME);

    // ----------------------------------------------------------------------
    // Part 2: INT64 Column vs Double > Long.MAX_VALUE (~9.22E18)
    // Literal: 1.0E19
    // ----------------------------------------------------------------------

    // TC-3: int64 >= 1.0E19
    // Logic: Impossible for any INT64 value.
    // Expected: Empty result.
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col >= 1.0E19 ORDER BY time",
        headerInt64,
        emptyResult,
        DATABASE_NAME);

    // TC-4: int64 <= 1.0E19
    // Logic: Always True for valid values. Must exclude NULLs (Row 6 and 7).
    // Expected: All rows where int64_col is not null.
    String[] allInt64Rows = {
      "1970-01-01T00:00:00.001Z,20,",
      "1970-01-01T00:00:00.002Z,29,",
      "1970-01-01T00:00:00.003Z,30,",
      "1970-01-01T00:00:00.004Z,31,",
      "1970-01-01T00:00:00.005Z,35,", // Row 5 has int64
      "1970-01-01T00:00:00.008Z,1000,",
      "1970-01-01T00:00:00.009Z,-29,",
      "1970-01-01T00:00:00.010Z,-30,"
    };
    tableResultSetEqualTest(
        "SELECT time, int64_col FROM PredicateTestTable WHERE int64_col <= 1.0E19 ORDER BY time",
        headerInt64,
        allInt64Rows,
        DATABASE_NAME);
  }
}
