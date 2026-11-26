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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DEVICE;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBPredicateConversionTreeIT {

  private static final String DATABASE_NAME = "root.test_pred";
  private static final String DEVICE_ID = DATABASE_NAME + ".d1";
  private static final String NOT_DATABASE_NAME = "root.test_not_regression";
  private static final String TEMPLATE_DATABASE_NAME = "root.test_not_template";

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
        "INSERT INTO " + DEVICE_ID + "(time, int32_col, int64_col) VALUES(10, -30, -30)",
        "CREATE DATABASE " + NOT_DATABASE_NAME,
        "CREATE TIMESERIES " + NOT_DATABASE_NAME + ".d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES " + NOT_DATABASE_NAME + ".d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES " + NOT_DATABASE_NAME + ".d1.b WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d1(time, s1, s2, b) VALUES(1, 1, 10, true)",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d1(time, s1, s2, b) VALUES(2, 6, 20, false)",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d1(time, s1, s2, b) VALUES(3, 3, 30, true)",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d1(time, s1, s2, b) VALUES(4, 8, 40, false)",
        "CREATE TIMESERIES " + NOT_DATABASE_NAME + ".d2.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d2(time, s1) VALUES(1, 2)",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d2(time, s1) VALUES(2, 7)",
        "CREATE TIMESERIES " + NOT_DATABASE_NAME + ".d3.x WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES " + NOT_DATABASE_NAME + ".d3.y WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d3(time, x, y) VALUES(1, NaN, 1)",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d3(time, x, y) VALUES(2, 3.0, 2)",
        "INSERT INTO " + NOT_DATABASE_NAME + ".d3(time, x, y) VALUES(3, 10.0, 3)",
        "CREATE DATABASE " + TEMPLATE_DATABASE_NAME,
        "CREATE DEVICE TEMPLATE not_predicate_template ALIGNED (s1 INT64, b BOOLEAN)",
        "SET DEVICE TEMPLATE not_predicate_template TO " + TEMPLATE_DATABASE_NAME + ".d1",
        "SET DEVICE TEMPLATE not_predicate_template TO " + TEMPLATE_DATABASE_NAME + ".d2",
        "INSERT INTO " + TEMPLATE_DATABASE_NAME + ".d1(time, s1, b) ALIGNED VALUES(1, 3, true)",
        "INSERT INTO " + TEMPLATE_DATABASE_NAME + ".d1(time, s1, b) ALIGNED VALUES(2, 8, false)",
        "INSERT INTO " + TEMPLATE_DATABASE_NAME + ".d2(time, s1, b) ALIGNED VALUES(1, 4, false)",
        "INSERT INTO " + TEMPLATE_DATABASE_NAME + ".d2(time, s1, b) ALIGNED VALUES(2, 9, true)"
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

  @Test
  public void testWhereAndHavingWithNot() {
    String[] expectedWhereHeader = new String[] {TIMESTAMP_STR, DEVICE_ID + ".int32_col"};
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE NOT (int32_col > 20)",
        expectedWhereHeader,
        new String[] {"1,20,", "9,-29,", "10,-30,"});
    resultSetEqualTest(
        "SELECT int32_col FROM " + DEVICE_ID + " WHERE NOT (int32_col > 20 AND int32_col < 40)",
        expectedWhereHeader,
        new String[] {"1,20,", "8,1000,", "9,-29,", "10,-30,"});

    String[] expectedHavingHeader = new String[] {TIMESTAMP_STR, count(DEVICE_ID + ".int32_col")};
    resultSetEqualTest(
        "SELECT count(int32_col) FROM "
            + DEVICE_ID
            + " GROUP BY ([1,11),2ms) HAVING NOT (count(int32_col) > 1)",
        expectedHavingHeader,
        new String[] {"5,1,", "7,1,"});

    String[] expectedWhereAlignByDeviceHeader = new String[] {TIMESTAMP_STR, DEVICE, "int32_col"};
    resultSetEqualTest(
        "SELECT int32_col FROM root.test_pred.* WHERE NOT (int32_col > 20 AND int32_col < 40) ALIGN BY DEVICE",
        expectedWhereAlignByDeviceHeader,
        new String[] {
          "1,root.test_pred.d1,20,",
          "8,root.test_pred.d1,1000,",
          "9,root.test_pred.d1,-29,",
          "10,root.test_pred.d1,-30,",
        });

    String[] expectedHavingAlignByDeviceHeader =
        new String[] {TIMESTAMP_STR, DEVICE, count("int32_col")};
    resultSetEqualTest(
        "SELECT count(int32_col) FROM root.test_pred.* GROUP BY ([1,11),2ms) HAVING NOT (count(int32_col) > 1) ALIGN BY DEVICE",
        expectedHavingAlignByDeviceHeader,
        new String[] {"5,root.test_pred.d1,1,", "7,root.test_pred.d1,1,"});
  }

  @Test
  public void testTemplatedWhereWithNot() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, DEVICE, "s1"};
    resultSetEqualTest(
        "SELECT s1 FROM " + TEMPLATE_DATABASE_NAME + ".* WHERE NOT (s1 > 5) ALIGN BY DEVICE",
        expectedHeader,
        new String[] {
          "1,root.test_not_template.d1,3,", "1,root.test_not_template.d2,4,",
        });

    String[] expectedBooleanHeader = new String[] {TIMESTAMP_STR, DEVICE, "s1", "b"};
    resultSetEqualTest(
        "SELECT s1, b FROM " + TEMPLATE_DATABASE_NAME + ".* WHERE NOT b ALIGN BY DEVICE",
        expectedBooleanHeader,
        new String[] {
          "2,root.test_not_template.d1,8,false,", "1,root.test_not_template.d2,4,false,",
        });
  }

  @Test
  public void testUnsupportedNotPredicate() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, NOT_DATABASE_NAME + ".d1.s1", NOT_DATABASE_NAME + ".d1.b"};
    resultSetEqualTest(
        "SELECT s1, b FROM " + NOT_DATABASE_NAME + ".d1 WHERE NOT b OR s1 > 5",
        expectedHeader,
        new String[] {"2,6,false,", "4,8,false,"});
    resultSetEqualTest(
        "SELECT s1, b FROM " + NOT_DATABASE_NAME + ".d1 WHERE NOT b",
        expectedHeader,
        new String[] {"2,6,false,", "4,8,false,"});
    resultSetEqualTest(
        "SELECT s1, b FROM "
            + NOT_DATABASE_NAME
            + ".d1 WHERE NOT (CASE WHEN s1 > 5 THEN true ELSE false END) OR s2 > 35",
        expectedHeader,
        new String[] {"1,1,true,", "3,3,true,", "4,8,false,"});
  }

  @Test
  public void testNotPredicateWithTimeAndMultipleSeries() {
    resultSetEqualTest(
        "SELECT s1, s2 FROM " + NOT_DATABASE_NAME + ".d1 WHERE NOT (time > 2 OR s1 > 5)",
        new String[] {TIMESTAMP_STR, NOT_DATABASE_NAME + ".d1.s1", NOT_DATABASE_NAME + ".d1.s2"},
        new String[] {"1,1,10,"});

    resultSetEqualTest(
        "SELECT s1, s2 FROM "
            + NOT_DATABASE_NAME
            + ".d1 WHERE NOT (time > 2 OR s1 > 5) ALIGN BY DEVICE",
        new String[] {TIMESTAMP_STR, DEVICE, "s1", "s2"},
        new String[] {"1," + NOT_DATABASE_NAME + ".d1,1,10,"});
  }

  @Test
  public void testNestedNotPredicate() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, NOT_DATABASE_NAME + ".d1.s1", NOT_DATABASE_NAME + ".d1.s2"};
    resultSetEqualTest(
        "SELECT s1, s2 FROM "
            + NOT_DATABASE_NAME
            + ".d1 WHERE NOT (NOT (NOT (s1 > 5 AND s2 > 25)))",
        expectedHeader,
        new String[] {"1,1,10,", "2,6,20,", "3,3,30,"});
  }

  @Test
  public void testNotPredicateWithNaN() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT x, y FROM " + NOT_DATABASE_NAME + ".d3 WHERE NOT (x > 5 OR y > 2)")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertTrue(Double.isNaN(resultSet.getDouble(2)));
        assertEquals(1, resultSet.getInt(3));
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getLong(1));
        assertEquals(3.0, resultSet.getDouble(2), 0.0);
        assertEquals(2, resultSet.getInt(3));
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT x, y FROM " + NOT_DATABASE_NAME + ".d3 WHERE NOT (x + y > 5)")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertTrue(Double.isNaN(resultSet.getDouble(2)));
        assertTrue(resultSet.next());
        assertEquals(2, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testCompositeHavingNot() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s1), sum(s2) FROM "
                  + NOT_DATABASE_NAME
                  + ".d1 HAVING NOT (count(s1) > 5 AND sum(s2) > 50)")) {
        assertTrue(resultSet.next());
        assertEquals(4, resultSet.getLong(1));
        assertEquals(100.0, resultSet.getDouble(2), 0.0);
        assertFalse(resultSet.next());
      }

      try (ResultSet resultSet =
          statement.executeQuery(
              "SELECT count(s1) FROM "
                  + NOT_DATABASE_NAME
                  + ".* GROUP BY ([1,5),10ms), LEVEL=1 "
                  + "HAVING NOT (count(s1) > 0 AND sum(s2) > 150)")) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getLong(1));
        assertFalse(resultSet.next());
      }
    }
  }

  @Test
  public void testHavingNotWithBooleanAggregation() {
    resultSetEqualTest(
        "SELECT count(s1) FROM "
            + NOT_DATABASE_NAME
            + ".d1 GROUP BY ([1,5),1ms) HAVING NOT last_value(b)",
        new String[] {TIMESTAMP_STR, count(NOT_DATABASE_NAME + ".d1.s1")},
        new String[] {"2,1,", "4,1,"});

    resultSetEqualTest(
        "SELECT count(s1) FROM "
            + NOT_DATABASE_NAME
            + ".d1 GROUP BY ([1,5),1ms) HAVING NOT last_value(b) ALIGN BY DEVICE",
        new String[] {TIMESTAMP_STR, DEVICE, count("s1")},
        new String[] {"2," + NOT_DATABASE_NAME + ".d1,1,", "4," + NOT_DATABASE_NAME + ".d1,1,"});
  }

  @Test
  public void testTemplatedHavingNotWithBooleanAggregation() {
    Set<String> expectedRows = new HashSet<>();
    expectedRows.add("1," + TEMPLATE_DATABASE_NAME + ".d2,1,");
    expectedRows.add("2," + TEMPLATE_DATABASE_NAME + ".d1,1,");
    resultSetEqualTest(
        "SELECT count(s1) FROM "
            + TEMPLATE_DATABASE_NAME
            + ".* GROUP BY ([1,3),1ms) HAVING NOT last_value(b) ALIGN BY DEVICE",
        TIMESTAMP_STR + "," + DEVICE + "," + count("s1") + ",",
        expectedRows);
  }
}
