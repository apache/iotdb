/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.relational.it.query.old.builtinfunction.scalar;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

public class IoTDBFormatFunctionTableIT {

  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE number_table(device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT)",
        "CREATE TABLE string_table(device_id STRING ID, s1 STRING MEASUREMENT, s2 TEXT MEASUREMENT)",
        "CREATE TABLE boolean_table(device_id STRING ID, s1 BOOLEAN MEASUREMENT)",
        "CREATE TABLE timestamp_table(device_id STRING ID, s1 TIMESTAMP MEASUREMENT)",
        "CREATE TABLE date_table(device_id STRING ID, s1 DATE MEASUREMENT)",
        "CREATE TABLE null_table(device_id STRING ID, s1 INT32 MEASUREMENT)",
        // data for number series
        "INSERT INTO number_table(time, device_id, s1, s2, s3, s4) VALUES (10, 'd1', 1000000, 1000000, 3.1415926, 3.1415926)",
        "INSERT INTO number_table(time, device_id, s1, s2, s3, s4) VALUES (20, 'd1', 1, 1, 1234567.25, 1234567.25)",

        // data for string series
        "INSERT INTO string_table(time, device_id, s1, s2) VALUES (10, 'd1', 'world', 'world')",
        "INSERT INTO string_table(time, device_id, s1, s2) VALUES (20, 'd1', '123', '123')",

        // data for boolean series
        "INSERT INTO boolean_table(time, device_id, s1) VALUES (10, 'd1', 'true')",
        "INSERT INTO boolean_table(time, device_id, s1) VALUES (20, 'd1', 'false')",

        // data for timestamp series
        "INSERT INTO timestamp_table(time, device_id, s1) VALUES (10, 'd1', 10)",
        "INSERT INTO timestamp_table(time, device_id, s1) VALUES (20, 'd1', 20)",

        // data for date series
        "INSERT INTO date_table(time, device_id, s1) VALUES (10, 'd1', '2024-01-01')",
        "INSERT INTO date_table(time, device_id, s1) VALUES (20, 'd1', '2006-07-04')",

        // data for special series
        "INSERT INTO null_table(time, device_id) VALUES (10, 'd1')",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  protected static void insertData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testStringFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('hello %s', s1),FORMAT('hello %s', s2) FROM string_table WHERE time = 10",
        new String[] {"_col0", "_col1"}, new String[] {"hello world,hello world,"}, DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%s%%', s1),FORMAT('%s%%', s1) FROM string_table WHERE time = 20",
        new String[] {"_col0", "_col1"}, new String[] {"123%,123%,"}, DATABASE_NAME);
  }

  @Test
  public void testBooleanFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('%1$b %1$B', s1) FROM boolean_table",
        new String[] {"_col0"}, new String[] {"true TRUE,", "false FALSE,"}, DATABASE_NAME);
  }

  @Test
  public void testNumberFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('%,d %,d', s1, s2) FROM number_table WHERE time = 10",
        new String[] {"_col0"}, new String[] {"1,000,000 1,000,000,"}, DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%.2f %.2f', s3, s4) FROM number_table WHERE time = 10",
        new String[] {"_col0"}, new String[] {"3.14 3.14,"}, DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%03d %03d', s1, s2) FROM number_table WHERE time = 20",
        new String[] {"_col0"}, new String[] {"001 001,"}, DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%,.2f %,.2f', s3, s4) FROM number_table WHERE time = 20",
        new String[] {"_col0"}, new String[] {"1,234,567.25 1,234,567.25,"}, DATABASE_NAME);
  }

  @Test
  public void testTimestampFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tF %1$tT', s1) FROM timestamp_table",
        new String[] {"_col0"},
        new String[] {"1970-01-01 00:00:00,", "1970-01-01 00:00:00,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tF %1$tT.%1tL', s1) FROM timestamp_table",
        new String[] {"_col0"},
        new String[] {"1970-01-01 00:00:00.010,", "1970-01-01 00:00:00.020,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%tc', s1) FROM timestamp_table",
        new String[] {"_col0"},
        new String[] {"周四 1月 01 00:00:00 Z 1970,", "周四 1月 01 00:00:00 Z 1970,"},
        DATABASE_NAME);
  }

  @Test
  public void testDateFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tA, %1$tB %1$te, %1$tY', s1) FROM date_table",
        new String[] {"_col0"},
        new String[] {"星期一, 一月 1, 2024,", "星期二, 七月 4, 2006,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%1$s %1$tF %1$tY-%1$tm-%1$td', s1) FROM date_table",
        new String[] {"_col0"},
        new String[] {"2024-01-01 2024-01-01 2024-01-01,", "2006-07-04 2006-07-04 2006-07-04,"},
        DATABASE_NAME);
  }

  @Test
  public void testNullFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('Value:%s', s1) FROM null_table",
        new String[] {"_col0"}, new String[] {"Value:null,"}, DATABASE_NAME);
  }

  @Test
  public void testAnomalies() {
    tableAssertTestFail(
        "SELECT FORMAT('%1$tA, %1$tB %1$te, %1$tY', s1) FROM number_table",
        "701: Invalid format string:", DATABASE_NAME);

    tableAssertTestFail(
        "SELECT FORMAT('%f', s1) FROM string_table", "701: Invalid format string:", DATABASE_NAME);

    tableAssertTestFail(
        "SELECT FORMAT('%s %d', s1) FROM string_table",
        "701: Invalid format string:", DATABASE_NAME);

    tableAssertTestFail(
        "SELECT FORMAT('%s') FROM string_table",
        "701: Scalar function format must have at least two arguments, and first argument must be char type.",
        DATABASE_NAME);
  }
}
