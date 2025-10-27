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

import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;

import static java.lang.String.format;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFormatFunctionTableIT {

  private static final String DATABASE_NAME = "db";

  private static final ZoneId zoneId = ZoneId.of("Z");

  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE number_table(device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD, s3 FLOAT FIELD, s4 DOUBLE FIELD)",
        "CREATE TABLE string_table(device_id STRING TAG, s1 STRING FIELD, s2 TEXT FIELD)",
        "CREATE TABLE boolean_table(device_id STRING TAG, s1 BOOLEAN FIELD)",
        "CREATE TABLE timestamp_table(device_id STRING TAG, s1 TIMESTAMP FIELD)",
        "CREATE TABLE date_table(device_id STRING TAG, s1 DATE FIELD)",
        "CREATE TABLE null_table(device_id STRING TAG, s1 INT32 FIELD)",
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
        "INSERT INTO date_table(time, device_id, s1) VALUES (30, 'd2', '1970-01-01')",
        "INSERT INTO date_table(time, device_id, s1) VALUES (40, 'd2', '9999-12-31')",
        "INSERT INTO date_table(time, device_id, s1) VALUES (50, 'd2', '1900-01-01')",

        // data for special series
        "INSERT INTO null_table(time, device_id, s1) VALUES (10, 'd1', null)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setTimestampPrecision("ns");
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
    DateTimeUtils.initTimestampPrecision();
    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tF %1$tT', s1) FROM timestamp_table",
        new String[] {"_col0"},
        new String[] {"1970-01-01 00:00:00,", "1970-01-01 00:00:00,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tF %1$tT.%1tL', s1) FROM timestamp_table",
        new String[] {"_col0"},
        new String[] {"1970-01-01 00:00:00.000,", "1970-01-01 00:00:00.000,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%tc', s1) FROM timestamp_table",
        new String[] {"_col0"},
        new String[] {
          format("%tc,", DateTimeUtils.convertToZonedDateTime(10, zoneId)),
          format("%tc,", DateTimeUtils.convertToZonedDateTime(20, zoneId)),
        },
        DATABASE_NAME);
  }

  @Test
  public void testDateFormat() {
    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tA, %1$tB %1$te, %1$tY', s1) FROM date_table where device_id = 'd1'",
        new String[] {"_col0"},
        new String[] {
          format("%1$tA, %1$tB %1$te, %1$tY,", LocalDate.of(2024, 1, 1)),
          format("%1$tA, %1$tB %1$te, %1$tY,", LocalDate.of(2006, 7, 4))
        },
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%1$s %1$tF %1$tY-%1$tm-%1$td', s1) FROM date_table where device_id = 'd1'",
        new String[] {"_col0"},
        new String[] {"2024-01-01 2024-01-01 2024-01-01,", "2006-07-04 2006-07-04 2006-07-04,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT FORMAT('%1$tY', s1) FROM date_table where device_id = 'd2'",
        new String[] {"_col0"}, new String[] {"1970,", "9999,", "1900,"}, DATABASE_NAME);
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
        "701: Scalar function format must have at least two arguments, and first argument pattern must be TEXT or STRING type.",
        DATABASE_NAME);
  }
}
