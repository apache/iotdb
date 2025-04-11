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

package org.apache.iotdb.relational.it.query.old.query;

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

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBGreatestLeastTableIT {

  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE boolean_table(device_id STRING TAG, bool1 BOOLEAN FIELD, bool2 BOOLEAN FIELD)",
        "CREATE TABLE number_table(device_id STRING TAG, int1 INT32 FIELD, int2 INT32 FIELD, long1 INT64 FIELD, long2 INT64 FIELD, float1 FLOAT FIELD, float2 FLOAT FIELD, double1 DOUBLE FIELD, double2 DOUBLE FIELD)",
        "CREATE TABLE string_table(device_id STRING TAG, string1 STRING FIELD, string2 STRING FIELD, text1 TEXT FIELD, text2 TEXT FIELD)",
        "CREATE TABLE mix_type_table(device_id STRING TAG, s1 INT32 FIELD, s2 INT64 FIELD, s3 FLOAT FIELD, s4 DOUBLE FIELD, s5 BOOLEAN FIELD, s6 STRING FIELD, s7 TEXT FIELD)",
        "CREATE TABLE null_table(device_id STRING TAG, string1 STRING FIELD, string2 STRING FIELD, int1 INT32 FIELD, int2 INT32 FIELD, double1 DOUBLE FIELD, double2 DOUBLE FIELD, timestamp1 TIMESTAMP FIELD, timestamp2 TIMESTAMP FIELD)",
        "CREATE TABLE any_null_table(device_id STRING TAG, string1 STRING FIELD, string2 STRING FIELD, int1 INT32 FIELD, int2 INT32 FIELD, double1 DOUBLE FIELD, double2 DOUBLE FIELD, timestamp1 TIMESTAMP FIELD, timestamp2 TIMESTAMP FIELD)",
        // normal case
        "INSERT INTO number_table(time, device_id, int1, int2, long1, long2, float1, float2, double1, double2) VALUES (10, 'd1', 1000000, 2000000, 1000000, 2000000, 10.1, 20.2, 10.1, 20.2)",
        "INSERT INTO string_table(time, device_id, string1, string2, text1, text2) VALUES(10, 'd1', 'aaa', 'bbb', 'aaa', 'bbb')",
        "INSERT INTO boolean_table(time, device_id, bool1, bool2) VALUES(10, 'd1', true, false)",
        "INSERT INTO boolean_table(time, device_id, bool1, bool2) VALUES(20, 'd1', false, true)",
        "INSERT INTO boolean_table(time, device_id, bool1, bool2) VALUES(30, 'd1', true, true)",
        "INSERT INTO mix_type_table(time, device_id, s1, s2, s3, s4, s5, s6, s7) VALUES(10, 'd1', 1, 1, 1.0, 1.0, true, 'a', 'a')",
        "INSERT INTO null_table(time, device_id, string1, string2) VALUES(10, 'd1', null, null)",
        "INSERT INTO any_null_table(time, device_id, string2, int2, double2, timestamp2) VALUES(10, 'd1', 'test', 10, 10.0, 10)",
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
  public void testNumberTypeGreatestFunction() {

    tableResultSetEqualTest(
        "SELECT GREATEST(int1, int2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"2000000,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(long1, long2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"2000000,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(float1, float2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"20.2,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(double1, double2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"20.2,"},
        DATABASE_NAME);
  }

  @Test
  public void testNumberTypeLeastFunction() {
    tableResultSetEqualTest(
        "SELECT LEAST(int1, int2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"1000000,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(long1, long2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"1000000,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(float1, float2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"10.1,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(double1, double2) FROM number_table",
        new String[] {"_col0"},
        new String[] {"10.1,"},
        DATABASE_NAME);
  }

  @Test
  public void testStringTypeGreatestFunction() {
    tableResultSetEqualTest(
        "SELECT GREATEST(string1, string2) FROM string_table",
        new String[] {"_col0"},
        new String[] {"bbb,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(text1, text2) FROM string_table",
        new String[] {"_col0"},
        new String[] {"bbb,"},
        DATABASE_NAME);
  }

  @Test
  public void testStringTypeLeastFunction() {
    tableResultSetEqualTest(
        "SELECT LEAST(string1, string2) FROM string_table",
        new String[] {"_col0"},
        new String[] {"aaa,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(text1, text2) FROM string_table",
        new String[] {"_col0"},
        new String[] {"aaa,"},
        DATABASE_NAME);
  }

  @Test
  public void testBooleanTypeGreatestFunction() {
    tableResultSetEqualTest(
        "SELECT GREATEST(bool1, bool2) FROM boolean_table",
        new String[] {"_col0"},
        new String[] {"true,", "true,", "true,"},
        DATABASE_NAME);
  }

  @Test
  public void testBooleanTypeLeastFunction() {
    tableResultSetEqualTest(
        "SELECT LEAST(bool1, bool2) FROM boolean_table",
        new String[] {"_col0"},
        new String[] {"false,", "false,", "true,"},
        DATABASE_NAME);
  }

  @Test
  public void testAllNullValue() {
    tableResultSetEqualTest(
        "SELECT GREATEST(string1, string2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(string1, string2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(int1, int2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(int1, int2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(double1, double2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(double1, double2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(timestamp1, timestamp2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(timestamp1, timestamp2) FROM null_table",
        new String[] {"_col0"},
        new String[] {"null,"},
        DATABASE_NAME);
  }

  @Test
  public void testAnyNullValue() {
    tableResultSetEqualTest(
        "SELECT GREATEST(string1, string2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"test,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(string1, string2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"test,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(int1, int2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"10,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(int1, int2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"10,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(double1, double2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"10.0,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(double1, double2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"10.0,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT GREATEST(timestamp1, timestamp2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"1970-01-01T00:00:00.010Z,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT LEAST(timestamp1, timestamp2) FROM any_null_table",
        new String[] {"_col0"},
        new String[] {"1970-01-01T00:00:00.010Z,"},
        DATABASE_NAME);
  }

  @Test
  public void testAnomalies() {
    // do not support different type
    for (int i = 1; i <= 7; i++) {
      for (int j = i + 1; j <= 7; j++) {
        tableAssertTestFail(
            String.format("SELECT LEAST(s%d, s%d) FROM mix_type_table", i, j),
            "701: Scalar function least must have at least two arguments, and all type must be the same.",
            DATABASE_NAME);

        tableAssertTestFail(
            String.format("SELECT GREATEST(s%d, s%d) FROM mix_type_table", i, j),
            "701: Scalar function greatest must have at least two arguments, and all type must be the same.",
            DATABASE_NAME);
      }
    }
  }
}
