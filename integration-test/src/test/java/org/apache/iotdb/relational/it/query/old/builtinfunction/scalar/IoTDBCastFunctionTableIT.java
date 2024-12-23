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
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBCastFunctionTableIT {

  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "create table normal(device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 STRING MEASUREMENT)",
        // data for int series
        "INSERT INTO normal(time, device_id ,s1) values(0, 'd1', 0)",
        "INSERT INTO normal(time, device_id ,s1) values(1, 'd1', 1)",
        "INSERT INTO normal(time, device_id ,s1) values(2, 'd1', 2)",
        "INSERT INTO normal(time, device_id ,s1) values(3, 'd1', 3)",
        // data for long series
        "INSERT INTO normal(time, device_id ,s2) values(0, 'd1', 0)",
        "INSERT INTO normal(time, device_id ,s2) values(1, 'd1', 1)",
        "INSERT INTO normal(time, device_id ,s2) values(2, 'd1', 2)",
        "INSERT INTO normal(time, device_id ,s2) values(3, 'd1', 3)",
        // data for float series
        "INSERT INTO normal(time, device_id ,s3) values(0, 'd1', 0)",
        "INSERT INTO normal(time, device_id ,s3) values(1, 'd1', 1)",
        "INSERT INTO normal(time, device_id ,s3) values(2, 'd1', 2.7)",
        "INSERT INTO normal(time, device_id ,s3) values(3, 'd1', 3.33)",
        // data for double series
        "INSERT INTO normal(time, device_id ,s4) values(0, 'd1', 0)",
        "INSERT INTO normal(time, device_id ,s4) values(1, 'd1', 1.0)",
        "INSERT INTO normal(time, device_id ,s4) values(2, 'd1', 2.7)",
        "INSERT INTO normal(time, device_id ,s4) values(3, 'd1', 3.33)",
        // data for boolean series
        "INSERT INTO normal(time, device_id ,s5) values(0, 'd1', false)",
        "INSERT INTO normal(time, device_id ,s5) values(1, 'd1', false)",
        "INSERT INTO normal(time, device_id ,s5) values(2, 'd1', true)",
        "INSERT INTO normal(time, device_id ,s5) values(3, 'd1', true)",
        // data for text series
        "INSERT INTO normal(time, device_id ,s6) values(0, 'd1', '10000')",
        "INSERT INTO normal(time, device_id ,s6) values(1, 'd1', 3)",
        "INSERT INTO normal(time, device_id ,s6) values(2, 'd1', 'TRue')",
        "INSERT INTO normal(time, device_id ,s6) values(3, 'd1', 'faLse')",
        "flush",

        // special cases
        "create table special(device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 FLOAT MEASUREMENT, s4 DOUBLE MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 STRING MEASUREMENT)",
        "INSERT INTO special(Time, device_id ,s2) values(1, 'd1', 2147483648)",
        "INSERT INTO special(Time, device_id ,s3) values(1, 'd1', 2147483648.0)",
        "INSERT INTO special(Time, device_id ,s3) values(2, 'd1', 2e38)",
        "INSERT INTO special(Time, device_id ,s4) values(1, 'd1', 4e50)",
        "INSERT INTO special(Time, device_id ,s6) values(1, 'd1', 'test')",
        "INSERT INTO special(Time, device_id ,s6) values(2, 'd1', '1.1')",
        "INSERT INTO special(Time, device_id ,s6) values(3, 'd1', '4e60')",
        "INSERT INTO special(Time, device_id ,s6) values(4, 'd1', '4e60000')",
        "flush",

        // special cases for date and timestamp
        "create table dateType(device_id STRING ID, s1 DATE MEASUREMENT, s2 TIMESTAMP MEASUREMENT)",
        "INSERT INTO dateType(Time,device_id, s1, s2) values(1,'d1', '9999-12-31', 253402271999999)",
        "INSERT INTO dateType(Time,device_id, s1, s2) values(2,'d1', '1000-01-01', -30610224000000)",
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

  // region ================== New Transformer ==================
  @Test
  public void testNewTransformerWithIntSource() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s1"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s1 AS INT32) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s1 AS INT64) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s1 AS FLOAT) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s1 AS DOUBLE) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s1 AS BOOLEAN) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to string
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s1 AS STRING) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithLongSource() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s2"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };

    tableResultSetEqualTest(
        "select Time, CAST(s2 AS INT32) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s2 AS INT64) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s2 AS FLOAT) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s2 AS DOUBLE) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s2 AS BOOLEAN) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s2 AS STRING) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithFloatSource() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s3"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s3 AS INT32) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s3 AS INT64) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s3 AS FLOAT) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.700000047683716,",
          "1970-01-01T00:00:00.003Z,3.3299999237060547,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s3 AS DOUBLE) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s3 AS BOOLEAN) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s3 AS STRING) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithDoubleSource() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s4"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s4 AS INT32) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s4 AS INT64) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s4 AS FLOAT) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s4 AS DOUBLE) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s4 AS BOOLEAN) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to string
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s4 AS STRING) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithBooleanSource() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s5"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,0,",
          "1970-01-01T00:00:00.002Z,1,",
          "1970-01-01T00:00:00.003Z,1,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s5 AS INT32) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,0,",
          "1970-01-01T00:00:00.002Z,1,",
          "1970-01-01T00:00:00.003Z,1,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s5 AS INT64) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,1.0,",
          "1970-01-01T00:00:00.003Z,1.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s5 AS FLOAT) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,1.0,",
          "1970-01-01T00:00:00.003Z,1.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s5 AS DOUBLE) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,false,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s5 AS BOOLEAN) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,false,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s5 AS STRING) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithTextSource() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s6"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000,", "1970-01-01T00:00:00.001Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s6 AS INT32) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000,", "1970-01-01T00:00:00.001Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s6 AS INT64) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000.0,", "1970-01-01T00:00:00.001Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s6 AS FLOAT) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000.0,", "1970-01-01T00:00:00.001Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s6 AS DOUBLE) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,true,", "1970-01-01T00:00:00.003Z,false,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s6 AS BOOLEAN) as cast_s6 from normal where device_id='d1' and Time >= 2",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000,", "1970-01-01T00:00:00.001Z,3,",
          "1970-01-01T00:00:00.002Z,TRue,", "1970-01-01T00:00:00.003Z,faLse,",
        };
    tableResultSetEqualTest(
        "select Time, CAST(s6 AS STRING) as cast_s6 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  // endregion

  // region try_cast

  @Test
  public void testNewTransformerWithIntSourceInTryCast() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s1"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s1 AS INT32) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s1 AS INT64) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s1 AS FLOAT) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s1 AS DOUBLE) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s1 AS BOOLEAN) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to string
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s1 AS STRING) as cast_s1 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithLongSourceInTryCast() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s2"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };

    tableResultSetEqualTest(
        "select Time, TRY_CAST(s2 AS INT32) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s2 AS INT64) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s2 AS FLOAT) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s2 AS DOUBLE) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s2 AS BOOLEAN) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,2,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s2 AS STRING) as cast_s2 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithFloatSourceInTryCast() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s3"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s3 AS INT32) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s3 AS INT64) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s3 AS FLOAT) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.700000047683716,",
          "1970-01-01T00:00:00.003Z,3.3299999237060547,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s3 AS DOUBLE) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s3 AS BOOLEAN) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s3 AS STRING) as cast_s3 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithDoubleSourceInTryCast() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s4"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s4 AS INT32) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,1,",
          "1970-01-01T00:00:00.002Z,3,",
          "1970-01-01T00:00:00.003Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s4 AS INT64) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s4 AS FLOAT) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s4 AS DOUBLE) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,true,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s4 AS BOOLEAN) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to string
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,1.0,",
          "1970-01-01T00:00:00.002Z,2.7,",
          "1970-01-01T00:00:00.003Z,3.33,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s4 AS STRING) as cast_s4 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithBooleanSourceInTryCast() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s5"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,0,",
          "1970-01-01T00:00:00.002Z,1,",
          "1970-01-01T00:00:00.003Z,1,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s5 AS INT32) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0,",
          "1970-01-01T00:00:00.001Z,0,",
          "1970-01-01T00:00:00.002Z,1,",
          "1970-01-01T00:00:00.003Z,1,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s5 AS INT64) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,1.0,",
          "1970-01-01T00:00:00.003Z,1.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s5 AS FLOAT) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,0.0,",
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,1.0,",
          "1970-01-01T00:00:00.003Z,1.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s5 AS DOUBLE) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,false,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s5 AS BOOLEAN) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,false,",
          "1970-01-01T00:00:00.001Z,false,",
          "1970-01-01T00:00:00.002Z,true,",
          "1970-01-01T00:00:00.003Z,true,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s5 AS STRING) as cast_s5 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  @Test
  public void testNewTransformerWithTextSourceInTryCast() {
    // cast to int
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "cast_s6"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000,", "1970-01-01T00:00:00.001Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s6 AS INT32) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        intRetArray,
        DATABASE_NAME);

    // cast to long
    String[] longRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000,", "1970-01-01T00:00:00.001Z,3,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s6 AS INT64) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        longRetArray,
        DATABASE_NAME);

    // cast to float
    String[] floatRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000.0,", "1970-01-01T00:00:00.001Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s6 AS FLOAT) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        floatRetArray,
        DATABASE_NAME);

    // cast to double
    String[] doubleRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000.0,", "1970-01-01T00:00:00.001Z,3.0,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s6 AS DOUBLE) as cast_s6 from normal where device_id='d1' and Time < 2",
        expectedHeader,
        doubleRetArray,
        DATABASE_NAME);

    // cast to boolean
    String[] booleanRetArray =
        new String[] {
          "1970-01-01T00:00:00.002Z,true,", "1970-01-01T00:00:00.003Z,false,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s6 AS BOOLEAN) as cast_s6 from normal where device_id='d1' and Time >= 2",
        expectedHeader,
        booleanRetArray,
        DATABASE_NAME);

    // cast to text
    String[] stringRetArray =
        new String[] {
          "1970-01-01T00:00:00.000Z,10000,", "1970-01-01T00:00:00.001Z,3,",
          "1970-01-01T00:00:00.002Z,TRue,", "1970-01-01T00:00:00.003Z,faLse,",
        };
    tableResultSetEqualTest(
        "select Time, TRY_CAST(s6 AS STRING) as cast_s6 from normal where device_id='d1'",
        expectedHeader,
        stringRetArray,
        DATABASE_NAME);
  }

  // endregion

  // region special cases

  @Test
  public void testCastWithLongSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("USE " + DATABASE_NAME);
        statement.execute("select CAST(s2 AS INT32) from special where device_id='d1'");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTryCastWithLongSource() {
    String sql = "select TRY_CAST(s2 AS INT32) from special where device_id='d1'";
    tableResultSetEqualTest(
        sql,
        new String[] {"_col0"},
        new String[] {"null,", "null,", "null,", "null,"},
        DATABASE_NAME);
  }

  @Test
  public void testCastWithFloatSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("USE " + DATABASE_NAME);
        statement.execute("select CAST(s3 AS INT32) from special where device_id='d1'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("USE " + DATABASE_NAME);
        statement.execute("select CAST(s3 AS INT64) from special where device_id='d1'");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTryCastWithFloatSource() {
    String sql = "select TRY_CAST(s3 AS INT32) from special where device_id='d1'";
    tableResultSetEqualTest(
        sql,
        new String[] {"_col0"},
        new String[] {"2147483647,", "null,", "null,", "null,"},
        DATABASE_NAME);
  }

  @Test
  public void testCastWithDoubleSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("USE " + DATABASE_NAME);
        statement.execute("select CAST(s4 AS INT32) from special where device_id='d1'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("USE " + DATABASE_NAME);
        statement.execute("select CAST(s4 AS INT64) from special where device_id='d1'");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("USE " + DATABASE_NAME);
        statement.execute("select CAST(s4 AS Float) from special where device_id='d1'");
        fail();
      } catch (Exception ignored) {
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTryCastWithDoubleSource() {
    String sql = "select TRY_CAST(s4 AS Float) from special where device_id='d1'";
    tableResultSetEqualTest(
        sql,
        new String[] {"_col0"},
        new String[] {"null,", "null,", "null,", "null,"},
        DATABASE_NAME);
  }

  @Test
  public void testCastWithTextSource() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try {
        statement.execute(
            "select CAST(s6 AS INT32) from special where device_id='d1' and time = 1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "select CAST(s6 AS INT32) from special where device_id='d1' and time = 2");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "select CAST(s6 AS INT64) from special where device_id='d1' and time = 1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "select CAST(s6 AS INT64) from special where device_id='d1' and time = 2");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS FLOAT) from special where device_id='d1' and time=3");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select CAST(s6 AS FLOAT) from special where device_id='d1' and time=1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "select CAST(s6 AS DOUBLE) from special where device_id='d1' and time = 1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "select CAST(s6 AS DOUBLE) from special where device_id='d1' and time = 4");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute(
            "select CAST(s6 AS BOOLEAN) from special where device_id='d1' and time = 1");
        fail();
      } catch (Exception ignored) {
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testTryCastWithTextSource() {
    String sql = "select TRY_CAST(s6 AS INT32) from special where device_id='d1' and time = 1";
    tableResultSetEqualTest(sql, new String[] {"_col0"}, new String[] {"null,"}, DATABASE_NAME);
  }

  @Test
  public void testDateOutOfRange() {
    tableAssertTestFail(
        String.format(
            "select CAST((s1 + %d) AS TIMESTAMP) from dateType where time = 1",
            DateTimeUtils.correctPrecision(86400000)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);

    tableAssertTestFail(
        String.format(
            "select CAST((s2 + %d) AS DATE) from dateType where time = 1",
            DateTimeUtils.correctPrecision(86400000)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);

    tableAssertTestFail(
        String.format(
            "select CAST((s1 - %d) AS TIMESTAMP) from dateType where time = 2",
            DateTimeUtils.correctPrecision(86400000)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);

    tableAssertTestFail(
        String.format(
            "select CAST((s2 - %d) AS DATE) from dateType where time = 2",
            DateTimeUtils.correctPrecision(86400000)),
        "752: Year must be between 1000 and 9999.",
        DATABASE_NAME);
  }

  // endregion
}
