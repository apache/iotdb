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

import org.apache.iotdb.commons.conf.CommonDescriptor;
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

import java.sql.*;

import static org.apache.iotdb.db.it.utils.TestUtils.assertTableTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBBitwiseFunctionTableIT {
  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        // bit_count_normal
        "create table bit_count_normal_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bit_count_normal_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 9, 64, 1)",
        "INSERT INTO bit_count_normal_table(Time,device_id,s1,s2,s3,s4) values(2, 'd1', 'a', 9, 8, 1)",
        "INSERT INTO bit_count_normal_table(Time,device_id,s1,s2,s3,s4) values(3, 'd1', 'a', -7, 64, 1)",
        "INSERT INTO bit_count_normal_table(Time,device_id,s1) values(4, 'd1', 'a')",
        // bit_count_error
        "create table bit_count_error_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bit_count_error_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 9, 64, 1)",
        "INSERT INTO bit_count_error_table(Time,device_id,s1,s2,s3,s4) values(2, 'd1', 'a', 9, 2, 1)",
        "INSERT INTO bit_count_error_table(Time,device_id,s1) values(3, 'd1', 'a')",
        // bitwise_and
        "create table bitwise_and_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_and_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 19, 25, 1)",
        "INSERT INTO bitwise_and_table(Time,device_id,s1) values(2, 'd1', 'a')",
        // bitwise_not
        "create table bitwise_not_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_not_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', -12, 25, 1)",
        "INSERT INTO bitwise_not_table(Time,device_id,s1,s2,s3,s4) values(2, 'd1', 'a', 19, 25, 1)",
        "INSERT INTO bitwise_not_table(Time,device_id,s1,s2,s3,s4) values(3, 'd1', 'a', 25, 25, 1)",
        "INSERT INTO bitwise_not_table(Time,device_id,s1) values(4, 'd1', 'a')",
        // bitwise_or
        "create table bitwise_or_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_or_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 19, 25, 1)",
        "INSERT INTO bitwise_or_table(Time,device_id,s1) values(2, 'd1', 'a')",
        // bitwise_xor
        "create table bitwise_xor_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_xor_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 19, 25, 1)",
        "INSERT INTO bitwise_xor_table(Time,device_id,s1) values(2, 'd1', 'a')",
        // bitwise_left_shift
        "create table bitwise_left_shift_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 1, 2, 1)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1,s2,s3,s4) values(2, 'd1', 'a', 5, 2, 1)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1,s2,s3,s4) values(3, 'd1', 'a', 20, 0, 1)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1,s2,s3,s4) values(4, 'd1', 'a', 42, 0, 1)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1,s2,s3,s4) values(5, 'd1', 'a', 0, 1, 1)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1,s2,s3,s4) values(6, 'd1', 'a', 0, 2, 1)",
        "INSERT INTO bitwise_left_shift_table(Time,device_id,s1) values(7, 'd1', 'a')",
        // bitwise_right_shift
        "create table bitwise_right_shift_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 8, 3, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(2, 'd1', 'a', 9, 1, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(3, 'd1', 'a', 20, 0, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(4, 'd1', 'a', 42, 0, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(5, 'd1', 'a', 12, 64, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(6, 'd1', 'a', -45, 64, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(7, 'd1', 'a', 0, 1, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1,s2,s3,s4) values(8, 'd1', 'a', 0, 2, 1)",
        "INSERT INTO bitwise_right_shift_table(Time,device_id,s1) values(9, 'd1', 'a')",
        // bitwise_right_shift_arithmetic
        "create table bitwise_right_shift_arithmetic_table(device_id STRING TAG, s1 TEXT FIELD, s2 INT32 FIELD, s3 INT64 FIELD, s4 FLOAT FIELD)",
        "INSERT INTO bitwise_right_shift_arithmetic_table(Time,device_id,s1,s2,s3,s4) values(1, 'd1', 'a', 12, 64, 1)",
        "INSERT INTO bitwise_right_shift_arithmetic_table(Time,device_id,s1,s2,s3,s4) values(2, 'd1', 'a', -45, 64, 1)",
        "INSERT INTO bitwise_right_shift_arithmetic_table(Time,device_id,s1) values(3, 'd1', 'a')",
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
  public void bitCountTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4", "_col5"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,9,64,2,2,2,",
          "1970-01-01T00:00:00.002Z,9,8,2,2,2,",
          "1970-01-01T00:00:00.003Z,-7,64,2,62,62,",
          "1970-01-01T00:00:00.004Z,null,null,2,null,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bit_count(9,64),bit_count(s2,64),bit_count(s2,s3) from bit_count_normal_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitCountTestError1() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time,s2,s3,bit_count(9,64),bit_count(s2,64),bit_count(s2,s3) from bit_count_error_table",
        "Argument exception, the scalar function num must be representable with the bits specified. 9 cannot be represented with 2 bits.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitCountTestError2() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time,s2,s3,bit_count(9,1),bit_count(s2,1),bit_count(s2,s3) from bit_count_error_table",
        "Argument exception, the scalar function bit_count bits must be between 2 and 64.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitCountTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bit_count(s4) from bit_count_error_table",
        "701: Scalar function bit_count only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bit_count(s1,s4) from bit_count_error_table",
        "701: Scalar function bit_count only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseAndTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4", "_col5"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,19,25,17,17,17,",
          "1970-01-01T00:00:00.002Z,null,null,17,null,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_and(19,25),bitwise_and(s2,25),bitwise_and(s2,s3) from bitwise_and_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseAndTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_and(s4) from bitwise_and_table",
        "701: Scalar function bitwise_and only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_and(s1,s4) from bitwise_and_table",
        "701: Scalar function bitwise_and only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseNotTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,-12,25,11,11,",
          "1970-01-01T00:00:00.002Z,19,25,11,-20,",
          "1970-01-01T00:00:00.003Z,25,25,11,-26,",
          "1970-01-01T00:00:00.004Z,null,null,11,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_not(-12),bitwise_not(s2) from bitwise_not_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseNotTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_not(s4) from bitwise_not_table",
        "701: Scalar function bitwise_not only accepts one argument and it must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_not(s1,s4) from bitwise_not_table",
        "701: Scalar function bitwise_not only accepts one argument and it must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseOrTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4", "_col5"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,19,25,27,27,27,",
          "1970-01-01T00:00:00.002Z,null,null,27,null,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_or(19,25),bitwise_or(s2,25),bitwise_or(s2,s3) from bitwise_or_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseOrTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_or(s4) from bitwise_or_table",
        "701: Scalar function bitwise_or only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_or(s1,s4) from bitwise_or_table",
        "701: Scalar function bitwise_or only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseXorTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4", "_col5"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,19,25,10,10,10,",
          "1970-01-01T00:00:00.002Z,null,null,10,null,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_xor(19,25),bitwise_xor(s2,25),bitwise_xor(s2,s3) from bitwise_xor_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseXorTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_xor(s4) from bitwise_xor_table",
        "701: Scalar function bitwise_xor only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_xor(s1,s4) from bitwise_xor_table",
        "701: Scalar function bitwise_xor only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseLeftShiftTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,1,2,4,4,",
          "1970-01-01T00:00:00.002Z,5,2,4,20,",
          "1970-01-01T00:00:00.003Z,20,0,4,20,",
          "1970-01-01T00:00:00.004Z,42,0,4,42,",
          "1970-01-01T00:00:00.005Z,0,1,4,0,",
          "1970-01-01T00:00:00.006Z,0,2,4,0,",
          "1970-01-01T00:00:00.007Z,null,null,4,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_left_shift(1,2),bitwise_left_shift(s2,s3) from bitwise_left_shift_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseLeftShiftTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_left_shift(s4) from bitwise_left_shift_table",
        "701: Scalar function bitwise_left_shift only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_left_shift(s1,s4) from bitwise_left_shift_table",
        "701: Scalar function bitwise_left_shift only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseRightShiftTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,8,3,1,1,",
          "1970-01-01T00:00:00.002Z,9,1,1,4,",
          "1970-01-01T00:00:00.003Z,20,0,1,20,",
          "1970-01-01T00:00:00.004Z,42,0,1,42,",
          "1970-01-01T00:00:00.005Z,12,64,1,0,",
          "1970-01-01T00:00:00.006Z,-45,64,1,0,",
          "1970-01-01T00:00:00.007Z,0,1,1,0,",
          "1970-01-01T00:00:00.008Z,0,2,1,0,",
          "1970-01-01T00:00:00.009Z,null,null,1,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_right_shift(8,3),bitwise_right_shift(s2,s3) from bitwise_right_shift_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseRightShiftTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_right_shift(s4) from bitwise_right_shift_table",
        "701: Scalar function bitwise_right_shift only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_right_shift(s1,s4) from bitwise_right_shift_table",
        "701: Scalar function bitwise_right_shift only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }

  @Test
  public void bitwiseRightShiftArithmeticTestNormal() {
    String[] expectedHeader = new String[] {"time", "s2", "s3", "_col3", "_col4"};
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,12,64,0,0,",
          "1970-01-01T00:00:00.002Z,-45,64,0,-1,",
          "1970-01-01T00:00:00.003Z,null,null,0,null,"
        };
    tableResultSetEqualTest(
        "select time,s2,s3,bitwise_right_shift_arithmetic(19,25),bitwise_right_shift_arithmetic(s2,s3) from bitwise_right_shift_arithmetic_table",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void bitwiseRightShiftArithmeticTestWithNonInteger() {
    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_right_shift_arithmetic(s4) from bitwise_right_shift_arithmetic_table",
        "701: Scalar function bitwise_right_shift_arithmetic only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);

    assertTableTestFail(
        EnvFactory.getEnv(),
        "select time, bitwise_right_shift_arithmetic(s1,s4) from bitwise_right_shift_arithmetic_table",
        "701: Scalar function bitwise_right_shift_arithmetic only accepts two arguments and they must be Int32 or Int64 data type.",
        CommonDescriptor.getInstance().getConfig().getDefaultAdminName(),
        CommonDescriptor.getInstance().getConfig().getAdminPassword(),
        DATABASE_NAME);
  }
}
