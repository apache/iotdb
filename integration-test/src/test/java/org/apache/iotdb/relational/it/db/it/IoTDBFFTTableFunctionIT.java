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

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBFFTTableFunctionIT {

  private static final String DATABASE_NAME = "test_fft";
  private static final String[] SQLS =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE signal(device_id STRING TAG, temperature DOUBLE FIELD, speed INT32 FIELD, note STRING FIELD)",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (0, 'd1', 1.0, 1, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (1000, 'd1', 0.0, 2, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (2000, 'd1', 0.0, 3, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (3000, 'd1', 0.0, 4, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (0, 'd2', 2.0, 4, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (1000, 'd2', 0.0, 3, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (2000, 'd2', 0.0, 2, 'ok')",
        "INSERT INTO signal(time, device_id, temperature, speed, note) VALUES (3000, 'd2', 0.0, 1, 'ok')",
        "CREATE TABLE single_row(device_id STRING TAG, value DOUBLE FIELD)",
        "INSERT INTO single_row(time, device_id, value) VALUES (0, 'd1', 1.0)",
        "CREATE TABLE no_numeric(device_id STRING TAG, note STRING FIELD)",
        "INSERT INTO no_numeric(time, device_id, note) VALUES (0, 'd1', 'ok')",
        "CREATE TABLE with_null(device_id STRING TAG, value DOUBLE FIELD)",
        "INSERT INTO with_null(time, device_id, value) VALUES (0, 'd1', 1.0)",
        "INSERT INTO with_null(time, device_id, value) VALUES (1000, 'd1', null)",
        "CREATE TABLE irregular(device_id STRING TAG, value DOUBLE FIELD)",
        "INSERT INTO irregular(time, device_id, value) VALUES (0, 'd1', 1.0)",
        "INSERT INTO irregular(time, device_id, value) VALUES (1000, 'd1', 2.0)",
        "INSERT INTO irregular(time, device_id, value) VALUES (2500, 'd1', 3.0)",
        "FLUSH"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void insertData() {
    String currentSql = null;
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : SQLS) {
        currentSql = sql;
        statement.execute(sql);
      }
    } catch (Exception e) {
      throw new AssertionError("insertData failed while executing [" + currentSql + "].", e);
    }
  }

  @Test
  public void testFFTWithPartitionAndMultipleColumns() {
    String[] expectedHeader =
        new String[] {
          "device_id",
          "frequency_index",
          "frequency",
          "temperature_real",
          "temperature_imag",
          "speed_real",
          "speed_imag"
        };
    String[] retArray =
        new String[] {
          "d1,0,0.0,1.0,0.0,10.0,0.0,",
          "d1,1,0.25,1.0,0.0,-2.0,2.0,",
          "d1,2,-0.5,1.0,0.0,-2.0,0.0,",
          "d1,3,-0.25,1.0,0.0,-2.0,-2.0,",
          "d2,0,0.0,2.0,0.0,10.0,0.0,",
          "d2,1,0.25,2.0,0.0,2.0,-2.0,",
          "d2,2,-0.5,2.0,0.0,2.0,0.0,",
          "d2,3,-0.25,2.0,0.0,2.0,2.0,"
        };

    tableResultSetEqualTest(
        "SELECT * FROM FFT(DATA => signal PARTITION BY device_id ORDER BY time, "
            + "SAMPLE_INTERVAL => 1s, N => 4) ORDER BY device_id, frequency_index",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFFTDefaultNAndInferredSampleInterval() {
    String[] expectedHeader =
        new String[] {"frequency_index", "frequency", "temperature_real", "temperature_imag"};
    String[] retArray =
        new String[] {"0,0.0,1.0,0.0,", "1,0.25,1.0,0.0,", "2,-0.5,1.0,0.0,", "3,-0.25,1.0,0.0,"};

    tableResultSetEqualTest(
        "SELECT frequency_index, frequency, temperature_real, temperature_imag "
            + "FROM FFT(DATA => (SELECT time, temperature FROM signal WHERE device_id='d1') "
            + "ORDER BY time) ORDER BY frequency_index",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFFTTruncateAndZeroPad() {
    tableResultSetEqualTest(
        "SELECT frequency_index, frequency, speed_real, speed_imag "
            + "FROM FFT(DATA => (SELECT time, speed FROM signal WHERE device_id='d1') "
            + "ORDER BY time, SAMPLE_INTERVAL => 1s, N => 2) ORDER BY frequency_index",
        new String[] {"frequency_index", "frequency", "speed_real", "speed_imag"},
        new String[] {"0,0.0,3.0,0.0,", "1,-0.5,-1.0,0.0,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT frequency_index, frequency, temperature_real, temperature_imag "
            + "FROM FFT(DATA => (SELECT time, temperature FROM signal WHERE device_id='d1') "
            + "ORDER BY time, SAMPLE_INTERVAL => 1s, N => 8) ORDER BY frequency_index",
        new String[] {"frequency_index", "frequency", "temperature_real", "temperature_imag"},
        new String[] {
          "0,0.0,1.0,0.0,",
          "1,0.125,1.0,0.0,",
          "2,0.25,1.0,0.0,",
          "3,0.375,1.0,0.0,",
          "4,-0.5,1.0,0.0,",
          "5,-0.375,1.0,0.0,",
          "6,-0.25,1.0,0.0,",
          "7,-0.125,1.0,0.0,"
        },
        DATABASE_NAME);
  }

  @Test
  public void testFFTNorm() {
    tableResultSetEqualTest(
        "SELECT frequency_index, temperature_real, temperature_imag "
            + "FROM FFT(DATA => (SELECT time, temperature FROM signal WHERE device_id='d1') "
            + "ORDER BY time, SAMPLE_INTERVAL => 1s, N => 4, NORM => 'forward') "
            + "ORDER BY frequency_index",
        new String[] {"frequency_index", "temperature_real", "temperature_imag"},
        new String[] {"0,0.25,0.0,", "1,0.25,0.0,", "2,0.25,0.0,", "3,0.25,0.0,"},
        DATABASE_NAME);

    tableResultSetEqualTest(
        "SELECT frequency_index, temperature_real, temperature_imag "
            + "FROM FFT(DATA => (SELECT time, temperature FROM signal WHERE device_id='d1') "
            + "ORDER BY time, SAMPLE_INTERVAL => 1s, N => 4, NORM => 'ortho') "
            + "ORDER BY frequency_index",
        new String[] {"frequency_index", "temperature_real", "temperature_imag"},
        new String[] {"0,0.5,0.0,", "1,0.5,0.0,", "2,0.5,0.0,", "3,0.5,0.0,"},
        DATABASE_NAME);
  }

  @Test
  public void testFFTFailures() {
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => signal PARTITION BY device_id, SAMPLE_INTERVAL => 1s)",
        "701: Table argument with set semantics requires an ORDER BY clause.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => single_row PARTITION BY device_id ORDER BY time)",
        "701: FFT requires at least two rows to infer SAMPLE_INTERVAL.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => no_numeric PARTITION BY device_id ORDER BY time, SAMPLE_INTERVAL => 1s)",
        "701: No numeric columns found for FFT calculation.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => with_null PARTITION BY device_id ORDER BY time, SAMPLE_INTERVAL => 1s)",
        "701: FFT does not support null values in column [value].",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => irregular PARTITION BY device_id ORDER BY time)",
        "701: FFT requires evenly spaced input time values when SAMPLE_INTERVAL is not specified.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => irregular PARTITION BY device_id ORDER BY time, SAMPLE_INTERVAL => 1s)",
        "701: FFT input time interval must match the specified SAMPLE_INTERVAL.",
        DATABASE_NAME);
    tableAssertTestFail(
        "SELECT * FROM FFT(DATA => signal PARTITION BY device_id ORDER BY time, N => 65537)",
        "701: FFT transform length N must not exceed 65536.",
        DATABASE_NAME);
  }
}
