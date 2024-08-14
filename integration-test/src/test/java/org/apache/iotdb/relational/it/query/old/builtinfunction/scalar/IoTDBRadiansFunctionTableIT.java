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

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.*;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBRadiansFunctionTableIT {
  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO table1(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd', X'abcd')",
        "INSERT INTO table1(Time,device_id,s2) values(2, 'd1',  2)",
        "INSERT INTO table1(Time,device_id,s2) values(3, 'd1',  3)",
        "INSERT INTO table1(Time,device_id,s3) values(2, 'd1',  2)",
        "INSERT INTO table1(Time,device_id,s3) values(3, 'd1',  3)",
        "INSERT INTO table1(Time,device_id,s4) values(2, 'd1',  2.5)",
        "INSERT INTO table1(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO table1(Time,device_id,s5) values(2, 'd1',  2.5)",
        "INSERT INTO table1(Time,device_id,s5) values(3, 'd1',  3.5)",
        "INSERT INTO table1(Time,device_id,s8) values(2, 'd1',  2)",
        "INSERT INTO table1(Time,device_id,s8) values(3, 'd1',  3)",
        "flush",
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
  public void testNormalTransformer() {
    // case 1: support INT32, INT64, FLOAT, DOUBLE, TIMESTAMP
    String[] expectedHeader = new String[] {"time", "_col1", "_col2", "_col3", "_col4", "_col5"};
    int[] expectedBodyInt = new int[] {1, 2, 3};
    long[] expectedBodyLong = new long[] {1, 2, 3};
    float[] expectedBodyFloat = new float[] {1, 2.5f, 3.5f};
    double[] expectedBodyDouble = new double[] {1, 2.5, 3.5};
    long[] expectedBodyTimestamp = new long[] {1633046400000L, 2, 3};
    TestSQLDoubleResult(
        "select time,radians(s2),radians(s3),radians(s4),radians(s5),radians(s8) from table1",
        expectedHeader,
        DATABASE_NAME,
        expectedBodyInt,
        expectedBodyLong,
        expectedBodyFloat,
        expectedBodyDouble,
        expectedBodyTimestamp);
  }

  private void TestSQLDoubleResult(
      String sql,
      String[] expectedHeader,
      String database,
      int[] expectedBodyInt,
      long[] expectedBodyLong,
      float[] expectedBodyFloat,
      double[] expectedBodyDouble,
      long[] expectedBodyTimestamp) {
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", "+00:00");
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + database);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            assertEquals(expectedHeader[i - 1], resultSetMetaData.getColumnName(i));
          }
          assertEquals(expectedHeader.length, resultSetMetaData.getColumnCount());

          int cnt = 0;
          while (resultSet.next()) {
            assertEquals(
                Math.toRadians(expectedBodyInt[cnt]),
                Double.parseDouble(resultSet.getString(2)),
                0.00001);
            assertEquals(
                Math.toRadians(expectedBodyLong[cnt]),
                Double.parseDouble(resultSet.getString(3)),
                0.00001);
            assertEquals(
                Math.toRadians(expectedBodyFloat[cnt]),
                Double.parseDouble(resultSet.getString(4)),
                0.00001);
            assertEquals(
                Math.toRadians(expectedBodyDouble[cnt]),
                Double.parseDouble(resultSet.getString(5)),
                0.00001);
            assertEquals(
                Math.toRadians(expectedBodyTimestamp[cnt]),
                Double.parseDouble(resultSet.getString(6)),
                0.00001);

            for (int i = 1; i < expectedHeader.length; i++) {
              System.out.println(resultSet.getString(i));
            }
            cnt++;
          }
          assertEquals(expectedBodyInt.length, cnt);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testFailTransformer() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,radians(s2,1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,radians(s1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,radians(s6) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,radians(s7) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,radians(s9) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,radians(s10) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function radians only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }
}
