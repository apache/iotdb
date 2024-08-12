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

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBTanFunctionTableIT {
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
        "INSERT INTO table1(Time,device_id,s4) values(2, 'd1',  1.57079632675)",
        "INSERT INTO table1(Time,device_id,s4) values(3, 'd1',  3.5)",
        "INSERT INTO table1(Time,device_id,s5) values(2, 'd1',  1.57079632675)",
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
    String[] expectedHeader =
        new String[] {
          "time", "s2", "_col2", "s3", "_col4", "s4", "_col6", "s5", "_col8", "s8", "_col10"
        };
    String[] expectedAns =
        new String[] {
          "1970-01-01T00:00:00.001Z,1,1.5574077246549023,1,1.5574077246549023,1.0,1.5574077246549023,1.0,1.5574077246549023,2021-10-01T00:00:00.000Z,-0.513113872858981,",
          "1970-01-01T00:00:00.002Z,2,-2.185039863261519,2,-2.185039863261519,1.5707964,-2.287733242885646E7,1.57079632675,2.227340543395435E10,1970-01-01T00:00:00.002Z,-2.185039863261519,",
          "1970-01-01T00:00:00.003Z,3,-0.1425465430742778,3,-0.1425465430742778,3.5,0.3745856401585947,3.5,0.3745856401585947,1970-01-01T00:00:00.003Z,-0.1425465430742778,",
        };
    tableResultSetEqualTest(
        "select time,s2,tan(s2),s3,tan(s3),s4,tan(s4),s5,tan(s5),s8,tan(s8) from table1",
        expectedHeader,
        expectedAns,
        DATABASE_NAME);
  }

  @Test
  public void testFailTransformer() {
    // case 1: more than one argument
    tableAssertTestFail(
        "select s2,tan(s2,1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s1,tan(s1) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s6,tan(s6) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s7,tan(s7) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s9,tan(s9) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s10,tan(s10) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function tan only accepts one argument and it must be TimeStamp, Double, Float, Int32 or Int64 data type.",
        DATABASE_NAME);
  }
}
