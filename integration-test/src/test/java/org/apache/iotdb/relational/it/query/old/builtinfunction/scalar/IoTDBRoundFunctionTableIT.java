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
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
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
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRoundFunctionTableIT {

  private static final String DATABASE_NAME = "db";

  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 INT32 MEASUREMENT, s2 INT64 MEASUREMENT, s3 DOUBLE MEASUREMENT, s4 FLOAT MEASUREMENT, s5 BOOLEAN MEASUREMENT, s6 STRING MEASUREMENT)",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(1, 'd1', 2, 3, 0.11234, 101.143445345,true,null)",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(2, 'd1', 2, 4, 10.11234, 20.1443465345,true,'sss')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(3, 'd1', 2, 555, 120.161234, 20.61437245345,true,'sss')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(4, 'd1', 2, 12341234, 101.131234, null,true,'sss')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(5, 'd1', 2, 55678, 90.116234, 20.8143454345,true,'sss')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(6, 'd1', 2, 12355, null, 60.71443345345,true,'sss')",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6) values(1678695379764, 'd1', 2, 12345, 120.511234, 10.143425345,null,'sss')",
        "flush"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
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
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testRound() {
    String[] expectedHeader = new String[] {"time", "_col1"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,2.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,2.0,",
          "1970-01-01T00:00:00.004Z,2.0,",
          "1970-01-01T00:00:00.005Z,2.0,",
          "1970-01-01T00:00:00.006Z,2.0,",
          "2023-03-13T08:16:19.764Z,2.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,3.0,",
          "1970-01-01T00:00:00.002Z,4.0,",
          "1970-01-01T00:00:00.003Z,555.0,",
          "1970-01-01T00:00:00.004Z,1.2341234E7,",
          "1970-01-01T00:00:00.005Z,55678.0,",
          "1970-01-01T00:00:00.006Z,12355.0,",
          "2023-03-13T08:16:19.764Z,12345.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s2) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,10.0,",
          "1970-01-01T00:00:00.003Z,120.0,",
          "1970-01-01T00:00:00.004Z,101.0,",
          "1970-01-01T00:00:00.005Z,90.0,",
          "1970-01-01T00:00:00.006Z,null,",
          "2023-03-13T08:16:19.764Z,121.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s3) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,101.0,",
          "1970-01-01T00:00:00.002Z,20.0,",
          "1970-01-01T00:00:00.003Z,21.0,",
          "1970-01-01T00:00:00.004Z,null,",
          "1970-01-01T00:00:00.005Z,21.0,",
          "1970-01-01T00:00:00.006Z,61.0,",
          "2023-03-13T08:16:19.764Z,10.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s4) from table1", expectedHeader, intRetArray, DATABASE_NAME);
  }

  @Test
  public void testRoundWithPlaces() {
    String[] expectedHeader = new String[] {"time", "_col1"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,2.0,",
          "1970-01-01T00:00:00.002Z,2.0,",
          "1970-01-01T00:00:00.003Z,2.0,",
          "1970-01-01T00:00:00.004Z,2.0,",
          "1970-01-01T00:00:00.005Z,2.0,",
          "1970-01-01T00:00:00.006Z,2.0,",
          "2023-03-13T08:16:19.764Z,2.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s1, 1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,3.0,",
          "1970-01-01T00:00:00.002Z,4.0,",
          "1970-01-01T00:00:00.003Z,555.0,",
          "1970-01-01T00:00:00.004Z,1.2341234E7,",
          "1970-01-01T00:00:00.005Z,55678.0,",
          "1970-01-01T00:00:00.006Z,12355.0,",
          "2023-03-13T08:16:19.764Z,12345.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s2, 1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.1,",
          "1970-01-01T00:00:00.002Z,10.1,",
          "1970-01-01T00:00:00.003Z,120.2,",
          "1970-01-01T00:00:00.004Z,101.1,",
          "1970-01-01T00:00:00.005Z,90.1,",
          "1970-01-01T00:00:00.006Z,null,",
          "2023-03-13T08:16:19.764Z,120.5,"
        };
    tableResultSetEqualTest(
        "select time, round(s3, 1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.11,",
          "1970-01-01T00:00:00.002Z,10.11,",
          "1970-01-01T00:00:00.003Z,120.16,",
          "1970-01-01T00:00:00.004Z,101.13,",
          "1970-01-01T00:00:00.005Z,90.12,",
          "1970-01-01T00:00:00.006Z,null,",
          "2023-03-13T08:16:19.764Z,120.51,"
        };
    tableResultSetEqualTest(
        "select time, round(s3, 2) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.112,",
          "1970-01-01T00:00:00.002Z,10.112,",
          "1970-01-01T00:00:00.003Z,120.161,",
          "1970-01-01T00:00:00.004Z,101.131,",
          "1970-01-01T00:00:00.005Z,90.116,",
          "1970-01-01T00:00:00.006Z,null,",
          "2023-03-13T08:16:19.764Z,120.511,"
        };
    tableResultSetEqualTest(
        "select time, round(s3, 3) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.1123,",
          "1970-01-01T00:00:00.002Z,10.1123,",
          "1970-01-01T00:00:00.003Z,120.1612,",
          "1970-01-01T00:00:00.004Z,101.1312,",
          "1970-01-01T00:00:00.005Z,90.1162,",
          "1970-01-01T00:00:00.006Z,null,",
          "2023-03-13T08:16:19.764Z,120.5112,"
        };
    tableResultSetEqualTest(
        "select time, round(s3, 4) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,101.14345,",
          "1970-01-01T00:00:00.002Z,20.14435,",
          "1970-01-01T00:00:00.003Z,20.61437,",
          "1970-01-01T00:00:00.004Z,null,",
          "1970-01-01T00:00:00.005Z,20.81435,",
          "1970-01-01T00:00:00.006Z,60.71443,",
          "2023-03-13T08:16:19.764Z,10.14342,"
        };
    tableResultSetEqualTest(
        "select time, round(s4, 5) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,101.1,",
          "1970-01-01T00:00:00.002Z,20.1,",
          "1970-01-01T00:00:00.003Z,20.6,",
          "1970-01-01T00:00:00.004Z,null,",
          "1970-01-01T00:00:00.005Z,20.8,",
          "1970-01-01T00:00:00.006Z,60.7,",
          "2023-03-13T08:16:19.764Z,10.1,"
        };
    tableResultSetEqualTest(
        "select time, round(s4, 1) from table1", expectedHeader, intRetArray, DATABASE_NAME);
  }

  @Test
  public void testRoundWithNegativePlaces() {

    String[] expectedHeader = new String[] {"time", "_col1"};
    String[] intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,0.0,",
          "1970-01-01T00:00:00.003Z,0.0,",
          "1970-01-01T00:00:00.004Z,0.0,",
          "1970-01-01T00:00:00.005Z,0.0,",
          "1970-01-01T00:00:00.006Z,0.0,",
          "2023-03-13T08:16:19.764Z,0.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s1, -1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,0.0,",
          "1970-01-01T00:00:00.003Z,560.0,",
          "1970-01-01T00:00:00.004Z,1.234123E7,",
          "1970-01-01T00:00:00.005Z,55680.0,",
          "1970-01-01T00:00:00.006Z,12360.0,",
          "2023-03-13T08:16:19.764Z,12340.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s2, -1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,0.0,",
          "1970-01-01T00:00:00.002Z,10.0,",
          "1970-01-01T00:00:00.003Z,120.0,",
          "1970-01-01T00:00:00.004Z,100.0,",
          "1970-01-01T00:00:00.005Z,90.0,",
          "1970-01-01T00:00:00.006Z,null,",
          "2023-03-13T08:16:19.764Z,120.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s3,-1) from table1", expectedHeader, intRetArray, DATABASE_NAME);

    intRetArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,100.0,",
          "1970-01-01T00:00:00.002Z,20.0,",
          "1970-01-01T00:00:00.003Z,20.0,",
          "1970-01-01T00:00:00.004Z,null,",
          "1970-01-01T00:00:00.005Z,20.0,",
          "1970-01-01T00:00:00.006Z,60.0,",
          "2023-03-13T08:16:19.764Z,10.0,"
        };
    tableResultSetEqualTest(
        "select time, round(s4, -1) from table1", expectedHeader, intRetArray, DATABASE_NAME);
  }

  @Test
  public void testRoundBooleanAndText() {
    tableAssertTestFail(
        "select round(s5) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function round only supports two numeric data types [INT32, INT64, FLOAT, DOUBLE]",
        DATABASE_NAME);

    tableAssertTestFail(
        "select round(s6) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function round only supports two numeric data types [INT32, INT64, FLOAT, DOUBLE]",
        DATABASE_NAME);

    tableAssertTestFail(
        "select round(time) from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function round only supports two numeric data types [INT32, INT64, FLOAT, DOUBLE]",
        DATABASE_NAME);
  }
}
