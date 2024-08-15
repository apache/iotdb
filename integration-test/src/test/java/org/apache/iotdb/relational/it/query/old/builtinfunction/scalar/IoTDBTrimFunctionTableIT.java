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
public class IoTDBTrimFunctionTableIT {
  private static final String DATABASE_NAME = "db";

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT, s10 BLOB MEASUREMENT)",
        "INSERT INTO table1(Time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'ab', X'abcd')",
        "INSERT INTO table1(Time,device_id,s1) values(2, 'd1', 'xyTestxy')",
        "INSERT INTO table1(Time,device_id,s1) values(3, 'd1', '  Test  ')",
        "INSERT INTO table1(Time,device_id,s9) values(2, 'd1', 'xy')",
        "INSERT INTO table1(Time,device_id,s9) values(3, 'd1', ' T')",
        "flush"
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
    // support the trim(trimSource) trim(specification From trimSource)
    String[] expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,abcd,ab,ab,",
          "1970-01-01T00:00:00.002Z,xyTestxy,xyTestxy,xy,xy,",
          "1970-01-01T00:00:00.003Z,  Test  ,Test  , T,T,",
        };
    tableResultSetEqualTest(
        "select time,s1,trim(LEADING FROM s1),s9,trim(s9) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the trim(trimSource, trimChar) trim(trimChar From trimSource)
    expectedHeader = new String[] {"time", "s1", "_col2", "s9", "_col4"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,bc,ab,b,",
          "1970-01-01T00:00:00.002Z,xyTestxy,yTestxy,xy,y,",
          "1970-01-01T00:00:00.003Z,  Test  ,  Test  , T, T,",
        };
    tableResultSetEqualTest(
        "select time,s1,trim(s1, 'axd'),s9,trim('ax' FROM s9) from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // support the trim(trimSpecification trimChar From trimSource)
    expectedHeader = new String[] {"time", "s1", "s9", "_col3", "_col4"};
    retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,abcd,ab,cd,abcd,",
          "1970-01-01T00:00:00.002Z,xyTestxy,xy,Test,xyTest,",
          "1970-01-01T00:00:00.003Z,  Test  , T,est,  Test,",
        };
    tableResultSetEqualTest(
        "select time,s1,s9,trim(BOTH s9 FROM s1), trim(TRAILING s9 FROM s1)from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testFailTransformer() {
    // case 1: wrong data type
    tableAssertTestFail(
        "select s2,trim(s2, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 2: wrong data type
    tableAssertTestFail(
        "select s3,trim(s3, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 3: wrong data type
    tableAssertTestFail(
        "select s4,trim(s4, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 4: wrong data type
    tableAssertTestFail(
        "select s5,trim(s5, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 5: wrong data type
    tableAssertTestFail(
        "select s6,trim(s6, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 6: wrong data type
    tableAssertTestFail(
        "select s7,trim(s7, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 7: wrong data type
    tableAssertTestFail(
        "select s8,trim(s8, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);

    // case 8: wrong data type
    tableAssertTestFail(
        "select s10,trim(s10, 'es') from table1",
        TSStatusCode.SEMANTIC_ERROR.getStatusCode()
            + ": Scalar function trim only accepts one or two arguments and they must be text or string data type.",
        DATABASE_NAME);
  }
}
