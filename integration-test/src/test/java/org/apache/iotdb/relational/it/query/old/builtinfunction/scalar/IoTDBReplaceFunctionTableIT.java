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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBReplaceFunctionTableIT {

  private static final String DATABASE_NAME = "db";
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "use " + DATABASE_NAME,
        "create table table1(device_id STRING ID, s1 TEXT MEASUREMENT, s2 INT32 MEASUREMENT, s3 INT64 MEASUREMENT, s4 FLOAT MEASUREMENT, s5 DOUBLE MEASUREMENT, s6 BOOLEAN MEASUREMENT, s7 DATE MEASUREMENT, s8 TIMESTAMP MEASUREMENT, s9 STRING MEASUREMENT)",
        "INSERT INTO table1(time,device_id,s1,s2,s3,s4,s5,s6,s7,s8,s9) values(1, 'd1', 'abcd', 1, 1, 1, 1, true, '2021-10-01', 1633046400000, 'abcd')",
        "INSERT INTO table1(time,device_id,s1) values(2, 'd1', 'test\\\\')",
        "INSERT INTO table1(time,device_id,s1) values(3, 'd1', 'abcd\\\\')",
        "INSERT INTO table1(time,device_id,s9) values(2, 'd1', 'test\\\\')",
        "INSERT INTO table1(time,device_id,s9) values(3, 'd1', 'abcd\\\\')",
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
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNewTransformer() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "_col1", "_col2"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,ABcd,abcd,",
          "1970-01-01T00:00:00.002Z,test\\\\,testaa,",
          "1970-01-01T00:00:00.003Z,ABcd\\\\,abcdaa,"
        };
    tableResultSetEqualTest(
        "select Time, REPLACE(s1, 'ab', 'AB'), REPLACE(s1, '\\', 'a') from table1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    String[] retArray2 =
        new String[] {
          "1970-01-01T00:00:00.001Z,ABcd,abcd,",
          "1970-01-01T00:00:00.002Z,test\\\\,testaa,",
          "1970-01-01T00:00:00.003Z,ABcd\\\\,abcdaa,"
        };
    tableResultSetEqualTest(
        "select Time, REPLACE(s9, 'ab', 'AB'), REPLACE(s9, '\\', 'a') from table1",
        expectedHeader,
        retArray2,
        DATABASE_NAME);
  }

  @Test
  public void testWithoutTo() {
    String[] expectedHeader = new String[] {TIMESTAMP_STR, "_col1"};
    String[] retArray =
        new String[] {
          "1970-01-01T00:00:00.001Z,acd,",
          "1970-01-01T00:00:00.002Z,test\\\\,",
          "1970-01-01T00:00:00.003Z,acd\\\\,"
        };
    tableResultSetEqualTest(
        "select Time, REPLACE(s1, 'b') from table1", expectedHeader, retArray, DATABASE_NAME);
  }

  @Test
  public void testWrongInputType() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      try {
        statement.execute("select REPLACE(s2, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s3, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s4, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s5, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s6, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s7, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      try {
        statement.execute("select REPLACE(s8, 'a', 'b') from table1");
        fail();
      } catch (Exception ignored) {
      }

      //      try {
      //        statement.execute("select REPLACE(s10, 'a', 'b') from table1");
      //        fail();
      //      } catch (Exception ignored) {
      //      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }
}
