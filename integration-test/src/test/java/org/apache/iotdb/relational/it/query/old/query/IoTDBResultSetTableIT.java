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

package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.db.it.utils.TestUtils.tableAssertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBResultSetTableIT {
  private static final String DATABASE_NAME = "test";
  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE t1(device STRING ID, status BOOLEAN MEASUREMENT, temperature FLOAT MEASUREMENT, type INT32 MEASUREMENT, grade INT64 MEASUREMENT)",
        "CREATE TABLE sg(device STRING ID, status FLOAT MEASUREMENT)",
      };

  private static final String[] emptyResultSet = new String[] {};

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void intAndLongConversionTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "insert into t1(time, device, status, type, grade) values (1000, 'wt01', true, 1, 1000)");
      statement.execute(
          "insert into t1(time, device, status, type, grade) values (2000, 'wt01', false, 2, 2000)");

      // TODO
      /*try (ResultSet resultSet1 =
                statement.executeQuery("select count(status) from t1")) {
              resultSet1.next();
              // type of r1 is INT64(long), test long convert to int
              int countStatus = resultSet1.getInt(1);
              Assert.assertEquals(2L, countStatus);
            }
      */
      try (ResultSet resultSet2 =
          statement.executeQuery("select type from t1 where time = 1000 limit 1")) {
        resultSet2.next();
        // type of r2 is INT32(int), test int convert to long
        long type = resultSet2.getLong(1);
        Assert.assertEquals(1, type);
      }

      try (ResultSet resultSet3 =
          statement.executeQuery("select grade from t1 where time = 1000 limit 1")) {
        resultSet3.next();
        // type of r3 is INT64(long), test long convert to int
        int grade = resultSet3.getInt(1);
        Assert.assertEquals(1000, grade);
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void columnTypeTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);

      try (ResultSet resultSet = statement.executeQuery("select * from sg")) {
        Assert.assertTrue(!resultSet.next());
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(3, metaData.getColumnCount());
        assertEquals("time", metaData.getColumnName(1));
        assertEquals(Types.TIMESTAMP, metaData.getColumnType(1));
        assertEquals("TIMESTAMP", metaData.getColumnTypeName(1));

        assertEquals("device", metaData.getColumnName(2));
        assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        assertEquals("STRING", metaData.getColumnTypeName(2));

        assertEquals("status", metaData.getColumnName(3));
        assertEquals(Types.FLOAT, metaData.getColumnType(3));
        assertEquals("FLOAT", metaData.getColumnTypeName(3));
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void emptyQueryTest1() {
    tableAssertTestFail("select * from sg1", "701: Table 'test.sg1' does not exist", DATABASE_NAME);
  }

  @Test
  public void emptyQueryTest2() {
    String[] expectedHeader =
        new String[] {"time", "device", "status", "temperature", "type", "grade"};
    tableResultSetEqualTest(
        "select * from t1 where false", expectedHeader, emptyResultSet, DATABASE_NAME);
  }

  @Test
  public void timeWasNullTest() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.execute(
          "CREATE TABLE test(device STRING ID, s1 INT64 MEASUREMENT, s2 INT64 MEASUREMENT, s3 INT64 MEASUREMENT)");

      for (int i = 0; i < 10; i++) {
        statement.addBatch(
            "insert into test(time, device, s1, s2) values(" + i + ",'d1'," + 1 + "," + 1 + ")");
      }

      statement.execute("insert into test(time, device, s3) values(103,'d1',1)");
      statement.execute("insert into test(time, device, s3) values(104,'d1',1)");
      statement.execute("insert into test(time, device, s3) values(105,'d1',1)");
      statement.executeBatch();
      try (ResultSet resultSet = statement.executeQuery("select * from test")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
          for (int i = 1; i <= columnCount; i++) {
            int ct = metaData.getColumnType(i);
            if (ct == Types.TIMESTAMP) {
              if (resultSet.wasNull()) {
                fail();
              }
            }
          }
        }
      }
    }
  }

  @Ignore // TODO
  @Test
  public void emptyLastQueryTest() {
    String expectedHeader =
        "time"
            + ","
            + ColumnHeaderConstant.TIMESERIES
            + ","
            + ColumnHeaderConstant.VALUE
            + ","
            + ColumnHeaderConstant.DATATYPE
            + ",";
    resultSetEqualTest("select last s1 from sg", expectedHeader, emptyResultSet);
  }
}
