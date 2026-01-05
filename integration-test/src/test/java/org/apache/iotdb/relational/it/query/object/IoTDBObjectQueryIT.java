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

package org.apache.iotdb.relational.it.query.object;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.apache.iotdb.jdbc.IoTDBJDBCResultSet.OBJECT_ERR_MSG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBObjectQueryIT {

  private static final String DATABASE_NAME = "test_db";

  private static final String TIME_ZONE = "+00:00";

  private static final String[] createSqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE t1(device_id STRING TAG, o1 OBJECT, b1 BLOB, s1 STRING, l1 INT64, l2 INT64)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(1, 'd1', X'cafebabe01', to_object(true, 0, X'cafebabe01'), 'cafebabe01', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(2, 'd1', X'cafebabe0202', to_object(true, 0, X'cafebabe02'), 'cafebabe02', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(3, 'd1', X'cafebabe0303', to_object(true, 0, X'cafebabe03'), 'cafebabe03', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(4, 'd1', X'cafebabe04', to_object(true, 0, X'cafebabe04'), 'cafebabe04', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(1, 'd2', X'cafebade01', to_object(true, 0, X'cafebade01'), 'cafebade01', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(2, 'd2', X'cafebade0202', to_object(true, 0, X'cafebade02'), 'cafebade02', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(3, 'd2', X'cafebade0302', to_object(true, 0, X'cafebade03'), 'cafebade03', 0, 100)",
        "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(4, 'd2', X'cafebade04', to_object(true, 0, X'cafebade04'), 'cafebade04', 0, 100)",
        "FLUSH",
      };

  @BeforeClass
  public static void classSetUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareTableData(createSqls);
  }

  @AfterClass
  public static void classTearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void jdbcTest() {
    try (Connection connection =
        EnvFactory.getEnv()
            .getConnection(
                SessionConfig.DEFAULT_USER,
                SessionConfig.DEFAULT_PASSWORD,
                BaseEnv.TABLE_SQL_DIALECT)) {
      connection.setClientInfo("time_zone", TIME_ZONE);
      try (Statement statement = connection.createStatement()) {
        statement.execute("use " + DATABASE_NAME);
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, o1, s1 FROM t1 WHERE device_id = 'd1' ORDER BY time")) {
          int cnt = 0;
          while (resultSet.next()) {
            cnt++;
            try {
              resultSet.getBlob(3);
              fail();
            } catch (SQLException e) {
              assertEquals(OBJECT_ERR_MSG, e.getMessage());
            }

            try {
              resultSet.getBytes("o1");
              fail();
            } catch (SQLException e) {
              assertEquals(OBJECT_ERR_MSG, e.getMessage());
            }

            String s = resultSet.getString(3);
            assertEquals("(Object) 5 B", s);
          }
          assertEquals(4, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, READ_OBJECT(o1), s1 FROM t1 WHERE device_id = 'd2' AND READ_OBJECT(o1)=b1 ORDER BY time")) {
          int cnt = 0;
          String[] ans = {"0xcafebade01", "0xcafebade04"};
          while (resultSet.next()) {
            String s = resultSet.getString(3);
            assertEquals(ans[cnt], s);
            cnt++;
          }
          assertEquals(2, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, READ_OBJECT(o1, 0, -1), s1 FROM t1 WHERE device_id = 'd2' AND READ_OBJECT(o1)=b1 ORDER BY time")) {
          int cnt = 0;
          String[] ans = {"0xcafebade01", "0xcafebade04"};
          while (resultSet.next()) {
            String s = resultSet.getString(3);
            assertEquals(ans[cnt], s);
            cnt++;
          }
          assertEquals(2, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, READ_OBJECT(o1, l1), s1 FROM t1 WHERE device_id = 'd2' AND READ_OBJECT(o1)=b1 ORDER BY time")) {
          int cnt = 0;
          String[] ans = {"0xcafebade01", "0xcafebade04"};
          while (resultSet.next()) {
            String s = resultSet.getString(3);
            assertEquals(ans[cnt], s);
            cnt++;
          }
          assertEquals(2, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, READ_OBJECT(o1, l1, l2), s1 FROM t1 WHERE device_id = 'd2' AND READ_OBJECT(o1)=b1 ORDER BY time")) {
          int cnt = 0;
          String[] ans = {"0xcafebade01", "0xcafebade04"};
          while (resultSet.next()) {
            String s = resultSet.getString(3);
            assertEquals(ans[cnt], s);
            cnt++;
          }
          assertEquals(2, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, READ_OBJECT(o1, l1, -1), s1 FROM t1 WHERE device_id = 'd2' AND READ_OBJECT(o1)=b1 ORDER BY time")) {
          int cnt = 0;
          String[] ans = {"0xcafebade01", "0xcafebade04"};
          while (resultSet.next()) {
            String s = resultSet.getString(3);
            assertEquals(ans[cnt], s);
            cnt++;
          }
          assertEquals(2, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, READ_OBJECT(o1, 0, l2), s1 FROM t1 WHERE device_id = 'd2' AND READ_OBJECT(o1)=b1 ORDER BY time")) {
          int cnt = 0;
          String[] ans = {"0xcafebade01", "0xcafebade04"};
          while (resultSet.next()) {
            String s = resultSet.getString(3);
            assertEquals(ans[cnt], s);
            cnt++;
          }
          assertEquals(2, cnt);
        }

        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT time, b1, o1, s1 FROM t1 WHERE device_id = 'd1' FILL METHOD LINEAR")) {
          int cnt = 0;
          while (resultSet.next()) {
            cnt++;
            String s = resultSet.getString(3);
            assertEquals("(Object) 5 B", s);
          }
          assertEquals(4, cnt);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void sessionTest() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);

      // SessionDataSet
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT time, b1, o1, s1 FROM t1 WHERE device_id = 'd1' ORDER BY time")) {
        int cnt = 0;
        while (dataSet.hasNext()) {
          cnt++;
          RowRecord rowRecord = dataSet.next();
          Field field = rowRecord.getField(2);
          String s = field.getStringValue();
          assertEquals("(Object) 5 B", s);
          Object blob = field.getObjectValue(TSDataType.OBJECT);
          assertTrue(blob instanceof String);
          assertEquals("(Object) 5 B", blob);

          try {
            field.getBinaryV();
            fail();
          } catch (UnsupportedOperationException e) {
            assertEquals("OBJECT Type only support getStringValue", e.getMessage());
          }
        }
        assertEquals(4, cnt);
      }

      // SessionDataSet.DataIterator
      try (SessionDataSet dataSet =
          session.executeQueryStatement(
              "SELECT time, b1, o1, s1 FROM t1 WHERE device_id = 'd2' ORDER BY time")) {
        SessionDataSet.DataIterator iterator = dataSet.iterator();
        int cnt = 0;
        while (iterator.next()) {
          cnt++;
          Object o = iterator.getObject(3);
          assertTrue(o instanceof String);
          assertEquals("(Object) 5 B", o);
          String s = iterator.getString("o1");
          assertEquals("(Object) 5 B", s);
          try {
            iterator.getBlob(3);
            fail();
          } catch (StatementExecutionException e) {
            assertEquals("OBJECT Type only support getString", e.getMessage());
          }
        }
        assertEquals(4, cnt);
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIllegalObjectValue() {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE " + DATABASE_NAME);
      try {
        session.executeNonQueryStatement(
            "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(1, 'd1', X'cafebabe01', 1, 'cafebabe01', 0, 100)");
        fail();
      } catch (StatementExecutionException e) {
        Assert.assertTrue(e.getMessage().contains("data type is not consistent"));
      }

      try {
        session.executeNonQueryStatement(
            "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(1, 'd1', X'cafebabe01', 'test', 'cafebabe01', 0, 100)");
        fail();
      } catch (StatementExecutionException e) {
        Assert.assertTrue(e.getMessage().contains("data type is not consistent"));
      }

      try {
        session.executeNonQueryStatement(
            "INSERT INTO t1(time, device_id, b1, o1, s1, l1, l2) VALUES(1, 'd1', X'cafebabe01', X'cafebabe01', 'cafebabe01', 0, 100)");
      } catch (StatementExecutionException e) {
        Assert.assertTrue(e.getMessage().contains("data type is not consistent"));
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      fail(e.getMessage());
    }
  }
}
