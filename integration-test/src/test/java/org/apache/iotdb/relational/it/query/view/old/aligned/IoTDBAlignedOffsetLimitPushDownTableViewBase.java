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

package org.apache.iotdb.relational.it.query.view.old.aligned;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class IoTDBAlignedOffsetLimitPushDownTableViewBase {
  private static final String USE_VIEW_DB = "USE db_view";

  protected static void prepareViewData() throws Exception {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(1,1,1)");
      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(2,2,2)");
      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(3,3,3)");
      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(4,4,4)");
      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(5,5,null)");
      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(6,6,null)");
      statement.addBatch("insert into root.db.d1(time,s1,s2) aligned values(7,7,7)");

      statement.addBatch("flush");

      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS db_view");
      statement.execute(USE_VIEW_DB);
      statement.execute(
          "CREATE VIEW table0 (device string tag, s1 double field, s2 double field) as root.db.**");

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Checks LIMIT pushdown after a time predicate returns rows 3 and 4 through a view.
  @Test
  public void viewSelectWithLimitPushDownTest1() {

    String[] retArray = new String[] {"3,3.0", "4,4.0"};

    String[] columnNames = {"s1"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_VIEW_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1 from table0 where device='d1' and time >= 3 limit 2")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // Checks OFFSET pushdown returns the remaining rows, including a null field, through a view.
  @Test
  public void viewSelectWithLimitPushDownTest2() {

    String[] retArray = new String[] {"6,6.0,null", "7,7.0,7.0"};

    String[] columnNames = {"s1", "s2"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_VIEW_DB);
      try (ResultSet resultSet =
          statement.executeQuery(
              "select Time,s1,s2 from table0 where device='d1' and time >= 1 offset 5")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          map.put(resultSetMetaData.getColumnName(i), i);
        }
        assertEquals(columnNames.length + 1, resultSetMetaData.getColumnCount());
        int cnt = 0;
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          builder.append(resultSet.getLong(1));
          for (String columnName : columnNames) {
            int index = map.get(columnName);
            builder.append(",").append(resultSet.getString(index));
          }
          assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
