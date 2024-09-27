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

package org.apache.iotdb.relational.it.query.old.aligned;

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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.relational.it.query.old.aligned.TableUtils.USE_DB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBAlignedOffsetLimitPushDownTableIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE IF NOT EXISTS db");
      statement.execute(USE_DB);
      statement.execute(
          "CREATE TABLE table0 (device string id, s1 double measurement, s2 double measurement)");

      statement.execute("insert into table0(device,time,s1,s2) values('d1',1,1,1)");
      statement.execute("insert into table0(device,time,s1,s2) values('d1',2,2,2)");
      statement.execute("insert into table0(device,time,s1,s2) values('d1',3,3,3)");
      statement.execute("insert into table0(device,time,s1,s2) values('d1',4,4,4)");
      statement.execute("insert into table0(device,time,s1,s2) values('d1',5,5,null)");
      statement.execute("insert into table0(device,time,s1,s2) values('d1',6,6,null)");
      statement.execute("insert into table0(device,time,s1,s2) values('d1',7,7,7)");

      statement.execute("flush");
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
  public void selectWithLimitPushDownTest1() {

    String[] retArray = new String[] {"3,3.0", "4,4.0"};

    String[] columnNames = {"s1"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
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

  @Test
  public void selectWithLimitPushDownTest2() {

    String[] retArray = new String[] {"6,6.0,null", "7,7.0,7.0"};

    String[] columnNames = {"s1", "s2"};

    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(USE_DB);
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
