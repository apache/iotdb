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
package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBInsertNullIT {
  private static final List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    initCreateSQLStatement();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static void initCreateSQLStatement() {
    sqls.add("CREATE DATABASE root.sg");
    sqls.add("CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=BOOLEAN");
    sqls.add("CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=FLOAT");
    sqls.add("CREATE TIMESERIES root.sg.d1.s3 WITH DATATYPE=INT32");
    sqls.add("CREATE ALIGNED TIMESERIES root.sg.d2(s1 BOOLEAN,s2 FLOAT,s3 INT32)");
  }

  private static void insertData() throws SQLException {
    connection = EnvFactory.getEnv().getConnection();
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }

  @Test
  public void testInsertNull() {
    String[] retArray =
        new String[] {
          "1,null,1.0,1,", "2,true,null,2,", "3,true,3.0,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d1(time,s1,s2,s3) values(1,null,1.0,1)");
      statement.execute("insert into root.sg.d1(time,s1,s2,s3) values(2,true,null,2)");
      statement.execute("insert into root.sg.d1(time,s1,s2,s3) values(3,true,3.0,null)");

      try (ResultSet resultSet = statement.executeQuery("select * from root.sg.d1")) {
        assertNotNull(resultSet);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.sg.d1.s1,root.sg.d1.s2,root.sg.d1.s3",
                new int[] {
                  Types.TIMESTAMP, Types.BOOLEAN, Types.FLOAT, Types.INTEGER,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertAlignedNull() {
    String[] retArray =
        new String[] {
          "1,null,1.0,1,", "2,true,null,2,", "3,true,3.0,null,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d2(time,s1,s2,s3) aligned values(1,null,1.0,1)");
      statement.execute("insert into root.sg.d2(time,s1,s2,s3) aligned values(2,true,null,2)");
      statement.execute("insert into root.sg.d2(time,s1,s2,s3) aligned values(3,true,3.0,null)");

      try (ResultSet resultSet = statement.executeQuery("select * from root.sg.d2")) {
        assertNotNull(resultSet);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,root.sg.d2.s1,root.sg.d2.s2,root.sg.d2.s3",
                new int[] {
                  Types.TIMESTAMP, Types.BOOLEAN, Types.FLOAT, Types.INTEGER,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        assertEquals(3, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      assertNotNull(typeIndex);
      assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }
}
