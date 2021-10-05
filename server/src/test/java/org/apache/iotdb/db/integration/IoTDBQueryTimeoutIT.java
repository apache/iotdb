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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.jdbc.IoTDBStatement;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class IoTDBQueryTimeoutIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    QueryResourceManager.getInstance().endQuery(EnvironmentUtils.TEST_QUERY_JOB_ID);
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /** Test show query processlist, there is supposed to no result. */
  @Test
  public void queryProcessListTest() {
    String headerResult = "Time, queryId, statement, ";

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("show query processlist");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      ResultSetMetaData metaData = resultSet.getMetaData();
      Assert.assertEquals(3, metaData.getColumnCount());
      StringBuilder headerBuilder = new StringBuilder();
      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        headerBuilder.append(metaData.getColumnName(i)).append(", ");
      }
      Assert.assertEquals(headerResult, headerBuilder.toString());

      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(1, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test query with timeout, which is supposed to throw an QueryTimeoutRuntimeException. Note: This
   * test is not guaranteed to time out.
   */
  @Test
  public void queryWithTimeoutTest() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.setFetchSize(40000);
      try {
        ((IoTDBStatement) statement)
            .executeQuery("select count(*) from root group by ([1, 80000), 2ms)", 1);
      } catch (IoTDBSQLException e) {
        Assert.assertTrue(e.getMessage().contains("Current query is time out"));
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test executing query after a timeout query, it's supposed to execute correctly. */
  @Test
  public void queryAfterTimeoutQueryTest() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.setFetchSize(40000);
      try {
        ((IoTDBStatement) statement)
            .executeQuery("select count(*) from root group by ([1, 80000), 2ms)", 1);
      } catch (IoTDBSQLException e) {
        Assert.assertTrue(e.getMessage().contains("Current query is time out"));
      }

      Boolean hasResultSet = statement.execute("select max_time(s1) from root.sg1.d1");
      Assert.assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        Assert.assertEquals(80000, resultSet.getLong("max_time(root.sg1.d1.s1)"));
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (int i = 0; i <= 80000; i++) {
        statement.execute(String.format("insert into root.sg1.d1(time,s1) values(%d,%d)", i, i));
        statement.execute(String.format("insert into root.sg2.d2(time,s2) values(%d,%d)", i, i));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
