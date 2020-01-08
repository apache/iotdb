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

import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBDeleteStorageGroupIT {

  private static IoTDB daemon;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeleteStorageGroup() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
            getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement();) {
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt01");
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt02");
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt03");
      statement.execute("SET STORAGE GROUP TO root.ln.wf01.wt04");
      statement.execute("DELETE STORAGE GROUP root.ln.wf01.wt01");
      boolean hasResult = statement.execute("SHOW STORAGE GROUP");
      assertTrue(hasResult);
      String[] expected = new String[]{
              "root.ln.wf01.wt02",
              "root.ln.wf01.wt03",
              "root.ln.wf01.wt04"
      };
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      ResultSet resultSet = statement.getResultSet();
      List<String> result = new ArrayList<>();
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  /**
   * Star is not allowed in delete storage group statement
   *
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  @Test(expected = IoTDBSQLException.class)
  public void testDeleteStorageGroupWithStar() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
            getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement();) {
      statement.execute("SET STORAGE GROUP TO root.ln1.wf01.wt01");
      statement.execute("SET STORAGE GROUP TO root.ln1.wf01.wt02");
      statement.execute("SET STORAGE GROUP TO root.ln1.wf02.wt03");
      statement.execute("SET STORAGE GROUP TO root.ln1.wf02.wt04");
      statement.execute("DELETE STORAGE GROUP root.ln1.wf02.*");
    }
  }

  @Test
  public void testDeleteMultipleStorageGroupWithQuote() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement();) {
      statement.execute("SET STORAGE GROUP TO root.ln2.wf01.wt01");
      statement.execute("SET STORAGE GROUP TO root.ln2.wf01.wt02");
      statement.execute("SET STORAGE GROUP TO root.ln2.wf02.wt03");
      statement.execute("SET STORAGE GROUP TO root.ln2.wf02.wt04");
      statement.execute("DELETE STORAGE GROUP root.ln2.wf01.wt01, root.ln2.wf02.wt03");
      boolean hasResult = statement.execute("SHOW STORAGE GROUP");
      assertTrue(hasResult);
      String[] expected = new String[]{
              "root.ln2.wf01.wt02",
              "root.ln2.wf02.wt04"
      };
      List<String> expectedList = new ArrayList<>();
      Collections.addAll(expectedList, expected);
      ResultSet resultSet = statement.getResultSet();
      List<String> result = new ArrayList<>();
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      assertEquals(expected.length, result.size());
      assertTrue(expectedList.containsAll(result));
    }
  }

  @Test(expected = IoTDBSQLException.class)
  public void testCreateTimeseriesInDeletedStorageGroup() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
            getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement();) {
      statement.execute("SET STORAGE GROUP TO root.ln3.wf01.wt01");
      statement.execute("DELETE STORAGE GROUP root.ln3.wf01.wt01");
      statement.execute("CREATE TIMESERIES root.ln3.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    }
  }

  @Test(expected = IoTDBSQLException.class)
  public void deleteNonExistStorageGroup() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection = DriverManager.
            getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement();) {
      statement.execute("SET STORAGE GROUP TO root.ln4.wf01.wt01");
      statement.execute("DELETE STORAGE GROUP root.ln4.wf01.wt02");
    }
  }
}
