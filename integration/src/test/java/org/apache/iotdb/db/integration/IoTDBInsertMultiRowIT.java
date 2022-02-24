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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @Author: Architect @Date: 2021-03-30 18:36 @Description: This class is initially intend to test
 * the issue of IOTDB-924
 */
@Category({LocalStandaloneTest.class})
public class IoTDBInsertMultiRowIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    initCreateSQLStatement();
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
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
    sqls.add("SET STORAGE GROUP TO root.t1");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }

  @Test
  public void testInsertMultiRow() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute("insert into root.t1.wf01.wt01(timestamp, status) values (1, true)");
    st0.execute("insert into root.t1.wf01.wt01(timestamp, status) values (2, true),(3, false)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, status) values (4, true),(5, true),(6, false)");

    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, temperature, status) values (7, 15.3, true)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, temperature, status) values (8, 18.3, false),(9, 23.1, false)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, temperature, status) values (10, 22.3, true),(11, 18.8, false),(12, 24.4, true)");
    st0.close();

    Statement st1 = connection.createStatement();
    ResultSet rs1 = st1.executeQuery("select count(status) from root.t1.wf01.wt01");
    rs1.next();
    long countStatus = rs1.getLong(1);
    Assert.assertEquals(countStatus, 12L);

    ResultSet rs2 = st1.executeQuery("select count(temperature) from root.t1.wf01.wt01");
    rs2.next();
    long countTemperature = rs2.getLong(1);
    Assert.assertEquals(countTemperature, 6L);

    st1.close();
  }

  @Test(expected = Exception.class)
  public void testInsertWithTimesColumns() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(timestamp) values(1)");
  }

  @Test
  public void testInsertMultiRowWithMisMatchDataType() {
    try {
      Statement st1 = connection.createStatement();
      st1.execute(
          "insert into root.t1.wf01.wt01(timestamp, s1) values(1, 1.0) (2, 'hello'), (3, true)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains(Integer.toString(TSStatusCode.MULTIPLE_ERROR.getStatusCode())));
    }
  }
}
