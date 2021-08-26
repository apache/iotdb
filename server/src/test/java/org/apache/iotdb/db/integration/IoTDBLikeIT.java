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
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IoTDBLikeIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
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
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=TEXT, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465600000,'1',12.1)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465660000,'14',13.1)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465720000,'616',5.5)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465780000,'626',8.1)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465840000,'6116',4.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465900000,'6%16',10.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509465960000,'8[sS]*',11.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509466020000,'%123',18.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509466080000,'123%',18.3)");
    sqls.add(
        "insert into root.t1.wf01.wt01 (time,status,temperature) values (1509466090000,'\\',10.3)");
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
  public void testLike() throws SQLException {
    Statement st0 = connection.createStatement();
    boolean hasResultSet =
        st0.execute("select status from root.t1.wf01.wt01 where status like '1'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("1", outputResultStr(st0.getResultSet()));
    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals(
        "1,14,616,626,6116,6%16,8[sS]*,%123,123%,\\", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '1%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("1,14,123%", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '%1%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("1,14,616,6116,6%16,%123,123%", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '6%6'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("616,626,6116,6%16", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '1_'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("14", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '6_1%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("6116,6%16", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '6\\%%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("6%16", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '\\%%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("%123", outputResultStr(st0.getResultSet()));

    hasResultSet = st0.execute("select status from root.t1.wf01.wt01 where status like '%\\%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("123%", outputResultStr(st0.getResultSet()));
    hasResultSet =
        st0.execute("select status from root.t1.wf01.wt01 where status like '%\\\\\\\\%'");
    Assert.assertTrue(hasResultSet);
    Assert.assertEquals("\\", outputResultStr(st0.getResultSet()));
  }

  @Test(expected = Exception.class)
  public void testLikeNonTextCloumn() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("select * from root.t1.wf01.wt01 where temperature like '1'");
  }

  private String outputResultStr(ResultSet resultSet) throws SQLException {
    StringBuilder resultBuilder = new StringBuilder();
    while (resultSet.next()) {
      resultBuilder.append(resultSet.getString(2)).append(",");
    }
    String result = resultBuilder.toString();
    return result.substring(0, result.length() - 1);
  }
}
