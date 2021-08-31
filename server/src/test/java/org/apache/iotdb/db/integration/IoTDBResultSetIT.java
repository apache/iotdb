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

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 27/9/20 22:56
 */
public class IoTDBResultSetIT {
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
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.type WITH DATATYPE=INT32, ENCODING=RLE");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.grade WITH DATATYPE=INT64, ENCODING=RLE");
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
  public void testIntAndLongConversion() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, status, type, grade) values (1000, true, 1, 1000)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, status, type, grade) values (2000, false, 2, 2000)");
    st0.close();

    Statement st1 = connection.createStatement();
    ResultSet rs1 = st1.executeQuery("select count(status) from root.t1.wf01.wt01");
    rs1.next();
    // type of r1 is INT64(long), test long convert to int
    int countStatus = rs1.getInt(1);
    Assert.assertTrue(countStatus == 2L);

    ResultSet rs2 =
        st1.executeQuery("select type from root.t1.wf01.wt01 where time = 1000 limit 1");
    rs2.next();
    // type of r2 is INT32(int), test int convert to long
    long type = rs2.getLong(2);
    Assert.assertTrue(type == 1);

    ResultSet rs3 =
        st1.executeQuery("select grade from root.t1.wf01.wt01 where time = 1000 limit 1");
    rs3.next();
    // type of r3 is INT64(long), test long convert to int
    int grade = rs3.getInt(2);
    Assert.assertTrue(grade == 1000);

    st1.close();
  }
}
