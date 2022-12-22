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

import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.itbase.category.RemoteTest;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description This class is initially intend to test the issue of IOTDB-920, that is: Disable
 *     insert row thats only contains time/timestamp column
 * @time 27/9/20 20:56
 */
@Category({LocalStandaloneTest.class, ClusterTest.class, RemoteTest.class})
public class IOTDBInsertIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initBeforeClass();
    initCreateSQLStatement();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close();
    EnvFactory.getEnv().cleanAfterClass();
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
    sqls.add("CREATE DATABASE root.t1");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.f1 WITH DATATYPE=FLOAT, ENCODING=PLAIN");
    sqls.add("CREATE TIMESERIES root.t1.wf01.wt01.d1 WITH DATATYPE=DOUBLE, ENCODING=PLAIN");
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
  public void testNormalInsert() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute("insert into root.t1.wf01.wt01(timestamp, status) values (1000, true)");
    st0.execute("insert into root.t1.wf01.wt01(timestamp, status) values (2000, false)");
    st0.execute("insert into root.t1.wf01.wt01(timestamp, status) values (3000, true)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, status, temperature) values (4000, true, 17.1)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, status, temperature) values (5000, true, 20.1)");
    st0.execute(
        "insert into root.t1.wf01.wt01(timestamp, status, temperature) values (6000, true, 22)");
    st0.close();

    Statement st1 = connection.createStatement();
    ResultSet rs1 = st1.executeQuery("select count(status) from root.t1.wf01.wt01");
    rs1.next();
    long countStatus = rs1.getLong(1);
    Assert.assertTrue(countStatus == 6L);

    ResultSet rs2 = st1.executeQuery("select count(temperature) from root.t1.wf01.wt01");
    rs2.next();
    long countTemperature = rs2.getLong(1);
    Assert.assertTrue(countTemperature == 3);

    st1.close();
  }

  @Test(expected = Exception.class)
  public void testInsertWithTimesColumns() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(timestamp, status) values(11000)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithException1() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(timestamp, status) values(11000, true, 17.1)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithException2() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute(
        "insert into root.t1.wf01.wt01(timestamp, status, temperature) values(11000, true, 20.1, false)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithException3() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(status) values(11000, true)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithException4() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(status, temperature) values(true)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithException5() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(status, temperature) values(true, 20.1, false)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithException6() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute(" insert into root.t1.*.a(timestamp, b) values(1509465600000, true)");
  }

  @Test
  public void testInsertWithDuplicatedMeasurements() {
    try (Statement st1 = connection.createStatement()) {
      st1.execute(
          "insert into root.t1.wf01.wt01(time, s3, status, status) values(100, true, 20.1, 20.2)");
      Assert.fail();
    } catch (SQLException e) {
      Assert.assertEquals("411: Insertion contains duplicated measurement: status", e.getMessage());
    }
  }

  @Test
  public void testInsertInfinityFloatValue() {
    try (Statement st1 = connection.createStatement()) {
      st1.execute("insert into root.t1.wf01.wt01(time, f1) values(100, 3.4028235E300)");
      Assert.fail();
    } catch (SQLException e) {
      Assert.assertEquals(
          "313: failed to insert measurements [f1] caused by The input float value is Infinity",
          e.getMessage());
    }
  }

  @Test
  public void testInsertInfinityDoubleValue() {
    try (Statement st1 = connection.createStatement()) {
      st1.execute("insert into root.t1.wf01.wt01(time, d1) values(100, 3.4028235E6000)");
      Assert.fail();
    } catch (SQLException e) {
      Assert.assertEquals(
          "313: failed to insert measurements [d1] caused by The input double value is Infinity",
          e.getMessage());
    }
  }
}
