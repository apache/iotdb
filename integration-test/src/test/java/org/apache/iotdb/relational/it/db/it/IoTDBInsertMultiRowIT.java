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

package org.apache.iotdb.relational.it.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBInsertMultiRowIT {
  private static List<String> sqls = new ArrayList<>();
  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxInnerCompactionCandidateFileNum(2);
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
    sqls.add("CREATE DATABASE t1");
    sqls.add("USE \"t1\"");
    sqls.add(
        "create table wf01 (id1 string id, status boolean measurement, temperature float measurement)");
  }

  private static void insertData() throws ClassNotFoundException, SQLException {
    connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
    Statement statement = connection.createStatement();

    for (String sql : sqls) {
      statement.execute(sql);
    }

    statement.close();
  }

  @Ignore // aggregation
  @Test
  public void testInsertMultiRow() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute("insert into wf01(id1, time, status) values ('wt01', 1, true)");
    st0.execute("insert into wf01(id1, time, status) values ('wt01', 2, true), ('wt01', 3, false)");
    st0.execute(
        "insert into wf01(id1, time, status) values ('wt01', 4, true), ('wt01', 5, true), ('wt01', 6, false)");

    st0.execute("insert into wf01(id1, time, temperature, status) values ('wt01', 7, 15.3, true)");
    st0.execute(
        "insert into wf01(id1, time, temperature, status) values ('wt01', 8, 18.3, false), ('wt01', 9, 23.1, false)");
    st0.execute(
        "insert into wf01(id1, time, temperature, status) values ('wt01', 10, 22.3, true), ('wt01', 11, 18.8, false), ('wt01', 12, 24.4, true)");
    st0.close();

    Statement st1 = connection.createStatement();
    ResultSet rs1 = st1.executeQuery("select count(status) from wf01");
    rs1.next();
    long countStatus = rs1.getLong(1);
    assertEquals(countStatus, 12L);

    ResultSet rs2 = st1.executeQuery("select count(temperature) from wf01");
    rs2.next();
    long countTemperature = rs2.getLong(1);
    assertEquals(countTemperature, 6L);

    st1.close();
  }

  @Test(expected = Exception.class)
  @Ignore
  public void testInsertWithTimesColumns() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into wf01(id1,time) values('wt01', 1)");
  }

  @Test
  public void testInsertMultiRowWithMisMatchDataType() {
    try {
      Statement st1 = connection.createStatement();
      st1.execute(
          "insert into wf01(id1, time, status) values('wt01', 1, 1.0), ('wt01', 2, 'hello')");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains(Integer.toString(TSStatusCode.METADATA_ERROR.getStatusCode())));
    }
  }

  @Test
  @Ignore // TODO: delete
  public void testInsertMultiRowWithNull() {
    try (Statement st1 = connection.createStatement()) {
      st1.execute(
          "insert into root.t1.d99.wt01(time, s1, s2) values(100, null, 1), (101, null, 2)");
      fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains(Integer.toString(TSStatusCode.METADATA_ERROR.getStatusCode())));
    }
    try (Statement st2 = connection.createStatement()) {
      st2.execute("CREATE TIMESERIES root.t1.d1.s1 WITH DATATYPE=double, ENCODING=PLAIN;");
      st2.execute(
          "INSERT INTO root.t1.d1(time, s1) VALUES (6, 10),(7,12),(8,14),(9,160),(10,null),(11,58)");
    } catch (SQLException e) {
      fail();
    }
  }

  @Test
  public void testInsertMultiRowWithWrongTimestampPrecision() {
    try (Statement st1 = connection.createStatement()) {
      st1.execute(
          "insert into wf01(id1, time, status) values('wt01', 1618283005586000, true), ('wt01', 1618283005586001, false)");
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Current system timestamp precision is ms"));
    }
  }

  @Test
  public void testInsertMultiRowWithMultiTimePartition() throws Exception {
    try (Statement st1 = connection.createStatement()) {
      st1.execute("create table sg1 (id1 string id, s1 int32 measurement)");
      st1.execute("insert into sg1(id1, time, s1) values('d1', 604800010,1)");
      st1.execute("flush");
      st1.execute("insert into sg1(id1, time, s1) values('d1', 604799990,1), ('d1', 604800001,1)");
      st1.execute("flush");
      ResultSet rs1 = st1.executeQuery("select time, s1 from sg1");
      assertTrue(rs1.next());
      assertEquals(604799990, rs1.getLong("time"));
      assertTrue(rs1.next());
      assertEquals(604800001, rs1.getLong("time"));
      assertTrue(rs1.next());
      assertEquals(604800010, rs1.getLong("time"));
      assertFalse(rs1.next());
    }
  }
}
