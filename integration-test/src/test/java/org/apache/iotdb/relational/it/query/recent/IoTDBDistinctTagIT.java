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

package org.apache.iotdb.relational.it.query.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBDistinctTagIT {
  private static final String DATABASE_NAME = "test";

  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS test",
        "USE test",
        // test flush
        "CREATE TABLE IF NOT EXISTS t1(deviceId STRING TAG, attr1 STRING ATTRIBUTE, s1 INT64 FIELD)",
        "insert into t1(time, deviceId, attr1, s1) values(1000, 'd1', 'a1', 10)",
        "insert into t1(time, deviceId, attr1, s1) values(1000, 'd2', 'a2', 11)",
        "insert into t1(time, deviceId, attr1, s1) values(2000, 'd2', 'xx', 12)",
        "insert into t1(time, deviceId, attr1, s1) values(4000, 'd1', 'a1', 13)",
        "flush",
        "insert into t1(time, deviceId, attr1, s1) values(5000, 'd3', 'a3', 10)",
        "insert into t1(time, deviceId, attr1, s1) values(6000, 'd4', 'a4', 11)",
        "clear attribute cache",
        "insert into t1(time, deviceId, attr1, s1) values(3000, 'd2', 'a2', 12)",
        "insert into t1(time, deviceId, attr1, s1) values(2000, 'd1', 'a1', 13)",
        "flush",
        "clear attribute cache",

        // test memory
        "CREATE TABLE IF NOT EXISTS t2(deviceId STRING TAG, attr1 STRING ATTRIBUTE, s1 INT64 FIELD)",
        "insert into t2(time, deviceId, attr1, s1) values(1000, 'd1', 'a1', 10)",
        "insert into t2(time, deviceId, attr1, s1) values(1000, 'd2', 'a2', 11)",
        "insert into t2(time, deviceId, attr1, s1) values(2000, 'd2', 'xx', 12)",
        "insert into t2(time, deviceId, attr1, s1) values(4000, 'd1', 'a1', 13)",
        "insert into t2(time, deviceId, attr1, s1) values(5000, 'd3', 'a3', 10)",
        "insert into t2(time, deviceId, attr1, s1) values(6000, 'd4', 'a4', 11)",
        "clear attribute cache",
        "insert into t2(time, deviceId, attr1, s1) values(3000, 'd2', 'a2', 12)",
        "insert into t2(time, deviceId, attr1, s1) values(2000, 'd1', 'a1', 13)",
        "clear attribute cache",
        "clear all cache",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testDistinct() {
    // distinct(deviceId)
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {"d1,", "d2,", "d3,", "d4,"};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t1 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t2 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // distinct(attr1)
    expectedHeader = new String[] {"attr1"};
    retArray = new String[] {"a1,", "a2,", "a3,", "a4,"};
    tableResultSetEqualTest(
        "select distinct(attr1) from test.t1 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(attr1) from test.t2 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDistinctWithTimeFilter() {
    // distinct(deviceId) ... where time > 3000;
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {"d1,", "d3,", "d4,"};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t1 where time > 3000 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t2 where time > 3000 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // distinct(attr1) ... where time = 5000;
    expectedHeader = new String[] {"attr1"};
    retArray = new String[] {"a3,"};
    tableResultSetEqualTest(
        "select distinct(attr1) from test.t1 where time = 5000 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(attr1) from test.t2 where time = 5000 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDistinctWithPushDownFilter() {
    // distinct(deviceId) ... where s1 = 11;
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {"d2,", "d4,"};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t1 where s1 = 11 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t2 where s1 = 11 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // distinct(attr1) ... where s1 > 11;
    expectedHeader = new String[] {"attr1"};
    retArray = new String[] {"a1,", "a2,"};
    tableResultSetEqualTest(
        "select distinct(attr1) from test.t1 where s1 > 11 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(attr1) from test.t2 where s1 > 11 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  @Test
  public void testDistinctWithDelete() throws SQLException {
    String[] sqls =
        new String[] {
          "USE test",
          // test flush
          "CREATE TABLE IF NOT EXISTS t3(deviceId STRING TAG, attr1 STRING ATTRIBUTE, s1 INT64 FIELD)",
          "insert into t3(time, deviceId, attr1, s1) values(1000, 'd1', 'a1', 10)",
          "insert into t3(time, deviceId, attr1, s1) values(1000, 'd2', 'a2', 11)",
          "insert into t3(time, deviceId, attr1, s1) values(2000, 'd2', 'xx', 12)",
          "insert into t3(time, deviceId, attr1, s1) values(4000, 'd1', 'a1', 13)",
          "flush",
          "delete from test.t3",
          "clear all cache",

          // test memory
          "CREATE TABLE IF NOT EXISTS t4(deviceId STRING TAG, attr1 STRING ATTRIBUTE, s1 INT64 FIELD)",
          "insert into t4(time, deviceId, attr1, s1) values(1000, 'd1', 'a1', 10)",
          "insert into t4(time, deviceId, attr1, s1) values(1000, 'd2', 'a2', 11)",
          "insert into t4(time, deviceId, attr1, s1) values(2000, 'd2', 'xx', 12)",
          "insert into t4(time, deviceId, attr1, s1) values(4000, 'd1', 'a1', 13)",
          "delete from test.t4",
          "clear all cache",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    }

    // distinct(deviceId)
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t3 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);
    tableResultSetEqualTest(
        "select distinct(deviceId) from test.t4 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    // distinct(attr1)
    expectedHeader = new String[] {"attr1"};
    retArray = new String[] {};
    tableResultSetEqualTest(
        "select distinct(attr1) from test.t3 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(attr1) from test.t4 order by attr1",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }

      // try to wait attribute cache cleared
      TimeUnit.SECONDS.sleep(5);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
