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

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.tableResultSetEqualTest;
import static org.junit.Assert.fail;

public abstract class IoTDBDistinctTagTableViewBase {
  private static final String DATABASE_NAME = "test_view";

  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.test",
        // test flush
        "CREATE TIMESERIES root.test.t1.d1.s1 INT64",
        "CREATE TIMESERIES root.test.t1.d2.s1 INT64",
        "CREATE TIMESERIES root.test.t1.d3.s1 INT64",
        "CREATE TIMESERIES root.test.t1.d4.s1 INT64",
        "insert into root.test.t1.d1(time, s1) values(1000, 10)",
        "insert into root.test.t1.d2(time, s1) values(1000, 11)",
        "insert into root.test.t1.d2(time, s1) values(2000, 12)",
        "insert into root.test.t1.d1(time, s1) values(4000, 13)",
        "flush",
        "insert into root.test.t1.d3(time, s1) values(5000, 10)",
        "insert into root.test.t1.d4(time, s1) values(6000, 11)",
        "insert into root.test.t1.d2(time, s1) values(3000, 12)",
        "insert into root.test.t1.d1(time, s1) values(2000, 13)",
        "flush",

        // test memory
        "CREATE TIMESERIES root.test.t2.d1.s1 INT64",
        "CREATE TIMESERIES root.test.t2.d2.s1 INT64",
        "CREATE TIMESERIES root.test.t2.d3.s1 INT64",
        "CREATE TIMESERIES root.test.t2.d4.s1 INT64",
        "insert into root.test.t2.d1(time, s1) values(1000, 10)",
        "insert into root.test.t2.d2(time, s1) values(1000, 11)",
        "insert into root.test.t2.d2(time, s1) values(2000, 12)",
        "insert into root.test.t2.d1(time, s1) values(4000, 13)",
        "insert into root.test.t2.d3(time, s1) values(5000, 10)",
        "insert into root.test.t2.d4(time, s1) values(6000, 11)",
        "insert into root.test.t2.d2(time, s1) values(3000, 12)",
        "insert into root.test.t2.d1(time, s1) values(2000, 13)",
      };

  protected static final String[] CREATE_TABLE_VIEW_SQLs =
      new String[] {
        "CREATE DATABASE IF NOT EXISTS test_view",
        "USE test_view",
        "CREATE VIEW t1(deviceId STRING TAG, s1 INT64 FIELD) as root.test.t1.**",
        "CREATE VIEW t2(deviceId STRING TAG, s1 INT64 FIELD) as root.test.t2.**",
      };

  protected static void prepareViewData() throws Exception {
    prepareData();
  }

  // Verifies DISTINCT over table views returns the expected unique tag combinations.
  @Test
  public void viewTestDistinct() {
    // distinct(deviceId)
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {"d1,", "d2,", "d3,", "d4,"};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test_view.t1 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(deviceId) from test_view.t2 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Verifies DISTINCT with a time predicate returns tags present in the selected interval.
  @Test
  public void viewTestDistinctWithTimeFilter() {
    // distinct(deviceId) ... where time > 3000;
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {"d1,", "d3,", "d4,"};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test_view.t1 where time > 3000 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(deviceId) from test_view.t2 where time > 3000 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  // Verifies DISTINCT keeps matching tags when a value predicate is pushed through the view.
  @Test
  public void viewTestDistinctWithPushDownFilter() {
    // distinct(deviceId) ... where s1 = 11;
    String[] expectedHeader = new String[] {"deviceId"};
    String[] retArray = new String[] {"d2,", "d4,"};
    tableResultSetEqualTest(
        "select distinct(deviceId) from test_view.t1 where s1 = 11 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);

    tableResultSetEqualTest(
        "select distinct(deviceId) from test_view.t2 where s1 = 11 order by deviceId",
        expectedHeader,
        retArray,
        DATABASE_NAME);
  }

  private static void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {

      for (String sql : CREATE_TABLE_VIEW_SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
