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

package org.apache.iotdb.pipe.it.pipe_table;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.RpcUtils;

import java.sql.Connection;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.fail;

public class Utils {

  public static void insertData(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (id1, s3, s2, s1, time) values ('t%s','%s', %s.0, %s, %s)",
              tableName, i, i, i, i, i));
    }
    list.add("flush");
    if (!TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list)) {
      fail();
    }
  }

  public static boolean insertDataNotThrowError(
      String dataBaseName, String tableName, int start, int end, BaseEnv baseEnv) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (id1, s3, s2, s1, time) values ('t%s','%s', %s.0, %s, %s)",
              tableName, i, i, i, i, i));
    }
    return TestUtils.tryExecuteNonQueriesWithRetry(
        dataBaseName, BaseEnv.TABLE_SQL_DIALECT, baseEnv, list);
  }

  public static void insertData(
      String dataBaseName,
      String tableName,
      int start,
      int end,
      BaseEnv baseEnv,
      DataNodeWrapper wrapper) {
    List<String> list = new ArrayList<>(end - start + 1);
    for (int i = start; i < end; ++i) {
      list.add(
          String.format(
              "insert into %s (id1, s3, s2, s1, time) values ('t%s','%s', %s.0, %s, %s)",
              tableName, i, i, i, i, i));
    }
    list.add("flush");
    if (!TestUtils.tryExecuteNonQueriesOnSpecifiedDataNodeWithRetry(
        baseEnv, wrapper, list, dataBaseName, BaseEnv.TABLE_SQL_DIALECT)) {
      fail();
    }
  }

  public static void createDataBaseAndTable(BaseEnv baseEnv, String table, String database) {
    try (Connection connection = baseEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("create database if not exists " + database);
      statement.execute("use " + database);
      statement.execute(
          "CREATE TABLE "
              + table
              + "(id1 string id, s1 int64 measurement, s2 float measurement, s3 string measurement)");
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public static Set<String> generateExpectedResults(int start, int end) {
    Set<String> expectedResSet = new HashSet<>();
    for (int i = start; i < end; ++i) {
      expectedResSet.add(
          String.format(
              "t%d,%d,%d.0,%d,%s,",
              i, i, i, i, RpcUtils.formatDatetime("default", "ms", i, ZoneOffset.UTC)));
    }
    return expectedResSet;
  }

  public static String generateHeaderResults() {
    return "id1,s3,s2,s1,time,";
  }

  public static String getQuerySql(String table) {
    return "select id1,s3,s2,s1,time from " + table;
  }

  public static String getQueryCountSql(String table) {
    return "select count(*) from " + table;
  }

  public static void assertData(
      String database, String table, int start, int end, BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv,
        Utils.getQuerySql("test"),
        Utils.generateHeaderResults(),
        Utils.generateExpectedResults(start, end),
        "test");
  }

  public static void assertCountData(String database, String table, int count, BaseEnv baseEnv) {
    TestUtils.assertDataEventuallyOnEnv(
        baseEnv, getQueryCountSql(table), "_col0,", Collections.singleton(count + ","), "test");
  }
}
