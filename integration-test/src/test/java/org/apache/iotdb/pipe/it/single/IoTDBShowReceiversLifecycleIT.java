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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBShowReceiversLifecycleIT {

  private static BaseEnv env;

  @BeforeClass
  public static void setUp() {
    env = EnvFactory.getEnv();
    env.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    env.initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    env.cleanClusterEnvironment();
  }

  @Test
  public void testShowReceiversPipeIdsDisappearAfterDropPipe() {
    final String database = "root.show_receivers_lifecycle";
    final String pipeName = "show_receivers_lifecycle_pipe";

    createWriteBackPipe(database, pipeName);

    assertShowReceivers("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceivers(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);

    TestUtils.executeNonQueries(env, Collections.singletonList("drop pipe " + pipeName), null);

    assertShowReceiversDoesNotContainPipe("show receivers", BaseEnv.TREE_SQL_DIALECT, pipeName);
    assertShowReceiversDoesNotContainPipe(
        "select * from information_schema.receivers", BaseEnv.TABLE_SQL_DIALECT, pipeName);
  }

  private void createWriteBackPipe(final String database, final String pipeName) {
    TestUtils.executeNonQueries(
        env,
        Arrays.asList(
            "create database " + database,
            "create timeseries " + database + ".d1.s1 with datatype=INT32, encoding=PLAIN",
            "create pipe "
                + pipeName
                + " with source ('pattern'='"
                + database
                + "') with sink ('sink'='write-back-sink')",
            "insert into " + database + ".d1(time, s1) values (1, 1)",
            "flush"),
        null);
  }

  private void assertShowReceivers(
      final String sql, final String sqlDialect, final String pipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertTrue(hasExpectedReceiver(sql, sqlDialect, pipeName)));
  }

  private boolean hasExpectedReceiver(
      final String sql, final String sqlDialect, final String pipeName) throws SQLException {
    try (final Connection connection =
            env.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        if ("DataNode".equals(resultSet.getString(1))
            && "thrift".equals(resultSet.getString(3))
            && resultSet.getString(4) != null
            && !resultSet.getString(4).isEmpty()
            && resultSet.getString(5) != null
            && !resultSet.getString(5).isEmpty()
            && resultSet.getInt(6) >= 1
            && resultSet.getInt(7) >= 1
            && resultSet.getString(8).contains(pipeName + "@")
            && "root".equals(resultSet.getString(9))
            && resultSet.getString(10) != null
            && !resultSet.getString(10).isEmpty()
            && resultSet.getString(11) != null
            && resultSet.getString(12) != null) {
          return true;
        }
      }
      return false;
    }
  }

  private void assertShowReceiversDoesNotContainPipe(
      final String sql, final String sqlDialect, final String pipeName) {
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(1L, TimeUnit.SECONDS)
        .pollInterval(1L, TimeUnit.SECONDS)
        .atMost(60L, TimeUnit.SECONDS)
        .untilAsserted(() -> Assert.assertFalse(containsPipe(sql, sqlDialect, pipeName)));
  }

  private boolean containsPipe(final String sql, final String sqlDialect, final String pipeName)
      throws SQLException {
    try (final Connection connection =
            env.getConnection(
                SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD, sqlDialect);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        final String pipeIds = resultSet.getString(8);
        if (pipeIds != null && pipeIds.contains(pipeName + "@")) {
          return true;
        }
      }
      return false;
    }
  }
}
