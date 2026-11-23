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

package org.apache.iotdb.db.it;

import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({
  LocalStandaloneIT.class,
  ClusterIT.class,
  TableLocalStandaloneIT.class,
  TableClusterIT.class
})
public class IoTDBShowPipeMemoryIT {

  private static final String PIPE_NAME = "memory_owner";
  private static final String USER = "pipe_memory_user";
  private static final String PASSWORD = "password123456";
  private static final String[] COLUMN_NAMES = {
    "block_id",
    "name",
    "category",
    "pipe_name",
    "pipe_model",
    "memory_usage_in_bytes",
    "peak_memory_size_in_bytes",
    "max_memory_size_in_bytes",
    "allocation_time",
    "assigner",
    "parent_block_id",
    "hierarchy_level",
    "accounted_memory_usage_in_bytes"
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    // Reserve enough pipe memory for both pipes and the shared caches in the small test JVM.
    EnvFactory.getEnv().getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:9:0");
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE USER " + USER + " '" + PASSWORD + "'");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testColumnsAndPrivilegesInBothModels() throws Exception {
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      for (String dialect : Arrays.asList(BaseEnv.TREE_SQL_DIALECT, BaseEnv.TABLE_SQL_DIALECT)) {
        try (Connection connection =
                EnvFactory.getEnv()
                    .getConnection(
                        dataNode,
                        SessionConfig.DEFAULT_USER,
                        SessionConfig.DEFAULT_PASSWORD,
                        dialect);
            Statement statement = connection.createStatement()) {
          assertPipeMemoryColumnsAndReadOwners(statement, "SHOW PIPE MEMORY");
        }
        try (Connection connection =
                EnvFactory.getEnv().getConnection(dataNode, USER, PASSWORD, dialect);
            Statement statement = connection.createStatement()) {
          assertThrows(SQLException.class, () -> statement.executeQuery("SHOW PIPE MEMORY"));
        }
      }
    }
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("GRANT SYSTEM ON root.** TO USER " + USER);
      try {
        for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
          for (String dialect :
              Arrays.asList(BaseEnv.TREE_SQL_DIALECT, BaseEnv.TABLE_SQL_DIALECT)) {
            try (Connection userConnection =
                    EnvFactory.getEnv().getConnection(dataNode, USER, PASSWORD, dialect);
                Statement userStatement = userConnection.createStatement()) {
              assertPipeMemoryColumnsAndReadOwners(userStatement, "SHOW PIPE MEMORY");
            }
          }
        }
      } finally {
        statement.execute("REVOKE SYSTEM ON root.** FROM USER " + USER);
      }
    }
  }

  @Test
  public void testTreeAndTablePipeOwnership() throws Exception {
    MultiEnvFactory.createEnv(1);
    final BaseEnv receiver = MultiEnvFactory.getEnv(0);
    receiver.getConfig().getCommonConfig().setIsPipeEnableMemoryCheck(false);
    receiver.initClusterEnvironment(1, 1);
    try (Connection treeConnection = EnvFactory.getEnv().getConnection(BaseEnv.TREE_SQL_DIALECT);
        Statement treeStatement = treeConnection.createStatement();
        Connection tableConnection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement tableStatement = tableConnection.createStatement()) {
      treeStatement.execute("CREATE DATABASE root.pipe_memory");
      treeStatement.execute("CREATE TIMESERIES root.pipe_memory.d.s INT32");
      treeStatement.execute("INSERT INTO root.pipe_memory.d(time, s) VALUES (0, 0)");
      tableStatement.execute("CREATE DATABASE pipe_memory_db");
      tableStatement.execute("CREATE TABLE pipe_memory_db.t (device STRING TAG, s INT32 FIELD)");
      tableStatement.execute("INSERT INTO pipe_memory_db.t(time, device, s) VALUES (0, 'd', 0)");
      final String createPipe =
          "CREATE PIPE "
              + PIPE_NAME
              + " WITH SOURCE ('source.realtime.mode'='forced-log', 'source.history.enable'='false')"
              + " WITH SINK ('sink'='iotdb-thrift-sync-sink', 'sink.ip'='"
              + receiver.getDataNodeWrapper(0).getIp()
              + "', 'sink.port'='"
              + receiver.getDataNodeWrapper(0).getPort()
              + "',"
              + " 'sink.batch.enable'='false')";
      try {
        treeStatement.execute(createPipe);
        tableStatement.execute(createPipe);
        // Complete the handshake before stopping the receiver, so each pipe retains its events.
        receiver.shutdownDataNode(0);
        treeStatement.execute("INSERT INTO root.pipe_memory.d(time, s) VALUES (1, 1)");
        tableStatement.execute("INSERT INTO pipe_memory_db.t(time, device, s) VALUES (1, 'd', 1)");
        final Set<String> expectedOwners = new HashSet<>(Arrays.asList("tree", "table"));
        Awaitility.await()
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(
                () -> {
                  assertTrue(
                      readPipeOwnersOnAllDataNodes(BaseEnv.TREE_SQL_DIALECT, "SHOW PIPE MEMORY")
                          .containsAll(expectedOwners));
                  assertTrue(
                      readPipeOwnersOnAllDataNodes(BaseEnv.TABLE_SQL_DIALECT, "SHOW PIPE MEMORY")
                          .containsAll(expectedOwners));
                  assertTrue(
                      readPipeOwnersOnAllDataNodes(
                              BaseEnv.TABLE_SQL_DIALECT,
                              "SELECT * FROM information_schema.pipe_memory")
                          .containsAll(expectedOwners));
                });
      } finally {
        treeStatement.execute("DROP PIPE IF EXISTS " + PIPE_NAME);
        tableStatement.execute("DROP PIPE IF EXISTS " + PIPE_NAME);
      }
    } finally {
      receiver.cleanClusterEnvironment();
    }
  }

  private static Set<String> readPipeOwnersOnAllDataNodes(String dialect, String sql)
      throws SQLException {
    final Set<String> owners = new HashSet<>();
    // Query each node separately because memory diagnostics describe local allocations.
    for (DataNodeWrapper dataNode : EnvFactory.getEnv().getDataNodeWrapperList()) {
      try (Connection connection =
              EnvFactory.getEnv()
                  .getConnection(
                      dataNode,
                      SessionConfig.DEFAULT_USER,
                      SessionConfig.DEFAULT_PASSWORD,
                      dialect);
          Statement statement = connection.createStatement()) {
        owners.addAll(assertPipeMemoryColumnsAndReadOwners(statement, sql));
      }
    }
    return owners;
  }

  private static Set<String> assertPipeMemoryColumnsAndReadOwners(Statement statement, String sql)
      throws SQLException {
    final Set<String> owners = new HashSet<>();
    try (ResultSet resultSet = statement.executeQuery(sql)) {
      final ResultSetMetaData metadata = resultSet.getMetaData();
      assertEquals(COLUMN_NAMES.length, metadata.getColumnCount());
      for (int i = 0; i < COLUMN_NAMES.length; i++) {
        assertEquals(COLUMN_NAMES[i], metadata.getColumnName(i + 1));
      }
      boolean hasFloatingMemory = false;
      while (resultSet.next()) {
        if ("FloatingMemory".equals(resultSet.getString("name"))) {
          assertNull(resultSet.getObject("pipe_name"));
          assertNull(resultSet.getObject("pipe_model"));
          assertTrue(resultSet.getLong("memory_usage_in_bytes") >= 0);
          hasFloatingMemory = true;
        }
        if (PIPE_NAME.equals(resultSet.getString("pipe_name"))) {
          final String model = resultSet.getString("pipe_model");
          assertTrue("tree".equals(model) || "table".equals(model));
          owners.add(model);
        }
      }
      assertTrue(hasFloatingMemory);
    }
    return owners;
  }
}
