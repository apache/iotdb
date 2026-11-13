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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/** For pipe tests whose cluster configuration stays constant across all methods in a class. */
public abstract class AbstractPipeTableModelSharedClusterIT
    extends AbstractPipeTableModelDualManualIT {

  private static BaseEnv sharedSender;
  private static BaseEnv sharedReceiver;
  private static CompletableFuture<Void> startup;

  @Override
  protected void setupConfig() {
    super.setupConfig();
    // These small functional workloads verify models/patterns, not automatic region expansion.
    // CPU-based defaults create many tiny TsFiles and concurrent loads can exhaust the IT heap.
    // Keep a region per database; separate databases and the configured replicas remain covered.
    for (final BaseEnv env : new BaseEnv[] {senderEnv, receiverEnv}) {
      env.getConfig()
          .getCommonConfig()
          .setDataRegionGroupExtensionPolicy("CUSTOM")
          .setDefaultDataRegionGroupNumPerDatabase(1);
    }
  }

  @ClassRule
  public static final ExternalResource CLUSTERS =
      new ExternalResource() {
        @Override
        protected void after() {
          // Always release both environments, including when initialization or a test fails.
          try {
            if (sharedSender != null) {
              sharedSender.cleanClusterEnvironment();
            }
          } finally {
            try {
              if (sharedReceiver != null) {
                sharedReceiver.cleanClusterEnvironment();
              }
            } finally {
              sharedSender = null;
              sharedReceiver = null;
              startup = null;
            }
          }
        }
      };

  @Override
  @Before
  public void setUp() {
    if (sharedSender == null) {
      MultiEnvFactory.createEnv(2);
      sharedSender = MultiEnvFactory.getEnv(0);
      sharedReceiver = MultiEnvFactory.getEnv(1);
      senderEnv = sharedSender;
      receiverEnv = sharedReceiver;
      setupConfig();
      // Join both starts before any test or cleanup runs; retain failures for subsequent methods.
      startup = initializeClusters();
    }
    senderEnv = sharedSender;
    receiverEnv = sharedReceiver;
    startup.join();
  }

  protected CompletableFuture<Void> initializeClusters() {
    return CompletableFuture.allOf(
        CompletableFuture.runAsync(senderEnv::initClusterEnvironment),
        CompletableFuture.runAsync(receiverEnv::initClusterEnvironment));
  }

  @Override
  protected void cleanEnvironment() {
    if (startup == null || startup.isCompletedExceptionally()) {
      // A failed start is cleaned by the class rule; do not open connections to unavailable nodes.
      return;
    }
    try {
      // Drop all user pipes in both SQL dialects before deleting any database. Otherwise an active
      // pipe can recreate the receiver's data during cleanup and contaminate the next method.
      dropUserPipes(senderEnv);
      dropUserPipes(receiverEnv);
      resetCatalog(senderEnv);
      resetCatalog(receiverEnv);
    } catch (Exception e) {
      throw new AssertionError("Failed to reset shared pipe test clusters", e);
    }
  }

  private static void dropUserPipes(final BaseEnv env) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      for (final boolean tableModel : new boolean[] {false, true}) {
        final TShowPipeResp response =
            client.showPipe(
                new TShowPipeReq()
                    .setIsTableModel(tableModel)
                    .setUserName(SessionConfig.DEFAULT_USER));
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
        for (final TShowPipeInfo pipe : response.getPipeInfoList()) {
          if (!pipe.getId().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
            final int statusCode =
                client
                    .dropPipeExtended(new TDropPipeReq(pipe.getId()).setIsTableModel(tableModel))
                    .getCode();
            // Automatic drop can remove a pipe after the SHOW PIPE snapshot.
            if (statusCode != TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode()) {
              assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statusCode);
            }
          }
        }
      }
    }
  }

  private static void resetCatalog(final BaseEnv env) throws Exception {
    for (final String dialect :
        new String[] {BaseEnv.TREE_SQL_DIALECT, BaseEnv.TABLE_SQL_DIALECT}) {
      try (final Connection connection = env.getConnection(dialect);
          final Statement statement = connection.createStatement()) {
        final List<String> databases = new ArrayList<>();
        try (final ResultSet result = statement.executeQuery("show databases")) {
          while (result.next()) {
            final String database = result.getString(1);
            if (!"information_schema".equals(database)) {
              databases.add(database);
            }
          }
        }
        for (final String database : databases) {
          if (BaseEnv.TABLE_SQL_DIALECT.equals(dialect)) {
            // A preceding DROP TABLE/VIEW can still be running after the pipe progress marker.
            TestUtils.dropDatabaseWithRetry(
                statement, "\"" + database.replace("\"", "\"\"") + "\"");
          } else {
            statement.execute("delete database " + database);
          }
        }
        if (BaseEnv.TREE_SQL_DIALECT.equals(dialect)) {
          deleteUsersAndRoles(statement);
        }
      }
    }
  }

  private static void deleteUsersAndRoles(final Statement statement) throws SQLException {
    // Authorization is shared by both dialects and survives database deletion. Remove the users
    // and roles created/replicated by a scenario, including their grants and role memberships.
    // Reuse the tree connection and retain the administrator needed by subsequent methods.
    for (final String type : new String[] {"user", "role"}) {
      final List<String> names = new ArrayList<>();
      try (final ResultSet result = statement.executeQuery("list " + type)) {
        while (result.next()) {
          final String name = result.getString("user".equals(type) ? "User" : "Role");
          if (!"user".equals(type) || !SessionConfig.DEFAULT_USER.equals(name)) {
            names.add(name);
          }
        }
      }
      for (final String name : names) {
        statement.execute("drop " + type + " `" + name.replace("`", "``") + "`");
      }
    }
  }
}
