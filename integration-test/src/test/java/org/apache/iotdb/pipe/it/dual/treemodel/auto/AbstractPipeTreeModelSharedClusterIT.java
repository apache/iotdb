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

package org.apache.iotdb.pipe.it.dual.treemodel.auto;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.rpc.thrift.TDropPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Shares clusters for tree pipe/data scenarios with fixed configuration. Tests that change cluster
 * topology, authorization, templates or other global state need their own lifecycle/reset logic.
 */
public abstract class AbstractPipeTreeModelSharedClusterIT extends AbstractPipeDualTreeModelAutoIT {

  private static BaseEnv sharedSender;
  private static BaseEnv sharedReceiver;
  private static CompletableFuture<Void> startup;

  @ClassRule
  public static final ExternalResource CLUSTERS =
      new ExternalResource() {
        @Override
        protected void after() {
          // Always release both clusters, including a partially initialized or failed environment.
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
      // Await both independent starts before running any scenario or attempting cleanup.
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
      return;
    }
    try {
      // Stop replication on both sides before deleting data, so a live pipe cannot recreate it.
      dropTreePipes(senderEnv);
      dropTreePipes(receiverEnv);
      deleteTreeDatabases(senderEnv);
      deleteTreeDatabases(receiverEnv);
    } catch (Exception e) {
      throw new AssertionError("Failed to reset shared tree pipe test clusters", e);
    }
  }

  private static void dropTreePipes(final BaseEnv env) throws Exception {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) env.getLeaderConfigNodeConnection()) {
      final TShowPipeResp response =
          client.showPipe(
              new TShowPipeReq().setIsTableModel(false).setUserName(SessionConfig.DEFAULT_USER));
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
      for (final TShowPipeInfo pipe : response.getPipeInfoList()) {
        if (!pipe.getId().startsWith(PipeStaticMeta.SYSTEM_PIPE_PREFIX)) {
          final int statusCode =
              client
                  .dropPipeExtended(new TDropPipeReq(pipe.getId()).setIsTableModel(false))
                  .getCode();
          // An in-flight DROP PIPE can finish after the SHOW PIPE snapshot.
          if (statusCode != TSStatusCode.PIPE_NOT_EXIST_ERROR.getStatusCode()) {
            assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), statusCode);
          }
        }
      }
    }
  }

  private static void deleteTreeDatabases(final BaseEnv env) throws Exception {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (final ResultSet result = statement.executeQuery("show databases")) {
        while (result.next()) {
          databases.add(result.getString(1));
        }
      }
      for (final String database : databases) {
        try {
          statement.execute("delete database " + database);
        } catch (final IoTDBSQLException e) {
          // An in-flight metadata DROP can finish after the SHOW DATABASES snapshot.
          if (e.getErrorCode() != TSStatusCode.PATH_NOT_EXIST.getStatusCode()) {
            throw e;
          }
        }
      }
    }
  }
}
