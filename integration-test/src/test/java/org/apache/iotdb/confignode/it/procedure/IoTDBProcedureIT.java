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

package org.apache.iotdb.confignode.it.procedure;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.procedure.impl.CreateManyDatabasesProcedure;
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.utils.constant.SqlConstant;
import org.apache.iotdb.it.env.EnvFactory;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.confignode.procedure.impl.CreateManyDatabasesProcedure.MAX_STATE;
import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

public class IoTDBProcedureIT {
  private static Logger LOGGER = LoggerFactory.getLogger(IoTDBProcedureIT.class);

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataReplicationFactor(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  /**
   * During CreateManyDatabasesProcedure executing, we expect that the procedure will be interrupted
   * only once. So let us shutdown the leader at the middle of the procedure.
   */
  @Test
  public void procedureRecoverAtAnotherConfigNodeTest() throws Exception {
    recoverTest(3, false);
  }

  @Test
  public void procedureRecoverAtTheSameConfigNodeTest() throws Exception {
    recoverTest(1, true);
  }

  private void recoverTest(int configNodeNum, boolean needReopenLeader) throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(configNodeNum, 1);

    // prepare expectedDatabases
    Set<String> expectedDatabases = new HashSet<>();
    for (int id = CreateManyDatabasesProcedure.getInitialStateStatic(); id < MAX_STATE; id++) {
      expectedDatabases.add(CreateManyDatabasesProcedure.DATABASE_NAME_PREFIX + id);
    }
    Assert.assertEquals(MAX_STATE, expectedDatabases.size());

    // prepare req
    final TGetDatabaseReq req =
        new TGetDatabaseReq(
            Arrays.asList(
                new ShowDatabaseStatement(new PartialPath(SqlConstant.getSingleRootArray()))
                    .getPathPattern()
                    .getNodes()),
            SchemaConstant.ALL_MATCH_SCOPE.serialize());

    SyncConfigNodeIServiceClient leaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    leaderClient.createManyDatabases();

    // Check whether the procedure is ongoing, and is still far from finishing
    TShowDatabaseResp resp = leaderClient.showDatabase(req);
    Assert.assertTrue(0 < resp.getDatabaseInfoMap().size());
    Assert.assertTrue(resp.getDatabaseInfoMap().size() < MAX_STATE);
    // Then shutdown the leader, wait the new leader exist and the procedure continue
    final int oldLeaderIndex = EnvFactory.getEnv().getLeaderConfigNodeIndex();
    EnvFactory.getEnv().getConfigNodeWrapper(oldLeaderIndex).stop();
    if (needReopenLeader) {
      EnvFactory.getEnv().getConfigNodeWrapper(oldLeaderIndex).start();
    }
    SyncConfigNodeIServiceClient newLeaderClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    Callable<Boolean> finalCheck =
        () -> {
          TShowDatabaseResp resp1 = newLeaderClient.showDatabase(req);
          if (MAX_STATE != resp1.getDatabaseInfoMap().size()) {
            return false;
          }
          resp1
              .getDatabaseInfoMap()
              .keySet()
              .forEach(databaseName -> expectedDatabases.remove(databaseName));
          return expectedDatabases.isEmpty();
        };
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(finalCheck);
  }
}
