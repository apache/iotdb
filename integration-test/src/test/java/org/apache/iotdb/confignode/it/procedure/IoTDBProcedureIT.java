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
import org.apache.iotdb.confignode.rpc.thrift.TGetDatabaseReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDatabaseResp;
import org.apache.iotdb.it.env.EnvFactory;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

public class IoTDBProcedureIT {

  private static final int testReplicationFactor = 2;

  private static final int testConfigNodeNum = 3, testDataNodeNum = 2;

  private static final long testTimePartitionInterval = 604800000;

  @Before
  public void setUp() {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(RATIS_CONSENSUS)
//        .setSchemaReplicationFactor(testReplicationFactor)
        .setDataReplicationFactor(1);
  }

  @Test
  public void recoverTest() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment(3, 1);
    final SyncConfigNodeIServiceClient configClient =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection();
    configClient.createManyDatabases();
    Thread.sleep(10000);
    TShowDatabaseResp resp =
        configClient.showDatabase(
            new TGetDatabaseReq().setDatabasePathPattern(new ArrayList<>()).setScopePatternTree(new byte[] {}));
    resp.getDatabaseInfoMap()
        .forEach(
            (key, value) -> {
              System.out.println(key + ": " + value);
            });
    //    for (Map.Entry<String, TDatabaseInfo> entry : .) {
    //
    //    }
  }
}
