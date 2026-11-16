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

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public abstract class AbstractPipeTableModelDualManualIT {

  protected BaseEnv senderEnv;
  protected BaseEnv receiverEnv;

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    executeOnBothEnvs(BaseEnv::initClusterEnvironment);
  }

  protected void executeOnBothEnvs(final Consumer<BaseEnv> action) {
    // The environments own separate node directories and ports. Await both tasks even if one
    // fails, so teardown cannot race with a cluster that is still starting or restarting.
    CompletableFuture.allOf(
            CompletableFuture.runAsync(() -> action.accept(senderEnv)),
            CompletableFuture.runAsync(() -> action.accept(receiverEnv)))
        .join();
  }

  protected void setupConfig() {
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnv.getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");
  }

  @After
  public final void tearDown() {
    cleanEnvironment();
  }

  protected void cleanEnvironment() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }
}
