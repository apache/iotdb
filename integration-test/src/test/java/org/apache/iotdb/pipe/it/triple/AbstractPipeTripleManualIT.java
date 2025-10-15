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

package org.apache.iotdb.pipe.it.triple;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;

abstract class AbstractPipeTripleManualIT {

  protected BaseEnv env1;
  protected BaseEnv env2;
  protected BaseEnv env3;

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(3);
    env1 = MultiEnvFactory.getEnv(0);
    env2 = MultiEnvFactory.getEnv(1);
    env3 = MultiEnvFactory.getEnv(2);
    setupConfig();
    env1.initClusterEnvironment(1, 1);
    env2.initClusterEnvironment(1, 1);
    env3.initClusterEnvironment(1, 1);
  }

  protected void setupConfig() {
    env1.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    env1.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:3:1");

    env2.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    env3.getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    // 10 min, assert that the operations will not time out
    env1.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    env2.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    env3.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @After
  public final void tearDown() {
    env1.cleanClusterEnvironment();
    env2.cleanClusterEnvironment();
    env3.cleanClusterEnvironment();
  }
}
