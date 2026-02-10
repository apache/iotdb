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
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;

public abstract class AbstractPipeTableModelDualManualIT {

  protected static ThreadLocal<BaseEnv> senderEnvContainer;
  protected static ThreadLocal<BaseEnv> receiverEnvContainer;

  protected BaseEnv senderEnv;
  protected BaseEnv receiverEnv;

  @BeforeClass
  public static void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnvContainer.set(MultiEnvFactory.getEnv(0));
    receiverEnvContainer.set(MultiEnvFactory.getEnv(1));
    setupConfig();
    senderEnvContainer.get().initClusterEnvironment();
    receiverEnvContainer.get().initClusterEnvironment();
  }

  protected static void setupConfig() {
    senderEnvContainer
        .get()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    receiverEnvContainer
        .get()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setEnforceStrongPassword(false)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    // 10 min, assert that the operations will not time out
    senderEnvContainer.get().getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnvContainer.get().getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);

    senderEnvContainer.get().getConfig().getConfigNodeConfig().setLeaderDistributionPolicy("HASH");
  }

  @AfterClass
  public static void tearDown() {
    senderEnvContainer.get().cleanClusterEnvironment();
    receiverEnvContainer.get().cleanClusterEnvironment();
  }

  @Before
  public void setEnv() {
    senderEnv = senderEnvContainer.get();
    receiverEnv = receiverEnvContainer.get();
  }

  @After
  public void cleanEnvironment() {
    TestUtils.executeNonQueries(senderEnv, Arrays.asList("drop database root.**"), null);
    TestUtils.executeNonQueries(receiverEnv, Arrays.asList("drop database root.**"), null);
  }
}
