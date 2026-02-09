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

package org.apache.iotdb.pipe.it.dual.treemodel.manual;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class AbstractPipeDualTreeModelManualIT {

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

  static void setupConfig() {
    senderEnvContainer
        .get()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnvContainer
        .get()
        .getConfig()
        .getDataNodeConfig()
        .setDataNodeMemoryProportion("3:3:1:1:3:1");

    receiverEnvContainer
        .get()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    // 10 min, assert that the operations will not time out
    senderEnvContainer.get().getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnvContainer.get().getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
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
  public final void cleanEnvironment() {
    TestUtils.executeNonQueries(senderEnv, Arrays.asList("drop database root.**"), null);
    TestUtils.executeNonQueries(receiverEnv, Arrays.asList("drop database root.**"), null);
  }

  protected static void awaitUntilFlush(BaseEnv env) {
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .pollDelay(2, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(
            () -> {
              TestUtils.executeNonQuery(env, "flush", null);
              return env.getDataNodeWrapperList().stream()
                  .anyMatch(
                      wrapper -> {
                        int fileNum = 0;
                        File sequence = new File(buildDataPath(wrapper, true));
                        File unsequence = new File(buildDataPath(wrapper, false));
                        if (sequence.exists() && sequence.listFiles() != null) {
                          fileNum += Objects.requireNonNull(sequence.listFiles()).length;
                        }
                        if (unsequence.exists() && unsequence.listFiles() != null) {
                          fileNum += Objects.requireNonNull(unsequence.listFiles()).length;
                        }
                        return fileNum > 0;
                      });
            });
  }

  private static String buildDataPath(DataNodeWrapper wrapper, boolean isSequence) {
    String nodePath = wrapper.getNodePath();
    return nodePath
        + File.separator
        + IoTDBConstant.DATA_FOLDER_NAME
        + File.separator
        + "datanode"
        + File.separator
        + IoTDBConstant.DATA_FOLDER_NAME
        + File.separator
        + (isSequence ? IoTDBConstant.SEQUENCE_FOLDER_NAME : IoTDBConstant.UNSEQUENCE_FOLDER_NAME);
  }
}
