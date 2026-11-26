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
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class AbstractPipeDualTreeModelManualIT {

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
    // The clusters own separate node directories and ports. Wait for both tasks, including when one
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
        .setAutoCreateSchemaEnabled(false)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:3:0");

    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @After
  public final void tearDown() {
    cleanEnvironment();
  }

  protected void cleanEnvironment() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }

  protected static void awaitUntilFlush(BaseEnv env) throws SQLException {
    // Reuse the connection across immediate, bounded checks instead of reopening it on every poll.
    try (Connection connection = env.getConnection();
        Statement statement = connection.createStatement()) {
      Awaitility.await()
          .atMost(1, TimeUnit.MINUTES)
          .pollDelay(0, TimeUnit.MILLISECONDS)
          .pollInterval(100, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                statement.execute("flush");
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
