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

package org.apache.iotdb.pipe.it.autocreate;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

abstract class AbstractPipeDualAutoIT {

  protected BaseEnv senderEnv;
  protected BaseEnv receiverEnv;

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);
    setupConfig();
    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  protected void setupConfig() {
    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @After
  public final void tearDown() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }

  protected static void awaitUntilFlush(BaseEnv env) {
    Awaitility.await()
        .atMost(1, TimeUnit.MINUTES)
        .pollDelay(2, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(
            () -> {
              if (!TestUtils.tryExecuteNonQueryWithRetry(env, "flush")) {
                return false;
              }
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
