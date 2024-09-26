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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2AutoCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2AutoCreateSchema.class})
public class IoTDBPipeConnectorCustomPortIT extends AbstractPipeDualAutoIT {

  @Override
  @Before
  public void setUp() {
    // Override to enable air-gap
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

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
        .setPipeAirGapReceiverEnabled(true)
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testRangePortsAvailable() {
    doTest(
        "iotdb-thrift-async-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getPort(),
        "range",
        "1024",
        "1025",
        0,
        "16",
        10000,
        0);
  }

  @Test
  public void testCandidatePortsAvailable() {
    doTest(
        "iotdb-thrift-async-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getPort(),
        "candidate",
        null,
        null,
        4,
        "16",
        10000,
        0);
  }

  @Test
  public void testAsyncPortRange() {
    doTest(
        "iotdb-thrift-async-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getPort(),
        "range",
        "1024",
        "1224",
        0,
        "3",
        2000,
        2);
  }

  @Test
  public void testAsyncPortCandidate() {
    doTest(
        "iotdb-thrift-async-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getPort(),
        "candidate",
        "1024",
        "1224",
        30,
        "1",
        2000,
        2);
  }

  @Test
  public void testSyncThriftPortRange() {
    doTest(
        "iotdb-thrift-sync-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getPort(),
        "range",
        "1024",
        "1224",
        30,
        "1",
        2000,
        2);
  }

  @Test
  public void testSyncThriftPortCandidate() {
    doTest(
        "iotdb-thrift-sync-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getPort(),
        "candidate",
        "1024",
        "1224",
        30,
        "1",
        2000,
        2);
  }

  @Test
  public void testAirGapPortRange() {
    doTest(
        "iotdb-air-gap-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getDataNodeWrapper(0).getPipeAirGapReceiverPort(),
        "range",
        "1024",
        "1224",
        30,
        "1",
        2000,
        2);
  }

  @Test
  public void testAirGapPortCandidate() {
    doTest(
        "iotdb-air-gap-connector",
        receiverEnv.getIP() + ":" + receiverEnv.getDataNodeWrapper(0).getPipeAirGapReceiverPort(),
        "candidate",
        "1024",
        "1224",
        30,
        "1",
        2000,
        2);
  }

  private void doTest(
      final String connector,
      final String urls,
      final String strategy,
      final String min,
      final String max,
      final int candidateNum,
      final String tasks,
      final long sleepTime,
      final int resultNum) {
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();
      extractorAttributes.put("realtime.mode", "forced-log");
      connectorAttributes.put("connector", connector);
      connectorAttributes.put("batch.enable", "false");
      connectorAttributes.put("node-urls", urls);
      connectorAttributes.put("send-port.restriction-strategy", strategy);
      if (strategy.equals("range")) {
        connectorAttributes.put("send-port.range.min", min);
        connectorAttributes.put("send-port.range.max", max);
      } else {
        StringBuilder candidateBuilder = new StringBuilder();
        for (int i = 0; i < candidateNum; i++) {
          candidateBuilder.append(1024 + i).append(",");
        }
        candidateBuilder.append(1024 + candidateNum);
        connectorAttributes.put("send-port.candidate", candidateBuilder.toString());
      }
      connectorAttributes.put("parallel.tasks", tasks);

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("p1", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Thread.sleep(sleepTime);
      TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "insert into root.db.d1(time, s1) values (2010-01-01T10:00:00+08:00, 1)",
              "insert into root.db.d1(time, s1) values (2010-01-02T10:00:00+08:00, 2)",
              "flush"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select count(*) from root.**",
          resultNum == 0 ? "Time," : "count(root.db.d1.s1),",
          resultNum == 0 ? new HashSet<>() : Collections.singleton(resultNum + ","));

    } catch (Exception e) {
      Assert.fail();
    }
  }
}
