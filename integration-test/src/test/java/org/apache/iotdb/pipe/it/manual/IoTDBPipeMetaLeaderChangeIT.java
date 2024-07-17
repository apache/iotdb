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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeMetaLeaderChangeIT extends AbstractPipeDualManualIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    senderEnv
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment(3, 3, 180);
    receiverEnv.initClusterEnvironment();
  }

  @Test
  public void testConfigNodeLeaderChange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    for (int i = 0; i < 10; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("create database root.ln%s", i))) {
        return;
      }
    }

    try {
      senderEnv.shutdownConfigNode(senderEnv.getLeaderConfigNodeIndex());
    } catch (final Exception e) {
      e.printStackTrace();
      return;
    }

    for (int i = 10; i < 20; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("create database root.ln%s", i))) {
        return;
      }
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "count databases", "count,", Collections.singleton("20,"));
  }

  @Test
  public void testSchemaRegionLeaderChange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> extractorAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    for (int i = 0; i < 10; ++i) {
      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv,
          String.format(
              "create timeSeries root.ln.wf01.GPS.status%s with datatype=BOOLEAN,encoding=PLAIN",
              i))) {
        return;
      }
    }

    final int index;
    try {
      index = senderEnv.getFirstLeaderSchemaRegionDataNodeIndex();
      senderEnv.shutdownDataNode(index);
    } catch (final Throwable e) {
      e.printStackTrace();
      return;
    }

    // Include "alter" in historical transfer for new leader as possible
    // in order to test the "withMerge" creation in receiver side
    if (!TestUtils.tryExecuteNonQueryOnSpecifiedDataNodeWithRetry(
        senderEnv,
        senderEnv.getDataNodeWrapper(index == 0 ? 1 : 0),
        "ALTER timeSeries root.ln.wf01.GPS.status0 UPSERT ALIAS=newAlias TAGS(tag3=v3) ATTRIBUTES(attr4=v4)")) {
      return;
    }

    for (int i = 10; i < 20; ++i) {
      if (!TestUtils.tryExecuteNonQueryOnSpecifiedDataNodeWithRetry(
          senderEnv,
          senderEnv.getDataNodeWrapper(index == 0 ? 1 : 0),
          String.format(
              "create timeSeries root.ln.wf01.GPS.status%s with datatype=BOOLEAN,encoding=PLAIN",
              i))) {
        return;
      }
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show timeSeries root.ln.wf01.GPS.status0",
        "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
        Collections.singleton(
            "root.ln.wf01.GPS.status0,newAlias,root.ln,BOOLEAN,PLAIN,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,"));

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv, "count timeSeries", "count(timeseries),", Collections.singleton("20,"));
  }
}
