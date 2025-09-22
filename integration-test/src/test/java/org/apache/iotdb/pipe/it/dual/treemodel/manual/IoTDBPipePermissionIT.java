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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
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
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipePermissionIT extends AbstractPipeDualTreeModelManualIT {
  @Override
  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // TODO: delete ratis configurations
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setDefaultSchemaRegionGroupNumPerDatabase(1)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    senderEnv.getConfig().getDataNodeConfig().setDataNodeMemoryProportion("3:3:1:1:3:1");
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(false)
        .setTimestampPrecision("ms")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setDataReplicationFactor(2)
        .setDnConnectionTimeoutMs(600000)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment(3, 3);
  }

  @Test
  public void testWithSyncSink() throws Exception {
    testWithSink("iotdb-thrift-sync-sink");
  }

  @Test
  public void testWithAsyncSink() throws Exception {
    testWithSink("iotdb-thrift-async-sink");
  }

  private void testWithSink(final String sink) throws Exception {
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create user `thulab` 'passwd123456'",
            "create role `admin`",
            "grant role `admin` to `thulab`",
            "grant WRITE, READ, SYSTEM, SECURITY on root.** to role `admin`"),
        null);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create user user 'passwd123456'",
              "create timeseries root.ln.wf02.wt01.temperature with datatype=INT64,encoding=PLAIN",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.ln.wf02.wt01(time, temperature, status) values (1800000000000, 23, true)"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");

      sinkAttributes.put("sink", sink);
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.username", "thulab");
      sinkAttributes.put("sink.password", "passwd123456");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "list user",
          "UserId,User,",
          new HashSet<>(Arrays.asList("0,root,", "10001,user,", "10000,thulab,")));
      final Set<String> expectedResSet = new HashSet<>();
      expectedResSet.add(
          "root.ln.wf02.wt01.temperature,null,root.ln,INT64,PLAIN,LZ4,null,null,null,null,BASE,");
      expectedResSet.add(
          "root.ln.wf02.wt01.status,null,root.ln,BOOLEAN,PLAIN,LZ4,null,null,null,null,BASE,");
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show timeseries root.ln.**",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          expectedResSet);
      expectedResSet.clear();

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.ln.**",
          "Time,root.ln.wf02.wt01.temperature,root.ln.wf02.wt01.status,",
          Collections.singleton("1800000000000,23,true,"));
    }
  }

  @Test
  public void testNoPermission() throws Exception {
    TestUtils.executeNonQueries(
        receiverEnv,
        Arrays.asList(
            "create user `thulab` 'passwd123456'",
            "create role `admin`",
            "grant role `admin` to `thulab`",
            "grant READ on root.ln.** to role `admin`"),
        null);

    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      TestUtils.executeNonQueries(
          senderEnv,
          Arrays.asList(
              "create user someUser 'passwd'",
              "create timeseries root.noPermission.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN"),
          null);
      awaitUntilFlush(senderEnv);

      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");

      sinkAttributes.put("sink", "iotdb-thrift-async-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.username", "thulab");
      sinkAttributes.put("sink.password", "passwd123456");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show timeseries",
          "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
          Collections.emptySet());
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "list user", "User,", Collections.singleton("root,"));
    }
  }
}
