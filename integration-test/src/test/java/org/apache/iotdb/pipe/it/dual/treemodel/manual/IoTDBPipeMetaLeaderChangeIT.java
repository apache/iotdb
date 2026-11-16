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
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.cluster.env.AbstractEnv;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTreeManual;
import org.apache.iotdb.rpc.TSStatusCode;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTreeManual.class})
public class IoTDBPipeMetaLeaderChangeIT extends AbstractPipeTreeModelManualSharedClusterIT {

  private static final String CONFIG_DATABASE_PREFIX = "root.meta_leader_config";
  private static final String SCHEMA_DATABASE = "root.meta_leader_schema";

  private int stoppedConfigNodeIndex = -1;
  private int stoppedDataNodeIndex = -1;

  @Override
  protected void setupConfig() {
    senderEnv
        .getConfig()
        .getCommonConfig()
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(3)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv
        .getConfig()
        .getCommonConfig()
        .setDatanodeMemoryProportion("3:3:1:1:1:0")
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setDnConnectionTimeoutMs(600000);
  }

  @Override
  protected CompletableFuture<Void> initializeClusters() {
    // The scenarios need three sender replicas for leader failover, with the original receiver
    // topology. Only their initial startup is independent; restore stopped nodes between methods.
    return CompletableFuture.allOf(
        CompletableFuture.runAsync(() -> senderEnv.initClusterEnvironment(3, 3, 180)),
        CompletableFuture.runAsync(receiverEnv::initClusterEnvironment));
  }

  @Override
  protected void cleanEnvironment() {
    // Restore the failed node before resetting pipe/data state: cleanup must reach every replica,
    // and the next scenario must start with the full topology even if this test's assertions fail.
    if (stoppedConfigNodeIndex >= 0) {
      if (!senderEnv.getConfigNodeWrapper(stoppedConfigNodeIndex).isAlive()) {
        senderEnv.startConfigNode(stoppedConfigNodeIndex);
      }
      ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
    }
    if (stoppedDataNodeIndex >= 0) {
      if (!senderEnv.getDataNodeWrapper(stoppedDataNodeIndex).isAlive()) {
        senderEnv.startDataNode(stoppedDataNodeIndex);
      }
      ((AbstractEnv) senderEnv).checkClusterStatusWithoutUnknown();
    }
    super.cleanEnvironment();
  }

  // Create ten databases, stop the ConfigNode leader, then create ten more through the new leader;
  // the receiver must contain all twenty under this scenario's database prefix.
  @Test
  public void testConfigNodeLeaderChange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    createDatabases(0, 10);

    stoppedConfigNodeIndex = senderEnv.getLeaderConfigNodeIndex();
    senderEnv.shutdownConfigNode(stoppedConfigNodeIndex);
    Assert.assertFalse(senderEnv.getConfigNodeWrapper(stoppedConfigNodeIndex).isAlive());
    Assert.assertNotEquals(stoppedConfigNodeIndex, senderEnv.getLeaderConfigNodeIndex());

    createDatabases(10, 20);

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count databases " + CONFIG_DATABASE_PREFIX + "*",
        "count,",
        Collections.singleton("20,"));
  }

  // Stop the SchemaRegion leader after ten timeseries, then alter the first and create ten more.
  // Failover must preserve all twenty timeseries and merge the alias, tags and attributes remotely.
  @Test
  public void testSchemaRegionLeaderChange() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    final String receiverIp = receiverDataNode.getIp();
    final int receiverPort = receiverDataNode.getPort();

    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      final Map<String, String> processorAttributes = new HashMap<>();
      final Map<String, String> sinkAttributes = new HashMap<>();

      sourceAttributes.put("source.inclusion", "all");
      sourceAttributes.put("source.inclusion.exclusion", "");
      sourceAttributes.put("source.forwarding-pipe-requests", "false");
      sourceAttributes.put("user", SessionConfig.DEFAULT_USER);

      sinkAttributes.put("sink", "iotdb-thrift-sink");
      sinkAttributes.put("sink.ip", receiverIp);
      sinkAttributes.put("sink.port", Integer.toString(receiverPort));
      sinkAttributes.put("sink.exception.conflict.resolve-strategy", "retry");
      sinkAttributes.put("sink.exception.conflict.retry-max-time-seconds", "-1");

      final TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", sinkAttributes)
                  .setExtractorAttributes(sourceAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    // Close this phase's session before stopping a node; client reconnection is not under test.
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = 0; i < 10; ++i) {
        session.executeNonQueryStatement(createTimeseriesStatement(i));
      }
    }

    stoppedDataNodeIndex = senderEnv.getFirstLeaderSchemaRegionDataNodeIndex();
    Assert.assertTrue(stoppedDataNodeIndex >= 0);
    senderEnv.shutdownDataNode(stoppedDataNodeIndex);
    Assert.assertFalse(senderEnv.getDataNodeWrapper(stoppedDataNodeIndex).isAlive());
    // Observe a replacement leader before sending mutations, avoiding a fixed election sleep.
    Awaitility.await()
        .pollInSameThread()
        .pollDelay(0, TimeUnit.MILLISECONDS)
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(180, TimeUnit.SECONDS)
        .until(
            () -> {
              final int leaderIndex = senderEnv.getFirstLeaderSchemaRegionDataNodeIndex();
              return leaderIndex >= 0 && leaderIndex != stoppedDataNodeIndex;
            });

    // Include "alter" in historical transfer for new leader as possible
    // in order to test the "withMerge" creation in receiver side
    // Use one session on a surviving node after election. The session's RPC retries apply only to
    // the current statement, so an error cannot replay earlier successful CREATE statements.
    final DataNodeWrapper survivingDataNode =
        senderEnv.getDataNodeWrapper(stoppedDataNodeIndex == 0 ? 1 : 0);
    try (final ISession session =
        senderEnv.getSessionConnection(
            Collections.singletonList(survivingDataNode.getIpAndPortString()))) {
      session.executeNonQueryStatement(
          "ALTER timeSeries "
              + SCHEMA_DATABASE
              + ".wf01.GPS.status0 UPSERT ALIAS=newAlias TAGS(tag3=v3) ATTRIBUTES(attr4=v4)");
      for (int i = 10; i < 20; ++i) {
        session.executeNonQueryStatement(createTimeseriesStatement(i));
      }
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "show timeSeries " + SCHEMA_DATABASE + ".wf01.GPS.status0",
        "Timeseries,Alias,Database,DataType,Encoding,Compression,Tags,Attributes,Deadband,DeadbandParameters,ViewType,",
        Collections.singleton(
            SCHEMA_DATABASE
                + ".wf01.GPS.status0,newAlias,"
                + SCHEMA_DATABASE
                + ",BOOLEAN,PLAIN,LZ4,{\"tag3\":\"v3\"},{\"attr4\":\"v4\"},null,null,BASE,"));

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeSeries " + SCHEMA_DATABASE + ".**",
        "count(timeseries),",
        Collections.singleton("20,"));
  }

  private void createDatabases(final int startInclusive, final int endExclusive) throws Exception {
    // Reuse one session per phase, closing it before ConfigNode failover.
    try (final ISession session = senderEnv.getSessionConnection()) {
      for (int i = startInclusive; i < endExclusive; ++i) {
        session.executeNonQueryStatement("create database " + CONFIG_DATABASE_PREFIX + i);
      }
    }
  }

  private static String createTimeseriesStatement(final int index) {
    return String.format(
        "create timeSeries %s.wf01.GPS.status%d with datatype=BOOLEAN,encoding=PLAIN",
        SCHEMA_DATABASE, index);
  }
}
