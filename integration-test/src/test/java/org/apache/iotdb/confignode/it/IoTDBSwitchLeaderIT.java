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
package org.apache.iotdb.confignode.it;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IoTDBSwitchLeaderIT {

  protected static String originalConfigNodeConsensusProtocolClass;
  protected static String originalSchemaRegionConsensusProtocolClass;
  protected static String originalDataRegionConsensusProtocolClass;
  private static final String testConsensusProtocolClass =
      "org.apache.iotdb.consensus.ratis.RatisConsensus";

  protected static int originalSchemaReplicationFactor;
  protected static int originalDataReplicationFactor;
  private static final int testReplicationFactor = 3;

  private static final int testConfigNodeNum = 3;
  private static final int testDataNodeNum = 3;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    ConfigFactory.getConfig().setConfigNodeConsesusProtocolClass(testConsensusProtocolClass);
    ConfigFactory.getConfig().setSchemaRegionConsensusProtocolClass(testConsensusProtocolClass);
    ConfigFactory.getConfig().setDataRegionConsensusProtocolClass(testConsensusProtocolClass);

    originalSchemaReplicationFactor = ConfigFactory.getConfig().getSchemaReplicationFactor();
    originalDataReplicationFactor = ConfigFactory.getConfig().getDataReplicationFactor();
    ConfigFactory.getConfig().setSchemaReplicationFactor(testReplicationFactor);
    ConfigFactory.getConfig().setDataReplicationFactor(testReplicationFactor);

    // Init 3C3D cluster environment
    EnvFactory.getEnv().initClusterEnvironment(testConfigNodeNum, testDataNodeNum);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanAfterClass();

    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass(originalConfigNodeConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass(originalSchemaRegionConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass(originalDataRegionConsensusProtocolClass);

    ConfigFactory.getConfig().setSchemaReplicationFactor(originalSchemaReplicationFactor);
    ConfigFactory.getConfig().setDataReplicationFactor(originalDataReplicationFactor);
  }

  /** Generate a PatternTree and serialize it into a ByteBuffer */
  private ByteBuffer generatePatternTreeBuffer(String[] paths)
      throws IllegalPathException, IOException {
    PathPatternTree patternTree = new PathPatternTree();
    for (String path : paths) {
      patternTree.appendPathPattern(new PartialPath(path));
    }
    patternTree.constructTree();

    PublicBAOS baos = new PublicBAOS();
    patternTree.serialize(baos);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  @Test
  public void loadStatisticsInheritIT()
      throws IOException, TException, IllegalPathException, InterruptedException {
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";
    final String d00 = sg0 + ".d0.s";
    final String d01 = sg0 + ".d1.s";
    final String d10 = sg1 + ".d0.s";
    final String d11 = sg1 + ".d1.s";

    TSStatus status;
    TSchemaPartitionTableResp schemaPartitionTableResp0;
    TDataPartitionTableResp dataPartitionTableResp0;
    TShowDataNodesResp showDataNodesResp0 = null;
    TShowRegionResp showRegionResp0 = null;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Set StorageGroups
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg0)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg1)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Create SchemaRegionGroups through getOrCreateSchemaPartition
      ByteBuffer buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
      schemaPartitionTableResp0 =
          client.getOrCreateSchemaPartitionTable(
              new TSchemaPartitionReq().setPathPatternTree(buffer));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp0.getStatus().getCode());

      // Create DataRegionGroups through getOrCreateDataPartition
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> seriesSlotMap = new HashMap<>();
      seriesSlotMap.put(
          new TSeriesPartitionSlot(1), Collections.singletonList(new TTimePartitionSlot(100)));
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> sgSlotsMap = new HashMap<>();
      sgSlotsMap.put(sg0, seriesSlotMap);
      sgSlotsMap.put(sg1, seriesSlotMap);
      dataPartitionTableResp0 =
          client.getOrCreateDataPartitionTable(new TDataPartitionReq(sgSlotsMap));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp0.getStatus().getCode());

      // Shutdown a DataNode
      EnvFactory.getEnv().shutdownDataNode(0);
      boolean isDetectedUnknown = false;
      for (int retry = 0; retry < 30; retry++) {
        showDataNodesResp0 = client.showDataNodes();
        showRegionResp0 = client.showRegion(new TShowRegionReq());

        for (TDataNodeInfo dataNodeInfo : showDataNodesResp0.getDataNodesInfoList()) {
          if (NodeStatus.Unknown.getStatus().equals(dataNodeInfo.getStatus())) {
            isDetectedUnknown = true;
            break;
          }
        }
      }
      Assert.assertTrue(isDetectedUnknown);
    }

    // Shutdown all ConfigNodes
    for (int i = 0; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().shutdownConfigNode(i);
    }
    // Sleep 1s before restart
    TimeUnit.SECONDS.sleep(1);
    // Restart all ConfigNodes to switch leader
    for (int i = 0; i < testConfigNodeNum; i++) {
      EnvFactory.getEnv().startConfigNode(i);
    }

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {
      // Check SchemaPartitionTable
      ByteBuffer buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
      Assert.assertEquals(
          schemaPartitionTableResp0,
          client.getSchemaPartitionTable(new TSchemaPartitionReq().setPathPatternTree(buffer)));

      // Check DataPartitionTable
      Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> seriesSlotMap = new HashMap<>();
      seriesSlotMap.put(
          new TSeriesPartitionSlot(1), Collections.singletonList(new TTimePartitionSlot(100)));
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> sgSlotsMap = new HashMap<>();
      sgSlotsMap.put(sg0, seriesSlotMap);
      sgSlotsMap.put(sg1, seriesSlotMap);
      Assert.assertEquals(
          dataPartitionTableResp0, client.getDataPartitionTable(new TDataPartitionReq(sgSlotsMap)));

      // Check cluster tools and loadStatistics
      Assert.assertEquals(showDataNodesResp0, client.showDataNodes());
      Assert.assertEquals(showRegionResp0, client.showRegion(new TShowRegionReq()));
    }
  }
}
