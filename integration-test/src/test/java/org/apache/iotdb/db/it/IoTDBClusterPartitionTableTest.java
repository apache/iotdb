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
package org.apache.iotdb.db.it;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: @MiniSho Move this test into org.apache.iotdb.db.it.confignode package
@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBClusterPartitionTableTest {

  protected static String originalConfigNodeConsensusProtocolClass;
  protected static String originalSchemaRegionConsensusProtocolClass;
  protected static String originalDataRegionConsensusProtocolClass;

  protected static long originalTimePartitionInterval;

  private static final long testTimePartitionInterval = 86400;
  private static final String sg = "root.sg";
  private static final int storageGroupNum = 5;
  private static final int seriesPartitionSlotsNum = 10;
  private static final int timePartitionSlotsNum = 100;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();
    originalTimePartitionInterval = ConfigFactory.getConfig().getTimePartitionInterval();

    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
    ConfigFactory.getConfig().setTimePartitionInterval(testTimePartitionInterval);

    EnvFactory.getEnv().initBeforeClass();
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
    ConfigFactory.getConfig().setTimePartitionInterval(originalTimePartitionInterval);
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
  public void testGetAndCreateSchemaPartition()
      throws TException, IOException, IllegalPathException {
    final String sg = "root.sg";
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    final String d00 = sg0 + ".d0.s";
    final String d01 = sg0 + ".d1.s";
    final String d10 = sg1 + ".d0.s";
    final String d11 = sg1 + ".d1.s";

    final String allPaths = "root.**";
    final String allSg0 = "root.sg0.**";
    final String allSg1 = "root.sg1.**";

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      TSStatus status;
      ByteBuffer buffer;
      TSchemaPartitionReq schemaPartitionReq;
      TSchemaPartitionTableResp schemaPartitionTableResp;
      Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable;

      // Set StorageGroups
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg0)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      status = client.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg1)));
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // Test getSchemaPartition, the result should be empty
      buffer = generatePatternTreeBuffer(new String[] {d00, d01, allSg1});
      schemaPartitionReq = new TSchemaPartitionReq(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(0, schemaPartitionTableResp.getSchemaPartitionTableSize());

      // Test getOrCreateSchemaPartition, ConfigNode should create SchemaPartitions and return
      buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getOrCreateSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(2, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      for (int i = 0; i < 2; i++) {
        Assert.assertTrue(schemaPartitionTable.containsKey(sg + i));
        Assert.assertEquals(2, schemaPartitionTable.get(sg + i).size());
      }

      // Test getSchemaPartition, when a device path doesn't match any StorageGroup and including
      // "**", ConfigNode will return all the SchemaPartitions
      buffer = generatePatternTreeBuffer(new String[] {allPaths});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(2, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      for (int i = 0; i < 2; i++) {
        Assert.assertTrue(schemaPartitionTable.containsKey(sg + i));
        Assert.assertEquals(2, schemaPartitionTable.get(sg + i).size());
      }

      // Test getSchemaPartition, when a device path matches with a StorageGroup and end with "*",
      // ConfigNode will return all the SchemaPartitions in this StorageGroup
      buffer = generatePatternTreeBuffer(new String[] {allSg0, d11});
      schemaPartitionReq.setPathPatternTree(buffer);
      schemaPartitionTableResp = client.getSchemaPartitionTable(schemaPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          schemaPartitionTableResp.getStatus().getCode());
      Assert.assertEquals(2, schemaPartitionTableResp.getSchemaPartitionTableSize());
      schemaPartitionTable = schemaPartitionTableResp.getSchemaPartitionTable();
      // Check "root.sg0"
      Assert.assertTrue(schemaPartitionTable.containsKey(sg0));
      Assert.assertEquals(2, schemaPartitionTable.get(sg0).size());
      // Check "root.sg1"
      Assert.assertTrue(schemaPartitionTable.containsKey(sg1));
      Assert.assertEquals(1, schemaPartitionTable.get(sg1).size());
    }
  }

  private Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      constructPartitionSlotsMap() {
    final String sg = "root.sg";
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result = new HashMap<>();

    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = sg + i;
      result.put(storageGroup, new HashMap<>());
      for (int j = 0; j < seriesPartitionSlotsNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(j);
        result.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
        for (long k = 0; k < timePartitionSlotsNum; k++) {
          TTimePartitionSlot timePartitionSlot =
              new TTimePartitionSlot(k * testTimePartitionInterval);
          result.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);
        }
      }
    }

    return result;
  }

  private void checkDataPartitionMap(
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          dataPartitionTable) {
    Assert.assertEquals(storageGroupNum, dataPartitionTable.size());
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = sg + i;
      Assert.assertTrue(dataPartitionTable.containsKey(storageGroup));
      Assert.assertEquals(seriesPartitionSlotsNum, dataPartitionTable.get(storageGroup).size());
      for (int j = 0; j < seriesPartitionSlotsNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(j);
        Assert.assertTrue(dataPartitionTable.get(storageGroup).containsKey(seriesPartitionSlot));
        Assert.assertEquals(
            timePartitionSlotsNum,
            dataPartitionTable.get(storageGroup).get(seriesPartitionSlot).size());

        Map<TTimePartitionSlot, List<TConsensusGroupId>> timePartitionSlotMap =
            dataPartitionTable.get(storageGroup).get(seriesPartitionSlot);
        for (long k = 0; k < timePartitionSlotsNum; k++) {
          TTimePartitionSlot timePartitionSlot =
              new TTimePartitionSlot(k * testTimePartitionInterval);
          Assert.assertTrue(timePartitionSlotMap.containsKey(timePartitionSlot));
          if (k > 0) {
            // Check consistency
            Assert.assertEquals(
                timePartitionSlotMap.get(new TTimePartitionSlot(0)),
                timePartitionSlotMap.get(timePartitionSlot));
          }
        }
      }
    }
  }

  @Test
  public void testGetAndCreateDataPartition() throws TException, IOException {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      TSStatus status;
      TDataPartitionReq dataPartitionReq;
      TDataPartitionTableResp dataPartitionTableResp;

      // Prepare partitionSlotsMap
      Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
          constructPartitionSlotsMap();

      // Set StorageGroups
      for (int i = 0; i < storageGroupNum; i++) {
        TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
        status = client.setStorageGroup(setReq);
        Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }

      // Test getDataPartitionTable, the result should be empty
      dataPartitionReq = new TDataPartitionReq(partitionSlotsMap);
      dataPartitionTableResp = client.getDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      Assert.assertEquals(0, dataPartitionTableResp.getDataPartitionTableSize());

      // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
      dataPartitionTableResp = client.getOrCreateDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      checkDataPartitionMap(dataPartitionTableResp.getDataPartitionTable());

      // Test getDataPartition, the result should only contain DataPartition created before
      dataPartitionReq.setPartitionSlotsMap(partitionSlotsMap);
      dataPartitionTableResp = client.getDataPartitionTable(dataPartitionReq);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataPartitionTableResp.getStatus().getCode());
      Assert.assertNotNull(dataPartitionTableResp.getDataPartitionTable());
      checkDataPartitionMap(dataPartitionTableResp.getDataPartitionTable());
    }
  }
}
