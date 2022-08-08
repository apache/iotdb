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
package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeStartupCheck;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.ratis.util.FileUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigNodeRPCServiceProcessorTest {

  ConfigNodeRPCServiceProcessor processor;

  @BeforeClass
  public static void beforeClass() throws StartupException, ConfigurationException, IOException {
    final ConfigNodeConfig configNodeConfig = ConfigNodeDescriptor.getInstance().getConf();
    UDFExecutableManager.setupAndGetInstance(
        configNodeConfig.getTemporaryLibDir(), configNodeConfig.getUdfLibDir());
    UDFClassLoaderManager.setupAndGetInstance(configNodeConfig.getUdfLibDir());
    UDFRegistrationService.setupAndGetInstance(configNodeConfig.getSystemUdfDir());
    ConfigNodeStartupCheck.getInstance().startUpCheck();
  }

  @Before
  public void before() throws IOException {
    processor = new ConfigNodeRPCServiceProcessor(new ConfigManager());
    processor.getConsensusManager().singleCopyMayWaitUntilLeaderReady();
  }

  @After
  public void after() throws IOException {
    processor.close();
    FileUtils.deleteFully(new File(ConfigNodeDescriptor.getInstance().getConf().getConsensusDir()));
    FileUtils.deleteFully(
        new File(CommonDescriptor.getInstance().getConfig().getProcedureWalFolder()));
  }

  @AfterClass
  public static void afterClass() throws IOException {
    UDFExecutableManager.getInstance().stop();
    UDFClassLoaderManager.getInstance().stop();
    UDFRegistrationService.getInstance().stop();
    FileUtils.deleteFully(new File(ConfigNodeConstant.DATA_DIR));
  }

  private void checkGlobalConfig(TGlobalConfig globalConfig) {
    Assert.assertEquals(
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass(),
        globalConfig.getDataRegionConsensusProtocolClass());
    Assert.assertEquals(
        ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionConsensusProtocolClass(),
        globalConfig.getSchemaRegionConsensusProtocolClass());
    Assert.assertEquals(
        ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionSlotNum(),
        globalConfig.getSeriesPartitionSlotNum());
    Assert.assertEquals(
        ConfigNodeDescriptor.getInstance().getConf().getSeriesPartitionExecutorClass(),
        globalConfig.getSeriesPartitionExecutorClass());
  }

  private void registerDataNodes() throws TException {
    for (int i = 0; i < 3; i++) {
      TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
      dataNodeLocation.setDataNodeId(-1);
      dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667 + i));
      dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 9003 + i));
      dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 8777 + i));
      dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 40010 + i));
      dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 50010 + i));

      TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
      dataNodeConfiguration.setLocation(dataNodeLocation);
      dataNodeConfiguration.setResource(new TNodeResource(8, 1024 * 1024));

      TDataNodeRegisterReq req = new TDataNodeRegisterReq(dataNodeConfiguration);
      TDataNodeRegisterResp resp = processor.registerDataNode(req);

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      Assert.assertEquals(i, resp.getDataNodeId());
      checkGlobalConfig(resp.getGlobalConfig());
    }
  }

  @Test
  public void testSetAndQueryStorageGroup() throws IllegalPathException, TException {
    TSStatus status;
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";

    // register DataNodes
    registerDataNodes();

    // set StorageGroup0 by default values
    TSetStorageGroupReq setReq0 = new TSetStorageGroupReq(new TStorageGroupSchema(sg0));
    status = processor.setStorageGroup(setReq0);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // set StorageGroup1 by specific values
    TSetStorageGroupReq setReq1 =
        new TSetStorageGroupReq(
            new TStorageGroupSchema(sg1)
                .setTTL(1024L)
                .setSchemaReplicationFactor(5)
                .setDataReplicationFactor(5)
                .setTimePartitionInterval(2048L));
    status = processor.setStorageGroup(setReq1);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // test count all StorageGroups
    TCountStorageGroupResp countResp =
        processor.countMatchedStorageGroups(Arrays.asList("root", "**"));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), countResp.getStatus().getCode());
    Assert.assertEquals(2, countResp.getCount());

    // test count one StorageGroup
    countResp = processor.countMatchedStorageGroups(Arrays.asList("root", "sg0"));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), countResp.getStatus().getCode());
    Assert.assertEquals(1, countResp.getCount());

    // test query all StorageGroupSchemas
    TStorageGroupSchemaResp getResp =
        processor.getMatchedStorageGroupSchemas(Arrays.asList("root", "**"));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), getResp.getStatus().getCode());
    Map<String, TStorageGroupSchema> schemaMap = getResp.getStorageGroupSchemaMap();
    Assert.assertEquals(2, schemaMap.size());
    TStorageGroupSchema storageGroupSchema = schemaMap.get(sg0);
    Assert.assertNotNull(storageGroupSchema);
    Assert.assertEquals(sg0, storageGroupSchema.getName());
    Assert.assertEquals(Long.MAX_VALUE, storageGroupSchema.getTTL());
    Assert.assertEquals(1, storageGroupSchema.getSchemaReplicationFactor());
    Assert.assertEquals(1, storageGroupSchema.getDataReplicationFactor());
    Assert.assertEquals(604800, storageGroupSchema.getTimePartitionInterval());
    storageGroupSchema = schemaMap.get(sg1);
    Assert.assertNotNull(storageGroupSchema);
    Assert.assertEquals(sg1, storageGroupSchema.getName());
    Assert.assertEquals(1024L, storageGroupSchema.getTTL());
    Assert.assertEquals(5, storageGroupSchema.getSchemaReplicationFactor());
    Assert.assertEquals(5, storageGroupSchema.getDataReplicationFactor());
    Assert.assertEquals(2048L, storageGroupSchema.getTimePartitionInterval());

    // test fail by re-register
    status = processor.setStorageGroup(setReq0);
    Assert.assertEquals(
        TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode(), status.getCode());

    // test StorageGroup setter interfaces
    PartialPath patternPath = new PartialPath(sg1);
    status =
        processor.setTTL(new TSetTTLReq(Arrays.asList(patternPath.getNodes()), Long.MAX_VALUE));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.setSchemaReplicationFactor(new TSetSchemaReplicationFactorReq(sg1, 1));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.setDataReplicationFactor(new TSetDataReplicationFactorReq(sg1, 1));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.setTimePartitionInterval(new TSetTimePartitionIntervalReq(sg1, 604800L));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // test setter results
    getResp = processor.getMatchedStorageGroupSchemas(Arrays.asList("root", "sg1"));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), getResp.getStatus().getCode());
    schemaMap = getResp.getStorageGroupSchemaMap();
    Assert.assertEquals(1, schemaMap.size());
    storageGroupSchema = schemaMap.get(sg1);
    Assert.assertNotNull(storageGroupSchema);
    Assert.assertEquals(sg1, storageGroupSchema.getName());
    Assert.assertEquals(Long.MAX_VALUE, storageGroupSchema.getTTL());
    Assert.assertEquals(1, storageGroupSchema.getSchemaReplicationFactor());
    Assert.assertEquals(1, storageGroupSchema.getDataReplicationFactor());
    Assert.assertEquals(604800, storageGroupSchema.getTimePartitionInterval());
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

    TSStatus status;
    ByteBuffer buffer;
    TSchemaPartitionReq schemaPartitionReq;
    TSchemaPartitionResp schemaPartitionResp;

    Map<String, Map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaPartitionMap;

    // Set StorageGroups
    status = processor.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg0)));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    status = processor.setStorageGroup(new TSetStorageGroupReq(new TStorageGroupSchema(sg1)));
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    // Test getOrCreateSchemaPartition, the result should be NOT_ENOUGH_DATANODE
    buffer = generatePatternTreeBuffer(new String[] {d00, d01, allSg1});
    schemaPartitionReq = new TSchemaPartitionReq(buffer);
    schemaPartitionResp = processor.getOrCreateSchemaPartition(schemaPartitionReq);
    Assert.assertEquals(
        TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode(),
        schemaPartitionResp.getStatus().getCode());
    Assert.assertNull(schemaPartitionResp.getSchemaRegionMap());

    // register DataNodes
    registerDataNodes();

    // Test getSchemaPartition, the result should be empty
    buffer = generatePatternTreeBuffer(new String[] {d00, d01, allSg1});
    schemaPartitionReq = new TSchemaPartitionReq(buffer);
    schemaPartitionResp = processor.getSchemaPartition(schemaPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), schemaPartitionResp.getStatus().getCode());
    Assert.assertEquals(0, schemaPartitionResp.getSchemaRegionMapSize());

    // Test getOrCreateSchemaPartition, ConfigNode should create SchemaPartitions and return
    buffer = generatePatternTreeBuffer(new String[] {d00, d01, d10, d11});
    schemaPartitionReq.setPathPatternTree(buffer);
    schemaPartitionResp = processor.getOrCreateSchemaPartition(schemaPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), schemaPartitionResp.getStatus().getCode());
    Assert.assertEquals(2, schemaPartitionResp.getSchemaRegionMapSize());
    schemaPartitionMap = schemaPartitionResp.getSchemaRegionMap();
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(schemaPartitionMap.containsKey(sg + i));
      Assert.assertEquals(2, schemaPartitionMap.get(sg + i).size());
      schemaPartitionMap
          .get(sg + i)
          .forEach(
              (tSeriesPartitionSlot, tRegionReplicaSet) -> {
                Assert.assertEquals(1, tRegionReplicaSet.getDataNodeLocationsSize());
                Assert.assertEquals(
                    TConsensusGroupType.SchemaRegion, tRegionReplicaSet.getRegionId().getType());
              });
    }

    // Test getSchemaPartition, when a device path doesn't match any StorageGroup and including
    // "**",
    // ConfigNode will return all the SchemaPartitions
    buffer = generatePatternTreeBuffer(new String[] {allPaths});
    schemaPartitionReq.setPathPatternTree(buffer);
    schemaPartitionResp = processor.getSchemaPartition(schemaPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), schemaPartitionResp.getStatus().getCode());
    Assert.assertEquals(2, schemaPartitionResp.getSchemaRegionMapSize());
    schemaPartitionMap = schemaPartitionResp.getSchemaRegionMap();
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(schemaPartitionMap.containsKey(sg + i));
      Assert.assertEquals(2, schemaPartitionMap.get(sg + i).size());
      schemaPartitionMap
          .get(sg + i)
          .forEach(
              (tSeriesPartitionSlot, tRegionReplicaSet) -> {
                Assert.assertEquals(1, tRegionReplicaSet.getDataNodeLocationsSize());
                Assert.assertEquals(
                    TConsensusGroupType.SchemaRegion, tRegionReplicaSet.getRegionId().getType());
              });
    }

    // Test getSchemaPartition, when a device path matches with a StorageGroup and end with "*",
    // ConfigNode will return all the SchemaPartitions in this StorageGroup
    buffer = generatePatternTreeBuffer(new String[] {allSg0, d11});
    schemaPartitionReq.setPathPatternTree(buffer);
    schemaPartitionResp = processor.getSchemaPartition(schemaPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), schemaPartitionResp.getStatus().getCode());
    Assert.assertEquals(2, schemaPartitionResp.getSchemaRegionMapSize());
    schemaPartitionMap = schemaPartitionResp.getSchemaRegionMap();
    // Check "root.sg0"
    Assert.assertTrue(schemaPartitionMap.containsKey(sg0));
    Assert.assertEquals(2, schemaPartitionMap.get(sg0).size());
    schemaPartitionMap
        .get(sg0)
        .forEach(
            (tSeriesPartitionSlot, tRegionReplicaSet) -> {
              Assert.assertEquals(1, tRegionReplicaSet.getDataNodeLocationsSize());
              Assert.assertEquals(
                  TConsensusGroupType.SchemaRegion, tRegionReplicaSet.getRegionId().getType());
            });
    // Check "root.sg1"
    Assert.assertTrue(schemaPartitionMap.containsKey(sg1));
    Assert.assertEquals(1, schemaPartitionMap.get(sg1).size());
    schemaPartitionMap
        .get(sg1)
        .forEach(
            (tSeriesPartitionSlot, tRegionReplicaSet) -> {
              Assert.assertEquals(1, tRegionReplicaSet.getDataNodeLocationsSize());
              Assert.assertEquals(
                  TConsensusGroupType.SchemaRegion, tRegionReplicaSet.getRegionId().getType());
            });
  }

  private Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>>
      constructPartitionSlotsMap(
          int storageGroupNum, int seriesPartitionSlotNum, long timePartitionSlotNum) {
    final String sg = "root.sg";
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> result = new HashMap<>();

    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = sg + i;
      result.put(storageGroup, new HashMap<>());
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(j);
        result.get(storageGroup).put(seriesPartitionSlot, new ArrayList<>());
        for (long k = 0; k < timePartitionSlotNum; k++) {
          TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(k);
          result.get(storageGroup).get(seriesPartitionSlot).add(timePartitionSlot);
        }
      }
    }

    return result;
  }

  private void checkDataPartitionMap(
      int storageGroupNum,
      int seriesPartitionSlotNum,
      long timePartitionSlotNum,
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TRegionReplicaSet>>>>
          dataPartitionMap) {
    final String sg = "root.sg";
    Assert.assertEquals(storageGroupNum, dataPartitionMap.size());
    for (int i = 0; i < storageGroupNum; i++) {
      String storageGroup = sg + i;
      Assert.assertTrue(dataPartitionMap.containsKey(storageGroup));
      Assert.assertEquals(seriesPartitionSlotNum, dataPartitionMap.get(storageGroup).size());
      for (int j = 0; j < seriesPartitionSlotNum; j++) {
        TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(j);
        Assert.assertTrue(dataPartitionMap.get(storageGroup).containsKey(seriesPartitionSlot));
        Assert.assertEquals(
            timePartitionSlotNum,
            dataPartitionMap.get(storageGroup).get(seriesPartitionSlot).size());
        for (long k = 0; k < timePartitionSlotNum; k++) {
          TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(k);
          Assert.assertTrue(
              dataPartitionMap
                  .get(storageGroup)
                  .get(seriesPartitionSlot)
                  .containsKey(timePartitionSlot));
          // One RegionReplicaSet
          Assert.assertEquals(
              1,
              dataPartitionMap
                  .get(storageGroup)
                  .get(seriesPartitionSlot)
                  .get(timePartitionSlot)
                  .size());
          // Is DataRegion
          Assert.assertEquals(
              TConsensusGroupType.DataRegion,
              dataPartitionMap
                  .get(storageGroup)
                  .get(seriesPartitionSlot)
                  .get(timePartitionSlot)
                  .get(0)
                  .getRegionId()
                  .getType());
          // Including one RegionReplica
          Assert.assertEquals(
              1,
              dataPartitionMap
                  .get(storageGroup)
                  .get(seriesPartitionSlot)
                  .get(timePartitionSlot)
                  .get(0)
                  .getDataNodeLocationsSize());
        }
      }
    }
  }

  @Test
  public void testGetAndCreateDataPartition() throws TException {
    final String sg = "root.sg";
    final int storageGroupNum = 2;
    final int seriesPartitionSlotNum = 4;
    final long timePartitionSlotNum = 6;

    TSStatus status;
    TDataPartitionReq dataPartitionReq;
    TDataPartitionResp dataPartitionResp;

    // Prepare partitionSlotsMap
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap0 =
        constructPartitionSlotsMap(storageGroupNum, seriesPartitionSlotNum, timePartitionSlotNum);
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap1 =
        constructPartitionSlotsMap(
            storageGroupNum * 2, seriesPartitionSlotNum * 2, timePartitionSlotNum * 2);

    // set StorageGroups
    for (int i = 0; i < storageGroupNum; i++) {
      TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
      status = processor.setStorageGroup(setReq);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    // Test getOrCreateDataPartition, the result should be NOT_ENOUGH_DATANODE
    dataPartitionReq = new TDataPartitionReq(partitionSlotsMap0);
    dataPartitionResp = processor.getOrCreateDataPartition(dataPartitionReq);
    Assert.assertEquals(
        TSStatusCode.NOT_ENOUGH_DATA_NODE.getStatusCode(), dataPartitionResp.getStatus().getCode());
    Assert.assertNull(dataPartitionResp.getDataPartitionMap());

    // register DataNodes
    registerDataNodes();

    // Test getDataPartition, the result should be empty
    dataPartitionReq = new TDataPartitionReq(partitionSlotsMap0);
    dataPartitionResp = processor.getDataPartition(dataPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), dataPartitionResp.getStatus().getCode());
    Assert.assertNotNull(dataPartitionResp.getDataPartitionMap());
    Assert.assertEquals(0, dataPartitionResp.getDataPartitionMapSize());

    // Test getOrCreateDataPartition, ConfigNode should create DataPartition and return
    dataPartitionResp = processor.getOrCreateDataPartition(dataPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), dataPartitionResp.getStatus().getCode());
    Assert.assertNotNull(dataPartitionResp.getDataPartitionMap());
    checkDataPartitionMap(
        storageGroupNum,
        seriesPartitionSlotNum,
        timePartitionSlotNum,
        dataPartitionResp.getDataPartitionMap());

    // Test getDataPartition, the result should only contain DataPartition created before
    dataPartitionReq.setPartitionSlotsMap(partitionSlotsMap1);
    dataPartitionResp = processor.getDataPartition(dataPartitionReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), dataPartitionResp.getStatus().getCode());
    Assert.assertNotNull(dataPartitionResp.getDataPartitionMap());
    checkDataPartitionMap(
        storageGroupNum,
        seriesPartitionSlotNum,
        timePartitionSlotNum,
        dataPartitionResp.getDataPartitionMap());
  }

  @Test
  public void testDeleteStorageGroup() throws TException {
    TSStatus status;
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";
    // register DataNodes
    registerDataNodes();
    ConfigNodeProcedureEnv.setSkipForTest(true);
    TSetStorageGroupReq setReq0 = new TSetStorageGroupReq(new TStorageGroupSchema(sg0));
    // set StorageGroup0 by default values
    status = processor.setStorageGroup(setReq0);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    // set StorageGroup1 by specific values
    TSetStorageGroupReq setReq1 = new TSetStorageGroupReq(new TStorageGroupSchema(sg1));
    status = processor.setStorageGroup(setReq1);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    TDeleteStorageGroupsReq deleteStorageGroupsReq = new TDeleteStorageGroupsReq();
    List<String> sgs = Arrays.asList(sg0, sg1);
    deleteStorageGroupsReq.setPrefixPathList(sgs);
    TSStatus deleteSgStatus = processor.deleteStorageGroups(deleteStorageGroupsReq);
    TStorageGroupSchemaResp root =
        processor.getMatchedStorageGroupSchemas(Arrays.asList("root", "*"));
    Assert.assertTrue(root.getStorageGroupSchemaMap().isEmpty());
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), deleteSgStatus.getCode());
  }

  @Test
  public void testDeleteStorageGroupInvalidateCacheFailed() throws TException {
    TSStatus status;
    final String sg0 = "root.sg0";
    final String sg1 = "root.sg1";
    // register DataNodes
    registerDataNodes();
    ConfigNodeProcedureEnv.setSkipForTest(true);
    ConfigNodeProcedureEnv.setInvalidCacheResult(false);
    TSetStorageGroupReq setReq0 = new TSetStorageGroupReq(new TStorageGroupSchema(sg0));
    // set StorageGroup0 by default values
    status = processor.setStorageGroup(setReq0);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    // set StorageGroup1 by specific values
    TSetStorageGroupReq setReq1 = new TSetStorageGroupReq(new TStorageGroupSchema(sg1));
    status = processor.setStorageGroup(setReq1);
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    TDeleteStorageGroupsReq deleteStorageGroupsReq = new TDeleteStorageGroupsReq();
    List<String> sgs = Arrays.asList(sg0, sg1);
    deleteStorageGroupsReq.setPrefixPathList(sgs);
    TSStatus deleteSgStatus = processor.deleteStorageGroups(deleteStorageGroupsReq);
    TStorageGroupSchemaResp root =
        processor.getMatchedStorageGroupSchemas(Arrays.asList("root", "*"));
    // rollback success
    Assert.assertEquals(root.getStorageGroupSchemaMap().size(), 2);
    Assert.assertEquals(TSStatusCode.MULTIPLE_ERROR.getStatusCode(), deleteSgStatus.getCode());
  }

  @Test
  public void testGetSchemaNodeManagementPartition()
      throws TException, IllegalPathException, IOException {
    final String sg = "root.sg";
    final int storageGroupNum = 2;

    TSStatus status;
    TSchemaNodeManagementReq nodeManagementReq;
    TSchemaNodeManagementResp nodeManagementResp;

    // register DataNodes
    registerDataNodes();

    // set StorageGroups
    for (int i = 0; i < storageGroupNum; i++) {
      TSetStorageGroupReq setReq = new TSetStorageGroupReq(new TStorageGroupSchema(sg + i));
      status = processor.setStorageGroup(setReq);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }

    ByteBuffer byteBuffer = generatePatternTreeBuffer(new String[] {"root"});
    nodeManagementReq = new TSchemaNodeManagementReq(byteBuffer);
    nodeManagementReq.setLevel(-1);
    nodeManagementResp = processor.getSchemaNodeManagementPartition(nodeManagementReq);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), nodeManagementResp.getStatus().getCode());
    Assert.assertEquals(2, nodeManagementResp.getMatchedNodeSize());
    Assert.assertNotNull(nodeManagementResp.getSchemaRegionMap());
    Assert.assertEquals(0, nodeManagementResp.getSchemaRegionMapSize());
  }
}
