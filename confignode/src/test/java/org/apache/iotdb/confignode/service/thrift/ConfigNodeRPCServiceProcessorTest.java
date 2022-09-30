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

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
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
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementReq;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetDataReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
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
import java.util.Arrays;
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
    Assert.assertEquals(86400000, storageGroupSchema.getTimePartitionInterval());
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
