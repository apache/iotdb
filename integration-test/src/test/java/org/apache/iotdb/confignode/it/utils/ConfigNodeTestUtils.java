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
package org.apache.iotdb.confignode.it.utils;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAdvancedClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TBasicClusterParameters;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.itbase.env.BaseConfig;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigNodeTestUtils {

  private static final BaseConfig CONF = ConfigFactory.getConfig();

  private static final int retryNum = 30;

  public static TShowClusterResp getClusterNodeInfos(
      IConfigNodeRPCService.Iface client, int expectedConfigNodeNum, int expectedDataNodeNum)
      throws TException, InterruptedException {
    TShowClusterResp clusterNodes = null;
    for (int i = 0; i < retryNum; i++) {
      clusterNodes = client.showCluster();
      if (clusterNodes.getConfigNodeListSize() == expectedConfigNodeNum
          && clusterNodes.getDataNodeListSize() == expectedDataNodeNum) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals(expectedConfigNodeNum, clusterNodes.getConfigNodeListSize());
    assertEquals(expectedDataNodeNum, clusterNodes.getDataNodeListSize());

    return clusterNodes;
  }

  public static void checkNodeConfig(
      List<TConfigNodeLocation> configNodeList,
      List<TDataNodeLocation> dataNodeList,
      List<ConfigNodeWrapper> configNodeWrappers,
      List<DataNodeWrapper> dataNodeWrappers) {
    // check ConfigNode
    for (TConfigNodeLocation configNodeLocation : configNodeList) {
      boolean found = false;
      for (ConfigNodeWrapper configNodeWrapper : configNodeWrappers) {
        if (configNodeWrapper.getIp().equals(configNodeLocation.getInternalEndPoint().getIp())
            && configNodeWrapper.getPort() == configNodeLocation.getInternalEndPoint().getPort()
            && configNodeWrapper.getConsensusPort()
                == configNodeLocation.getConsensusEndPoint().getPort()) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }

    // check DataNode
    for (TDataNodeLocation dataNodeLocation : dataNodeList) {
      boolean found = false;
      for (DataNodeWrapper dataNodeWrapper : dataNodeWrappers) {
        if (dataNodeWrapper.getIp().equals(dataNodeLocation.getClientRpcEndPoint().getIp())
            && dataNodeWrapper.getPort() == dataNodeLocation.getClientRpcEndPoint().getPort()
            && dataNodeWrapper.getInternalPort() == dataNodeLocation.getInternalEndPoint().getPort()
            && dataNodeWrapper.getSchemaRegionConsensusPort()
                == dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort()
            && dataNodeWrapper.getDataRegionConsensusPort()
                == dataNodeLocation.getDataRegionConsensusEndPoint().getPort()) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }

  /** Generate a PatternTree and serialize it into a ByteBuffer */
  public static ByteBuffer generatePatternTreeBuffer(String[] paths)
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

  public static Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> constructPartitionSlotsMap(
      String storageGroup,
      int seriesSlotStart,
      int seriesSlotEnd,
      long timeSlotStart,
      long timeSlotEnd,
      long timePartitionInterval) {
    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> result = new HashMap<>();
    result.put(storageGroup, new HashMap<>());

    for (int i = seriesSlotStart; i < seriesSlotEnd; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      result
          .get(storageGroup)
          .put(seriesPartitionSlot, new TTimeSlotList().setTimePartitionSlots(new ArrayList<>()));
      for (long j = timeSlotStart; j < timeSlotEnd; j++) {
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(j * timePartitionInterval);
        result
            .get(storageGroup)
            .get(seriesPartitionSlot)
            .getTimePartitionSlots()
            .add(timePartitionSlot);
      }
    }

    return result;
  }

  public static void checkDataPartitionTable(
      String storageGroup,
      int seriesSlotStart,
      int seriesSlotEnd,
      long timeSlotStart,
      long timeSlotEnd,
      long timePartitionInterval,
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          dataPartitionTable) {

    // Check the existence of StorageGroup
    Assert.assertTrue(dataPartitionTable.containsKey(storageGroup));

    // Check the number of SeriesPartitionSlot
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
        seriesPartitionTable = dataPartitionTable.get(storageGroup);
    Assert.assertEquals(seriesSlotEnd - seriesSlotStart, seriesPartitionTable.size());

    for (int i = seriesSlotStart; i < seriesSlotEnd; i++) {
      // Check the existence of SeriesPartitionSlot
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      Assert.assertTrue(seriesPartitionTable.containsKey(seriesPartitionSlot));

      // Check the number of TimePartitionSlot
      Map<TTimePartitionSlot, List<TConsensusGroupId>> timePartitionTable =
          seriesPartitionTable.get(seriesPartitionSlot);
      Assert.assertEquals(timeSlotEnd - timeSlotStart, timePartitionTable.size());

      for (long j = timeSlotStart; j < timeSlotEnd; j++) {
        // Check the existence of TimePartitionSlot
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(j * timePartitionInterval);
        Assert.assertTrue(timePartitionTable.containsKey(timePartitionSlot));
      }
    }
  }

  public static TConfigNodeLocation generateTConfigNodeLocation(
      int nodeId, ConfigNodeWrapper configNodeWrapper) {
    return new TConfigNodeLocation(
        nodeId,
        new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()),
        new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getConsensusPort()));
  }

  /**
   * Generate a ConfigNode register request with the given ConfigNodeLocation and default
   * configurations
   *
   * @param configNodeWrapper The given ConfigNode
   * @return TConfigNodeRegisterReq for the given ConfigNode
   */
  public static TConfigNodeRegisterReq generateTConfigNodeRegisterReq(
      ConfigNodeWrapper configNodeWrapper) {
    return new TConfigNodeRegisterReq()
        .setConfigNodeLocation(generateTConfigNodeLocation(-1, configNodeWrapper))
        .setBasicParameters(generateTBasicClusterParameters())
        .setAdvancedParameters(generateTAdvancedClusterParameters());
  }

  private static TBasicClusterParameters generateTBasicClusterParameters() {
    TBasicClusterParameters basicClusterParameters = new TBasicClusterParameters();
    basicClusterParameters.setConfigNodeConsensusProtocolClass(
        CONF.getConfigNodeConsensusProtocolClass());
    basicClusterParameters.setDataRegionConsensusProtocolClass(
        CONF.getDataRegionConsensusProtocolClass());
    basicClusterParameters.setSchemaRegionConsensusProtocolClass(
        CONF.getSchemaRegionConsensusProtocolClass());
    basicClusterParameters.setSeriesPartitionSlotNum(CONF.getSeriesPartitionSlotNum());
    basicClusterParameters.setSeriesPartitionExecutorClass(CONF.getSeriesPartitionExecutorClass());
    basicClusterParameters.setDefaultTTL(CONF.getDefaultTTL());
    basicClusterParameters.setTimePartitionInterval(CONF.getTimePartitionInterval());
    basicClusterParameters.setDataReplicationFactor(CONF.getDataReplicationFactor());
    basicClusterParameters.setSchemaReplicationFactor(CONF.getSchemaReplicationFactor());
    return basicClusterParameters;
  }

  private static TAdvancedClusterParameters generateTAdvancedClusterParameters() {
    TAdvancedClusterParameters advancedClusterParameters = new TAdvancedClusterParameters();
    advancedClusterParameters.setDataRegionPerProcessor(1.0);
    advancedClusterParameters.setSchemaRegionPerDataNode(1.0);
    advancedClusterParameters.setDiskSpaceWarningThreshold(0.05);
    advancedClusterParameters.setReadConsistencyLevel("strong");
    advancedClusterParameters.setLeastDataRegionGroupNum(CONF.getLeastDataRegionGroupNum());
    return advancedClusterParameters;
  }

  public static TConfigNodeRestartReq generateTConfigNodeRestartReq(
      String clusterName, int nodeId, ConfigNodeWrapper configNodeWrapper) {
    return new TConfigNodeRestartReq(
        clusterName, generateTConfigNodeLocation(nodeId, configNodeWrapper));
  }

  public static TDataNodeLocation generateTDataNodeLocation(
      int nodeId, DataNodeWrapper dataNodeWrapper) {
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(nodeId);
    dataNodeLocation.setClientRpcEndPoint(
        new TEndPoint(dataNodeWrapper.getIp(), dataNodeWrapper.getPort()));
    dataNodeLocation.setInternalEndPoint(
        new TEndPoint(dataNodeWrapper.getIp(), dataNodeWrapper.getInternalPort()));
    dataNodeLocation.setMPPDataExchangeEndPoint(
        new TEndPoint(dataNodeWrapper.getIp(), dataNodeWrapper.getMppDataExchangePort()));
    dataNodeLocation.setDataRegionConsensusEndPoint(
        new TEndPoint(dataNodeWrapper.getIp(), dataNodeWrapper.getDataRegionConsensusPort()));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(
        new TEndPoint(dataNodeWrapper.getIp(), dataNodeWrapper.getSchemaRegionConsensusPort()));
    return dataNodeLocation;
  }

  public static TDataNodeConfiguration generateTDataNodeConfiguration(
      int nodeId, DataNodeWrapper dataNodeWrapper) {
    TNodeResource dataNodeResource = new TNodeResource();
    dataNodeResource.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    dataNodeResource.setMaxMemory(Runtime.getRuntime().totalMemory());
    return new TDataNodeConfiguration(
        generateTDataNodeLocation(nodeId, dataNodeWrapper), dataNodeResource);
  }

  public static TDataNodeRegisterReq generateTDataNodeRegisterReq(DataNodeWrapper dataNodeWrapper) {
    return new TDataNodeRegisterReq(generateTDataNodeConfiguration(-1, dataNodeWrapper));
  }

  public static TDataNodeRestartReq generateTDataNodeRestartReq(
      String clusterName, int nodeId, DataNodeWrapper dataNodeWrapper) {
    return new TDataNodeRestartReq(
        clusterName, generateTDataNodeConfiguration(nodeId, dataNodeWrapper));
  }
}
