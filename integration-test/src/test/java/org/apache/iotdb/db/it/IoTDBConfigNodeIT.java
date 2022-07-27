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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.EnvUtils;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBConfigNodeIT {

  protected static String originalConfigNodeConsensusProtocolClass;
  protected static String originalSchemaRegionConsensusProtocolClass;
  protected static String originalDataRegionConsensusProtocolClass;

  private final int retryNum = 30;

  @Before
  public void setUp() throws Exception {
    originalConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    originalSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    originalDataRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getDataRegionConsensusProtocolClass();

    ConfigFactory.getConfig()
        .setConfigNodeConsesusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");

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
  }

  private TShowClusterResp getClusterNodeInfos(
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

  private void checkNodeConfig(
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

  @Test
  public void removeAndStopConfigNodeTest() throws TException, IOException, InterruptedException {
    TShowClusterResp clusterNodes;
    TSStatus status;

    List<ConfigNodeWrapper> configNodeWrappers = EnvFactory.getEnv().getConfigNodeWrapperList();
    List<DataNodeWrapper> dataNodeWrappers = EnvFactory.getEnv().getDataNodeWrapperList();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      // add ConfigNode
      for (int i = 0; i < 2; i++) {
        ConfigNodeWrapper configNodeWrapper =
            new ConfigNodeWrapper(
                false,
                configNodeWrappers.get(0).getIpAndPortString(),
                "IoTDBConfigNodeIT",
                "removeAndStopConfigNodeTest",
                EnvUtils.searchAvailablePorts());
        configNodeWrapper.createDir();
        configNodeWrapper.changeConfig(ConfigFactory.getConfig().getConfignodeProperties());
        configNodeWrapper.start();
        configNodeWrappers.add(configNodeWrapper);
      }
      EnvFactory.getEnv().setConfigNodeWrapperList(configNodeWrappers);
      clusterNodes = getClusterNodeInfos(client, 3, 3);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), clusterNodes.getStatus().getCode());
      List<TConfigNodeLocation> configNodeLocationList = clusterNodes.getConfigNodeList();
      List<TDataNodeLocation> dataNodeLocationList = clusterNodes.getDataNodeList();
      checkNodeConfig(
          configNodeLocationList, dataNodeLocationList, configNodeWrappers, dataNodeWrappers);

      // test remove ConfigNode
      TConfigNodeLocation removedConfigNodeLocation = clusterNodes.getConfigNodeList().get(1);
      for (int i = 0; i < retryNum; i++) {
        Thread.sleep(1000);
        status = client.removeConfigNode(removedConfigNodeLocation);
        if (TSStatusCode.SUCCESS_STATUS.getStatusCode() == status.getCode()) {
          break;
        }
      }

      clusterNodes = getClusterNodeInfos(client, 2, 3);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), clusterNodes.getStatus().getCode());
      checkNodeConfig(
          clusterNodes.getConfigNodeList(),
          clusterNodes.getDataNodeList(),
          configNodeWrappers,
          dataNodeWrappers);

      List<TConfigNodeLocation> configNodeList = clusterNodes.getConfigNodeList();
      for (TConfigNodeLocation configNodeLocation : configNodeList) {
        assertNotEquals(removedConfigNodeLocation, configNodeLocation);
      }

      // test stop ConfigNode
      status = client.stopConfigNode(removedConfigNodeLocation);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
    }
  }
}
