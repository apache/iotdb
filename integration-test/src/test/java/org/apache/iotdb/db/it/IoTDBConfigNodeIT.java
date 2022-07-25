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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.it.env.*;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBConfigNodeIT {

  protected static String enableConfigNodeConsensusProtocolClass;
  protected static String enableSchemaRegionConsensusProtocolClass;
  protected static String enableDataRegionConsensusProtocolClass;

  private final int retryNum = 30;

  @Before
  public void setUp() throws Exception {
    enableConfigNodeConsensusProtocolClass =
        ConfigFactory.getConfig().getConfigNodeConsesusProtocolClass();
    enableSchemaRegionConsensusProtocolClass =
        ConfigFactory.getConfig().getSchemaRegionConsensusProtocolClass();
    enableDataRegionConsensusProtocolClass =
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
        .setConfigNodeConsesusProtocolClass(enableConfigNodeConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setSchemaRegionConsensusProtocolClass(enableSchemaRegionConsensusProtocolClass);
    ConfigFactory.getConfig()
        .setDataRegionConsensusProtocolClass(enableDataRegionConsensusProtocolClass);
  }

  private TShowClusterResp getClusterNodeInfos(
      IConfigNodeRPCService.Iface client, int configNodeNum, int dataNodeNum)
      throws TException, InterruptedException {
    TShowClusterResp clusterNodes = null;
    for (int i = 0; i < retryNum; i++) {
      clusterNodes = client.showCluster();
      Thread.sleep(1000);

      if (clusterNodes.getConfigNodeListSize() == configNodeNum
          && clusterNodes.getDataNodeListSize() == dataNodeNum) break;
    }

    if (clusterNodes.getConfigNodeListSize() != configNodeNum
        || clusterNodes.getDataNodeListSize() != dataNodeNum) {
      fail("getClusterNodeInfos failed");
    }

    return clusterNodes;
  }

  @Test
  public void removeAndStopConfigNodeTest() {
    TShowClusterResp clusterNodes;
    TSStatus status;

    List<TConfigNodeLocation> configNodeLocations = new ArrayList<>();
    List<ConfigNodeWrapper> configNodeWrappers = EnvFactory.getEnv().getConfigNodeWrapperList();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      clusterNodes = getClusterNodeInfos(client, 1, 3);
      configNodeLocations.add(clusterNodes.getConfigNodeList().get(0));

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

        TConfigNodeLocation configNodeLocation =
            new TConfigNodeLocation(
                -1,
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getPort()),
                new TEndPoint(configNodeWrapper.getIp(), configNodeWrapper.getConsensusPort()));
        configNodeLocations.add(configNodeLocation);
      }

      EnvFactory.getEnv().setConfigNodeWrapperList(configNodeWrappers);

      clusterNodes = getClusterNodeInfos(client, 3, 3);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), clusterNodes.getStatus().getCode());

      // test remove ConfigNode
      status = client.removeConfigNode(clusterNodes.getConfigNodeList().get(1));
      TConfigNodeLocation stopConfigNodeLocation = configNodeLocations.get(1);
      configNodeLocations.remove(1);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      clusterNodes = getClusterNodeInfos(client, 2, 3);
      List<TConfigNodeLocation> configNodeList = clusterNodes.getConfigNodeList();
      for (int i = 0; i < 2; i++) {
        assertEquals(
            configNodeLocations.get(i).internalEndPoint, configNodeList.get(i).internalEndPoint);
        assertEquals(
            configNodeLocations.get(i).consensusEndPoint, configNodeList.get(i).consensusEndPoint);
      }

      // test stop ConfigNode
      status = client.stopConfigNode(stopConfigNodeLocation);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    } catch (TException | IOException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
