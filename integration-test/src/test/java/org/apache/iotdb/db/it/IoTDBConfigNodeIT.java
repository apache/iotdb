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
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TClusterNodeInfos;
import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
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

  @Test
  public void getAllClusterNodeInfosTest() {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      List<ConfigNodeWrapper> configNodeWrappers = EnvFactory.getEnv().getConfigNodeWrapperList();
      List<DataNodeWrapper> dataNodeWrappers = EnvFactory.getEnv().getDataNodeWrapperList();

      TClusterNodeInfos clusterNodes = null;
      for (int i = 0; i < retryNum; i++) {
        clusterNodes = client.getAllClusterNodeInfos();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (clusterNodes.getConfigNodeList().size() == 3) break;
      }

      List<TConfigNodeLocation> configNodeLocations = clusterNodes.getConfigNodeList();
      List<TDataNodeLocation> dataNodeLocations = clusterNodes.getDataNodeList();

      for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
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
        if (!found) {
          fail("ConfigNode not found in ConfigNodeList");
        }
      }

      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        boolean found = false;
        for (DataNodeWrapper dataNodeWrapper : dataNodeWrappers) {
          if (dataNodeWrapper.getIp().equals(dataNodeLocation.getInternalEndPoint().getIp())
              && dataNodeWrapper.getPort() == dataNodeLocation.getClientRpcEndPoint().getPort()
              && dataNodeWrapper.getInternalPort()
                  == dataNodeLocation.getInternalEndPoint().getPort()
              && dataNodeWrapper.getMppDataExchangePort()
                  == dataNodeLocation.getMPPDataExchangeEndPoint().getPort()
              && dataNodeWrapper.getSchemaRegionConsensusPort()
                  == dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort()
              && dataNodeWrapper.getDataRegionConsensusPort()
                  == dataNodeLocation.getDataRegionConsensusEndPoint().getPort()) {
            found = true;
            break;
          }
        }
        if (!found) {
          fail("DataNode not found in DataNodeList");
        }
      }

      assertEquals(3, configNodeLocations.size());
      assertEquals(3, dataNodeLocations.size());

      for (int i = 0; i < 6; i++) {
        assertEquals("Running", clusterNodes.getNodeStatus().get(i));
      }
    } catch (TException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
