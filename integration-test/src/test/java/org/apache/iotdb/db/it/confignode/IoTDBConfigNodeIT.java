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
package org.apache.iotdb.db.it.confignode;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerReq;
import org.apache.iotdb.confignode.rpc.thrift.TAuthorizerResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckUserPrivilegesReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
  public void removeAndStopConfigNodeTest() {
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void queryAndRemoveDataNodeTest() {
    TShowDataNodesResp showDataNodesResp = null;
    TDataNodeRegisterReq dataNodeRegisterReq;
    TDataNodeRegisterResp dataNodeRegisterResp;
    TDataNodeConfigurationResp dataNodeConfigurationResp;
    TDataNodeRemoveReq removeReq;
    TDataNodeRemoveResp removeResp;

    List<ConfigNodeWrapper> configNodeWrappers = EnvFactory.getEnv().getConfigNodeWrapperList();
    List<DataNodeWrapper> dataNodeWrappers = EnvFactory.getEnv().getDataNodeWrapperList();
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    List<TDataNodeLocation> removeDataNodeLocationList = new ArrayList<>();
    List<TDataNodeInfo> dataNodeInfos;

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      // add DataNode
      DataNodeWrapper dataNodeWrapper =
          new DataNodeWrapper(
              configNodeWrappers.get(0).getIpAndPortString(),
              "IoTDBConfigNodeIT",
              "dataNodeTest",
              EnvUtils.searchAvailablePorts());
      dataNodeWrapper.createDir();
      dataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());
      dataNodeWrapper.start();
      dataNodeWrappers.add(dataNodeWrapper);

      EnvFactory.getEnv().setDataNodeWrapperList(dataNodeWrappers);
      for (int i = 0; i < retryNum; i++) {
        showDataNodesResp = client.showDataNodes();
        if (showDataNodesResp.getDataNodesInfoListSize() == 4) {
          break;
        }
        Thread.sleep(1000);
      }
      assertEquals(4, showDataNodesResp.getDataNodesInfoListSize());

      dataNodeInfos = showDataNodesResp.getDataNodesInfoList();
      dataNodeConfiguration.setLocation(dataNodeLocation);
      dataNodeConfiguration.setResource(new TNodeResource(8, 1024 * 1024));
      dataNodeRegisterReq = new TDataNodeRegisterReq(dataNodeConfiguration);
      TDataNodeInfo dataNodeInfo = dataNodeInfos.get(3);

      // test success re-register
      dataNodeLocation.setDataNodeId(dataNodeInfo.getDataNodeId());
      dataNodeLocation.setClientRpcEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort()));
      dataNodeLocation.setMPPDataExchangeEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 1));
      dataNodeLocation.setInternalEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 2));
      dataNodeLocation.setDataRegionConsensusEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 3));
      dataNodeLocation.setSchemaRegionConsensusEndPoint(
          new TEndPoint(dataNodeInfo.getRpcAddresss(), dataNodeInfo.getRpcPort() + 4));

      dataNodeRegisterResp = client.registerDataNode(dataNodeRegisterReq);
      assertEquals(
          TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode(),
          dataNodeRegisterResp.getStatus().getCode());

      // test query all DataNodeInfos
      dataNodeConfigurationResp = client.getDataNodeConfiguration(-1);
      Map<Integer, TDataNodeConfiguration> infoMap =
          dataNodeConfigurationResp.getDataNodeConfigurationMap();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataNodeConfigurationResp.getStatus().getCode());
      assertEquals(4, infoMap.size());
      List<Map.Entry<Integer, TDataNodeConfiguration>> infoList =
          new ArrayList<>(infoMap.entrySet());
      infoList.sort(Comparator.comparingInt(Map.Entry::getKey));
      int count = 0;
      for (DataNodeWrapper expectedDataNode : dataNodeWrappers) {
        for (int i = 0; i < 4; i++) {
          TDataNodeLocation dnLocation = infoList.get(i).getValue().getLocation();
          if (expectedDataNode.getInternalPort() == dnLocation.getInternalEndPoint().getPort()
              && expectedDataNode.getMppDataExchangePort()
                  == dnLocation.getMPPDataExchangeEndPoint().getPort()
              && expectedDataNode.getSchemaRegionConsensusPort()
                  == dnLocation.getSchemaRegionConsensusEndPoint().getPort()
              && expectedDataNode.getDataRegionConsensusPort()
                  == dnLocation.getDataRegionConsensusEndPoint().getPort()) {
            count++;
            break;
          }
        }
      }
      assertEquals(4, count);

      // test query one DataNodeInfo
      dataNodeConfigurationResp = client.getDataNodeConfiguration(dataNodeLocation.getDataNodeId());
      infoMap = dataNodeConfigurationResp.getDataNodeConfigurationMap();
      TDataNodeLocation dnLocation =
          dataNodeConfigurationResp
              .getDataNodeConfigurationMap()
              .get(dataNodeLocation.getDataNodeId())
              .getLocation();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataNodeConfigurationResp.getStatus().getCode());
      assertEquals(1, infoMap.size());
      assertEquals(dataNodeLocation, dnLocation);

      // test remove DataNode
      dataNodeLocation = infoList.get(0).getValue().getLocation();
      removeDataNodeLocationList.add(dataNodeLocation);
      removeReq = new TDataNodeRemoveReq(removeDataNodeLocationList);
      removeResp = client.removeDataNode(removeReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), removeResp.getStatus().getCode());

      for (int i = 0; i < retryNum; i++) {
        showDataNodesResp = client.showDataNodes();
        if (showDataNodesResp.getDataNodesInfoListSize() == 3) {
          break;
        }
        Thread.sleep(1000);
      }
      assertEquals(3, showDataNodesResp.getDataNodesInfoListSize());

      dataNodeInfos = showDataNodesResp.getDataNodesInfoList();
      for (TDataNodeInfo dnInfo : dataNodeInfos) {
        assertNotEquals(dataNodeLocation.getDataNodeId(), dnInfo.getDataNodeId());
      }
      dataNodeConfigurationResp = client.getDataNodeConfiguration(dataNodeLocation.getDataNodeId());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          dataNodeConfigurationResp.getStatus().getCode());
      assertEquals(0, dataNodeConfigurationResp.getDataNodeConfigurationMap().size());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void showClusterAndNodesTest() {
    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {

      TShowClusterResp clusterNodes;
      TShowConfigNodesResp showConfigNodesResp;
      TShowDataNodesResp showDataNodesResp;

      List<ConfigNodeWrapper> configNodeWrappers = EnvFactory.getEnv().getConfigNodeWrapperList();
      List<DataNodeWrapper> dataNodeWrappers = EnvFactory.getEnv().getDataNodeWrapperList();

      // test showCluster
      clusterNodes = getClusterNodeInfos(client, 1, 3);
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
        assertTrue(found);
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
        assertTrue(found);
      }

      // test showConfigNodes
      showConfigNodesResp = client.showConfigNodes();
      List<TConfigNodeInfo> configNodesInfo = showConfigNodesResp.getConfigNodesInfoList();

      for (TConfigNodeInfo configNodeInfo : configNodesInfo) {
        boolean found = false;
        for (ConfigNodeWrapper configNodeWrapper : configNodeWrappers) {
          if (configNodeWrapper.getIp().equals(configNodeInfo.getInternalAddress())
              && configNodeWrapper.getPort() == configNodeInfo.getInternalPort()) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }

      // test showDataNodes
      showDataNodesResp = client.showDataNodes();
      List<TDataNodeInfo> dataNodesInfo = showDataNodesResp.getDataNodesInfoList();

      for (TDataNodeInfo dataNodeInfo : dataNodesInfo) {
        boolean found = false;
        for (DataNodeWrapper dataNodeWrapper : dataNodeWrappers) {
          if (dataNodeWrapper.getIp().equals(dataNodeInfo.getRpcAddresss())
              && dataNodeWrapper.getPort() == dataNodeInfo.getRpcPort()) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void cleanUserAndRole(IConfigNodeRPCService.Iface client) throws TException {
    TSStatus status;

    // clean user
    TAuthorizerReq authorizerReq =
        new TAuthorizerReq(
            AuthorOperator.AuthorType.LIST_USER.ordinal(),
            "",
            "",
            "",
            "",
            new HashSet<>(),
            Collections.singletonList(""));
    TAuthorizerResp authorizerResp = client.queryPermission(authorizerReq);
    status = authorizerResp.getStatus();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

    List<String> allUsers = authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_USER);
    for (String user : allUsers) {
      if (!user.equals("root")) {
        authorizerReq =
            new TAuthorizerReq(
                AuthorOperator.AuthorType.DROP_USER.ordinal(),
                user,
                "",
                "",
                "",
                new HashSet<>(),
                Collections.singletonList(""));
        status = client.operatePermission(authorizerReq);
        assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      }
    }
  }

  @Test
  public void permissionTest() {
    TSStatus status;
    List<String> userList = new ArrayList<>();
    userList.add("root");
    userList.add("tempuser0");
    userList.add("tempuser1");

    List<String> roleList = new ArrayList<>();
    roleList.add("temprole0");
    roleList.add("temprole1");

    TAuthorizerReq authorizerReq;
    TAuthorizerResp authorizerResp;
    TCheckUserPrivilegesReq checkUserPrivilegesReq;

    Set<Integer> privilegeList = new HashSet<>();
    privilegeList.add(PrivilegeType.DELETE_USER.ordinal());
    privilegeList.add(PrivilegeType.CREATE_USER.ordinal());

    Set<Integer> revokePrivilege = new HashSet<>();
    revokePrivilege.add(PrivilegeType.DELETE_USER.ordinal());

    List<String> privilege = new ArrayList<>();
    privilege.add("root.** : CREATE_USER");
    privilege.add("root.** : CREATE_USER");

    List<String> paths = new ArrayList<>();
    paths.add("root.ln.**");

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getConfigNodeConnection()) {
      cleanUserAndRole(client);

      // create user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.CREATE_USER.ordinal(),
              "tempuser0",
              "",
              "passwd",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorizerReq.setUserName("tempuser1");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq("tempuser0", paths, PrivilegeType.DELETE_USER.ordinal());
      status = client.checkUserPrivileges(checkUserPrivilegesReq).getStatus();
      assertEquals(TSStatusCode.NO_PERMISSION_ERROR.getStatusCode(), status.getCode());

      // drop user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.DROP_USER.ordinal(),
              "tempuser1",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_USER.ordinal(),
              "",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      userList.remove("tempuser1");
      assertEquals(userList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_USER));

      // create role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.CREATE_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      authorizerReq.setRoleName("temprole1");
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // drop role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.DROP_ROLE.ordinal(),
              "",
              "temprole1",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_ROLE.ordinal(),
              "",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      roleList.remove("temprole1");
      assertEquals(roleList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_ROLE));

      // alter user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.UPDATE_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "newpwd",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant user
      List<String> nodeNameList = new ArrayList<>();
      nodeNameList.add("root.ln.**");
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.GRANT_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              privilegeList,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // check user privileges
      checkUserPrivilegesReq =
          new TCheckUserPrivilegesReq("tempuser0", paths, PrivilegeType.DELETE_USER.ordinal());
      status = client.checkUserPrivileges(checkUserPrivilegesReq).getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.GRANT_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              privilegeList,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // grant role to user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.GRANT_USER_ROLE.ordinal(),
              "tempuser0",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // revoke user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.REVOKE_USER.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              revokePrivilege,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // revoke role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.REVOKE_ROLE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              revokePrivilege,
              nodeNameList);
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list privileges user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              nodeNameList);
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list user privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list privileges role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              nodeNameList);
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      privilege.remove(0);
      assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list role privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      Assert.assertEquals(
          privilege, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE));

      // list all role of user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_ROLE.ordinal(),
              "tempuser0",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      roleList.remove("temprole1");
      assertEquals(roleList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_ROLE));

      // list all user of role
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_USER.ordinal(),
              "",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      userList.remove("tempuser1");
      userList.remove("root");
      assertEquals(userList, authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_USER));

      // revoke role from user
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.REVOKE_USER_ROLE.ordinal(),
              "tempuser0",
              "temprole0",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      status = client.operatePermission(authorizerReq);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      // list root privileges
      authorizerReq =
          new TAuthorizerReq(
              AuthorOperator.AuthorType.LIST_USER_PRIVILEGE.ordinal(),
              "root",
              "",
              "",
              "",
              new HashSet<>(),
              new ArrayList<>());
      authorizerResp = client.queryPermission(authorizerReq);
      status = authorizerResp.getStatus();
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
      for (int i = 0; i < PrivilegeType.values().length; i++) {
        assertEquals(
            PrivilegeType.values()[i].toString(),
            authorizerResp.getAuthorizerInfo().get(IoTDBConstant.COLUMN_PRIVILEGE).get(i));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
