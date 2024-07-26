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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeRegisterResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.TimeoutChangeableTFastFramedTransport;

import junit.framework.TestCase;
import org.apache.thrift.transport.TSocket;
import org.junit.Assert;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.Socket;
import java.util.Collections;

public class ConfigNodeRPCServiceProcessorTest extends TestCase {

  /**
   * This test should be a normal data-node registration where a valid ip is used as address of the
   * rpc-service. Nothing special should happen here.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testRegisterDataNode() throws Exception {
    // Set up the system under test.
    CommonConfig commonConfig = Mockito.mock(CommonConfig.class);
    ConfigNodeConfig configNodeConfig = Mockito.mock(ConfigNodeConfig.class);
    ConfigNode configNode = Mockito.mock(ConfigNode.class);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    DataNodeRegisterResp registerDataNodeResponse = new DataNodeRegisterResp();
    registerDataNodeResponse.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    registerDataNodeResponse.setConfigNodeList(
        Collections.singletonList(new TConfigNodeLocation()));
    registerDataNodeResponse.setDataNodeId(42);
    registerDataNodeResponse.setRuntimeConfiguration(new TRuntimeConfiguration());
    Mockito.when(configManager.registerDataNode(Mockito.any(TDataNodeRegisterReq.class)))
        .thenReturn(registerDataNodeResponse);
    Socket socket = Mockito.mock(Socket.class);
    Mockito.when(socket.getInetAddress())
        .thenReturn(InetAddress.getByAddress(new byte[] {1, 2, 3, 4}));
    TSocket tSocket = Mockito.mock(TSocket.class);
    Mockito.when(tSocket.getSocket()).thenReturn(socket);
    TimeoutChangeableTFastFramedTransport transport =
        Mockito.mock(TimeoutChangeableTFastFramedTransport.class);
    Mockito.when(transport.getSocket()).thenReturn(tSocket);
    ConfigNodeRPCServiceProcessor sut =
        new ConfigNodeRPCServiceProcessor(
            commonConfig, configNodeConfig, configNode, configManager);

    // Prepare the test input
    TDataNodeLocation newDataNodeLocation = new TDataNodeLocation();
    newDataNodeLocation.setDataNodeId(42);
    newDataNodeLocation.setClientRpcEndPoint(new TEndPoint("1.2.3.4", 6667));
    TDataNodeConfiguration newDataNodeConfiguration = new TDataNodeConfiguration();
    newDataNodeConfiguration.setLocation(newDataNodeLocation);
    TDataNodeRegisterReq req = new TDataNodeRegisterReq();
    req.setClusterName("test-cluster");
    req.setDataNodeConfiguration(newDataNodeConfiguration);

    // Execute the test logic
    TDataNodeRegisterResp res = sut.registerDataNode(req);

    // Check the result
    Assert.assertEquals(registerDataNodeResponse.convertToRpcDataNodeRegisterResp(), res);
    // Check that the config manager was called to register a new node
    ArgumentCaptor<TDataNodeRegisterReq> acRequest =
        ArgumentCaptor.forClass(TDataNodeRegisterReq.class);
    Mockito.verify(configManager, Mockito.times(1)).registerDataNode(acRequest.capture());
    TDataNodeRegisterReq sentRequest = acRequest.getValue();
    Assert.assertEquals(
        "1.2.3.4",
        sentRequest.getDataNodeConfiguration().getLocation().getClientRpcEndPoint().getIp());
  }

  /**
   * This test should be a normal data-node restart where a valid ip is used as address of the
   * rpc-service. Nothing special should happen here.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testRestartDataNode() throws Exception {
    // Set up the system under test.
    CommonConfig commonConfig = Mockito.mock(CommonConfig.class);
    ConfigNodeConfig configNodeConfig = Mockito.mock(ConfigNodeConfig.class);
    ConfigNode configNode = Mockito.mock(ConfigNode.class);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    TDataNodeRestartResp restartDataNodeResponse = new TDataNodeRestartResp();
    restartDataNodeResponse.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    restartDataNodeResponse.setConfigNodeList(Collections.singletonList(new TConfigNodeLocation()));
    restartDataNodeResponse.setRuntimeConfiguration(new TRuntimeConfiguration());
    Mockito.when(configManager.restartDataNode(Mockito.any(TDataNodeRestartReq.class)))
        .thenReturn(restartDataNodeResponse);
    Socket socket = Mockito.mock(Socket.class);
    Mockito.when(socket.getInetAddress())
        .thenReturn(InetAddress.getByAddress(new byte[] {1, 2, 3, 4}));
    TSocket tSocket = Mockito.mock(TSocket.class);
    Mockito.when(tSocket.getSocket()).thenReturn(socket);
    TimeoutChangeableTFastFramedTransport transport =
        Mockito.mock(TimeoutChangeableTFastFramedTransport.class);
    Mockito.when(transport.getSocket()).thenReturn(tSocket);
    ConfigNodeRPCServiceProcessor sut =
        new ConfigNodeRPCServiceProcessor(
            commonConfig, configNodeConfig, configNode, configManager);

    // Prepare the test input
    TDataNodeLocation newDataNodeLocation = new TDataNodeLocation();
    newDataNodeLocation.setDataNodeId(42);
    newDataNodeLocation.setClientRpcEndPoint(new TEndPoint("1.2.3.4", 6667));
    TDataNodeConfiguration newDataNodeConfiguration = new TDataNodeConfiguration();
    newDataNodeConfiguration.setLocation(newDataNodeLocation);
    TDataNodeRestartReq req = new TDataNodeRestartReq();
    req.setClusterName("test-cluster");
    req.setDataNodeConfiguration(newDataNodeConfiguration);

    // Execute the test logic
    TDataNodeRestartResp res = sut.restartDataNode(req);

    // Check the result
    Assert.assertEquals(restartDataNodeResponse, res);
    // Check that the config manager was called to register a new node
    ArgumentCaptor<TDataNodeRestartReq> acRequest =
        ArgumentCaptor.forClass(TDataNodeRestartReq.class);
    Mockito.verify(configManager, Mockito.times(1)).restartDataNode(acRequest.capture());
    TDataNodeRestartReq sentRequest = acRequest.getValue();
    // In this case we expect the ConfigNodeRPCServiceProcessor to have replaced the
    // ip of "0.0.0.0" with the IP it got the request from.
    Assert.assertEquals(
        "1.2.3.4",
        sentRequest.getDataNodeConfiguration().getLocation().getClientRpcEndPoint().getIp());
  }
}
