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
package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import junit.framework.TestCase;
import org.junit.Assert;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

public class DataNodeServerCommandLineTest extends TestCase {

  // List of well known locations for this test
  protected static final TDataNodeLocation LOCATION_1 =
      new TDataNodeLocation(1, new TEndPoint("1.2.3.4", 6667), null, null, null, null);
  protected static final TDataNodeLocation LOCATION_2 =
      new TDataNodeLocation(2, new TEndPoint("1.2.3.5", 6667), null, null, null, null);
  protected static final TDataNodeLocation LOCATION_3 =
      new TDataNodeLocation(3, new TEndPoint("1.2.3.6", 6667), null, null, null, null);
  // An invalid location
  protected static final TDataNodeLocation INVALID_LOCATION =
      new TDataNodeLocation(23, new TEndPoint("4.3.2.1", 815), null, null, null, null);

  /**
   * In this test we pass an empty args list to the command. This is expected to fail.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testNoArgs() throws Exception {
    // No need to initialize these mocks with anything sensible, as they should never be used.
    ConfigNodeInfo configNodeInfo = null;
    IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager = null;
    DataNode dataNode = null;
    DataNodeServerCommandLine sut =
        new DataNodeServerCommandLine(configNodeInfo, configNodeClientManager, dataNode);

    int returnCode = sut.run(new String[0]);

    // We expect an error code of -1.
    Assert.assertEquals(-1, returnCode);
  }

  /**
   * In this test we pass too many arguments to the command. This should also fail with an error
   * code.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testTooManyArgs() throws Exception {
    // No need to initialize these mocks with anything sensible, as they should never be used.
    ConfigNodeInfo configNodeInfo = null;
    IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager = null;
    DataNode dataNode = null;
    DataNodeServerCommandLine sut =
        new DataNodeServerCommandLine(configNodeInfo, configNodeClientManager, dataNode);

    int returnCode = sut.run(new String[] {"-r", "2", "-s"});

    // We expect an error code of -1.
    Assert.assertEquals(-1, returnCode);
  }

  /**
   * In this test case we provide the coordinates for the data-node that we want to delete by
   * providing the node-id of that node.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testSingleDataNodeRemoveById() throws Exception {
    ConfigNodeInfo configNodeInfo = Mockito.mock(ConfigNodeInfo.class);
    IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
        Mockito.mock(IClientManager.class);
    ConfigNodeClient client = Mockito.mock(ConfigNodeClient.class);
    Mockito.when(configNodeClientManager.borrowClient(Mockito.any(ConfigRegionId.class)))
        .thenReturn(client);
    // This is the result of the getDataNodeConfiguration, which contains the list of known data
    // nodes.
    TDataNodeConfigurationResp tDataNodeConfigurationResp = new TDataNodeConfigurationResp();
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        1, new TDataNodeConfiguration(LOCATION_1, new TNodeResource()));
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        2, new TDataNodeConfiguration(LOCATION_2, new TNodeResource()));
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        3, new TDataNodeConfiguration(LOCATION_3, new TNodeResource()));
    Mockito.when(client.getDataNodeConfiguration(Mockito.anyInt()))
        .thenReturn(tDataNodeConfigurationResp);
    // Only return something sensible, if exactly this location is asked to be deleted.
    Mockito.when(
            client.removeDataNode(new TDataNodeRemoveReq(Collections.singletonList(LOCATION_2))))
        .thenReturn(
            new TDataNodeRemoveResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    DataNode dataNode = Mockito.mock(DataNode.class);
    DataNodeServerCommandLine sut =
        new DataNodeServerCommandLine(configNodeInfo, configNodeClientManager, dataNode);

    int returnCode = sut.run(new String[] {"-r", "2"});

    // Check the overall return code was ok.
    Assert.assertEquals(0, returnCode);
    // Check that the config node client was actually called with a request to remove the
    // node we want it to remove
    Mockito.verify(client, Mockito.times(1))
        .removeDataNode(new TDataNodeRemoveReq(Collections.singletonList(LOCATION_2)));
  }

  /**
   * In this test case we provide the coordinates for the data-node that we want to delete by
   * providing the node-id of that node. However, the coordinates are invalid and therefore the
   * deletion fails with an error.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testSingleDataNodeRemoveByIdWithInvalidCoordinates() throws Exception {
    ConfigNodeInfo configNodeInfo = Mockito.mock(ConfigNodeInfo.class);
    IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
        Mockito.mock(IClientManager.class);
    ConfigNodeClient client = Mockito.mock(ConfigNodeClient.class);
    Mockito.when(configNodeClientManager.borrowClient(Mockito.any(ConfigRegionId.class)))
        .thenReturn(client);
    // This is the result of the getDataNodeConfiguration, which contains the list of known data
    // nodes.
    TDataNodeConfigurationResp tDataNodeConfigurationResp = new TDataNodeConfigurationResp();
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        1, new TDataNodeConfiguration(LOCATION_1, new TNodeResource()));
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        2, new TDataNodeConfiguration(LOCATION_2, new TNodeResource()));
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        3, new TDataNodeConfiguration(LOCATION_3, new TNodeResource()));
    Mockito.when(client.getDataNodeConfiguration(Mockito.anyInt()))
        .thenReturn(tDataNodeConfigurationResp);
    DataNode dataNode = Mockito.mock(DataNode.class);
    DataNodeServerCommandLine sut =
        new DataNodeServerCommandLine(configNodeInfo, configNodeClientManager, dataNode);

    try {
      sut.run(new String[] {"-r", "23"});
      Assert.fail("This call should have failed");
    } catch (Exception e) {
      // This is actually what we expected
      Assert.assertTrue(e instanceof BadNodeUrlException);
    }
  }

  /**
   * In this test case we provide the coordinates for the data-node that we want to delete by
   * providing the node-id of that node. NOTE: The test was prepared to test deletion of multiple
   * nodes, however currently we don't support this.
   *
   * @throws Exception nothing should go wrong here.
   */
  public void testMultipleDataNodeRemoveById() throws Exception {
    ConfigNodeInfo configNodeInfo = Mockito.mock(ConfigNodeInfo.class);
    IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
        Mockito.mock(IClientManager.class);
    ConfigNodeClient client = Mockito.mock(ConfigNodeClient.class);
    Mockito.when(configNodeClientManager.borrowClient(Mockito.any(ConfigRegionId.class)))
        .thenReturn(client);
    // This is the result of the getDataNodeConfiguration, which contains the list of known data
    // nodes.
    TDataNodeConfigurationResp tDataNodeConfigurationResp = new TDataNodeConfigurationResp();
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        1, new TDataNodeConfiguration(LOCATION_1, new TNodeResource()));
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        2, new TDataNodeConfiguration(LOCATION_2, new TNodeResource()));
    tDataNodeConfigurationResp.putToDataNodeConfigurationMap(
        3, new TDataNodeConfiguration(LOCATION_3, new TNodeResource()));
    Mockito.when(client.getDataNodeConfiguration(Mockito.anyInt()))
        .thenReturn(tDataNodeConfigurationResp);
    // Only return something sensible, if exactly the locations we want are asked to be deleted.
    Mockito.when(
            client.removeDataNode(new TDataNodeRemoveReq(Arrays.asList(LOCATION_1, LOCATION_2))))
        .thenReturn(
            new TDataNodeRemoveResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    DataNode dataNode = Mockito.mock(DataNode.class);
    DataNodeServerCommandLine sut =
        new DataNodeServerCommandLine(configNodeInfo, configNodeClientManager, dataNode);

    try {
      sut.run(new String[] {"-r", "1,2"});
      Assert.fail("This call should have failed");
    } catch (Exception e) {
      // This is actually what we expected
      Assert.assertTrue(e instanceof IllegalArgumentException);
    }
  }
}
