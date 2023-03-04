/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.client.BaseClientTest;
import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketException;

import static org.junit.Assert.assertEquals;

public class SyncDataClientTest extends BaseClientTest {

  private TProtocolFactory protocolFactory;

  @Before
  public void setUp() {
    protocolFactory =
        ClusterDescriptor.getInstance().getConfig().isRpcThriftCompressionEnabled()
            ? new TCompactProtocol.Factory()
            : new TBinaryProtocol.Factory();
  }

  @Test
  public void testDataClient() throws IOException, InterruptedException, TTransportException {
    try {
      startDataServer();
      SyncDataClient dataClient =
          new SyncDataClient(protocolFactory, defaultNode, ClientCategory.DATA);

      assertEquals(
          "SyncDataClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=40010}",
          dataClient.toString());

      assertCheck(dataClient);

      dataClient =
          new SyncDataClient.SyncDataClientFactory(protocolFactory, ClientCategory.DATA)
              .makeObject(defaultNode)
              .getObject();

      assertEquals(
          "SyncDataClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=40010}",
          dataClient.toString());

      assertCheck(dataClient);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      stopDataServer();
    }
  }

  @Test
  public void testDataHeartbeatClient()
      throws IOException, InterruptedException, TTransportException {
    try {
      startDataHeartbeatServer();
      SyncDataClient dataHeartbeatClient =
          new SyncDataClient(protocolFactory, defaultNode, ClientCategory.DATA_HEARTBEAT);

      assertCheck(dataHeartbeatClient);
      assertEquals(
          "SyncDataHeartbeatClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=40011}",
          dataHeartbeatClient.toString());

      dataHeartbeatClient =
          new SyncDataClient.SyncDataClientFactory(protocolFactory, ClientCategory.DATA_HEARTBEAT)
              .makeObject(defaultNode)
              .getObject();
      assertCheck(dataHeartbeatClient);
      assertEquals(
          "SyncDataHeartbeatClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=40011}",
          dataHeartbeatClient.toString());
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      stopDataHeartbeatServer();
    }
  }

  private void assertCheck(SyncDataClient dataClient) throws SocketException {
    Assert.assertNotNull(dataClient);
    Assert.assertTrue(dataClient.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(dataClient.getNode(), defaultNode);

    dataClient.setTimeout(ClusterConstant.getConnectionTimeoutInMS());
    Assert.assertEquals(dataClient.getTimeout(), ClusterConstant.getConnectionTimeoutInMS());

    dataClient.close();
    Assert.assertFalse(dataClient.getInputProtocol().getTransport().isOpen());
  }
}
