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

public class SyncMetaClientTest extends BaseClientTest {

  private TProtocolFactory protocolFactory;

  @Before
  public void setUp() {
    protocolFactory =
        ClusterDescriptor.getInstance().getConfig().isRpcThriftCompressionEnabled()
            ? new TCompactProtocol.Factory()
            : new TBinaryProtocol.Factory();
  }

  @Test
  public void testMetaClient() throws IOException, InterruptedException, TTransportException {
    try {
      startMetaServer();
      SyncMetaClient metaClient =
          new SyncMetaClient(protocolFactory, defaultNode, ClientCategory.META);

      assertEquals(
          "SyncMetaClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=9003}",
          metaClient.toString());

      assertCheck(metaClient);

      metaClient =
          new SyncMetaClient.SyncMetaClientFactory(protocolFactory, ClientCategory.META)
              .makeObject(defaultNode)
              .getObject();

      assertEquals(
          "SyncMetaClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=9003}",
          metaClient.toString());

      assertCheck(metaClient);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      stopMetaServer();
    }
  }

  @Test
  public void testDataHeartbeatClient()
      throws IOException, InterruptedException, TTransportException {
    try {
      startMetaHeartbeatServer();
      SyncMetaClient metaHeartbeatClient =
          new SyncMetaClient(protocolFactory, defaultNode, ClientCategory.META_HEARTBEAT);

      assertCheck(metaHeartbeatClient);
      assertEquals(
          "SyncMetaHeartbeatClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=9004}",
          metaHeartbeatClient.toString());

      metaHeartbeatClient =
          new SyncMetaClient.SyncMetaClientFactory(protocolFactory, ClientCategory.META_HEARTBEAT)
              .makeObject(defaultNode)
              .getObject();
      assertCheck(metaHeartbeatClient);
      assertEquals(
          "SyncMetaHeartbeatClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
              + "dataPort:40010, clientPort:0, clientIp:localhost),port=9004}",
          metaHeartbeatClient.toString());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      stopMetaHeartbeatServer();
    }
  }

  private void assertCheck(SyncMetaClient metaClient) throws SocketException {
    Assert.assertNotNull(metaClient);
    Assert.assertTrue(metaClient.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(metaClient.getNode(), defaultNode);

    metaClient.setTimeout(ClusterConstant.getConnectionTimeoutInMS());
    Assert.assertEquals(metaClient.getTimeout(), ClusterConstant.getConnectionTimeoutInMS());

    metaClient.close();
    Assert.assertFalse(metaClient.getInputProtocol().getTransport().isOpen());
  }
}
