/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.client.BaseClientTest;
import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncMetaClientTest extends BaseClientTest {

  private final ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private TProtocolFactory protocolFactory;

  @Before
  public void setUp() {
    config.setUseAsyncServer(true);
    protocolFactory =
        config.isRpcThriftCompressionEnabled()
            ? new TCompactProtocol.Factory()
            : new TBinaryProtocol.Factory();
  }

  @Test
  public void testMetaClient() throws Exception {

    AsyncMetaClient.AsyncMetaClientFactory factory =
        new AsyncMetaClient.AsyncMetaClientFactory(protocolFactory, ClientCategory.META);

    AsyncMetaClient metaClient = factory.makeObject(defaultNode).getObject();

    assertEquals(
        "AsyncMetaClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
            + "dataPort:40010, clientPort:0, clientIp:localhost),port=9003}",
        metaClient.toString());
    assertCheck(metaClient);
    factory.close();
  }

  @Test
  public void testMetaHeartbeatClient() throws Exception {
    AsyncMetaClient.AsyncMetaClientFactory factory =
        new AsyncMetaClient.AsyncMetaClientFactory(protocolFactory, ClientCategory.META_HEARTBEAT);

    AsyncMetaClient metaClient = factory.makeObject(defaultNode).getObject();

    assertEquals(
        "AsyncMetaHeartbeatClient{node=Node(internalIp:localhost, metaPort:9003, nodeIdentifier:0, "
            + "dataPort:40010, clientPort:0, clientIp:localhost),port=9004}",
        metaClient.toString());
    assertCheck(metaClient);
    factory.close();
  }

  private void assertCheck(AsyncMetaClient dataClient) {
    Assert.assertNotNull(dataClient);
    assertTrue(dataClient.isReady());
    assertTrue(dataClient.isValid());
    Assert.assertEquals(dataClient.getNode(), defaultNode);

    dataClient.setTimeout(ClusterConstant.getConnectionTimeoutInMS());
    Assert.assertEquals(dataClient.getTimeout(), ClusterConstant.getConnectionTimeoutInMS());

    dataClient.close();
    Assert.assertNull(dataClient.getCurrMethod());
  }
}
