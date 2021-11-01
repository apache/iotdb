/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.client.async.AsyncMetaClient.FactoryAsync;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.RaftServer;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AsyncMetaClientTest {

  private final ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private boolean isAsyncServer;

  @Before
  public void setUp() {
    isAsyncServer = config.isUseAsyncServer();
    config.setUseAsyncServer(true);
  }

  @After
  public void tearDown() {
    config.setUseAsyncServer(isAsyncServer);
  }

  @Test
  public void test() throws IOException, TException {
    AsyncClientPool asyncClientPool = new AsyncClientPool(new FactoryAsync(new Factory()));
    AsyncMetaClient client;
    Node node = TestUtils.getNode(0);
    client =
        new AsyncMetaClient(
            new Factory(),
            new TAsyncClientManager(),
            new TNonblockingSocket(
                node.getInternalIp(), node.getMetaPort(), RaftServer.getConnectionTimeoutInMS()));
    assertTrue(client.isReady());

    client = (AsyncMetaClient) asyncClientPool.getClient(TestUtils.getNode(0));

    assertEquals(TestUtils.getNode(0), client.getNode());

    client.matchTerm(
        0,
        0,
        TestUtils.getRaftNode(0, 0),
        new AsyncMethodCallback<Boolean>() {
          @Override
          public void onComplete(Boolean aBoolean) {
            // do nothing
          }

          @Override
          public void onError(Exception e) {
            // do nothing
          }
        });
    assertFalse(client.isReady());

    client.onError(new Exception());
    assertNull(client.getCurrMethod());
    assertTrue(client.isReady());

    assertEquals(
        "MetaClient{node=ClusterNode{ internalIp='192.168.0.0', metaPort=9003, nodeIdentifier=0, dataPort=40010, clientPort=6667, clientIp='0.0.0.0'}}",
        client.toString());
  }
}
