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

package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.utils.ClientUtils;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

// TODO: add case: invalidate client and verify it can not be return by pool
public class ClientPoolFactoryTest {
  private ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();

  private long mockMaxWaitTimeoutMs = 10 * 1000L;
  private int mockMaxClientPerMember = 10;

  private int maxClientPerNodePerMember = clusterConfig.getMaxClientPerNodePerMember();
  private long waitClientTimeoutMS = clusterConfig.getWaitClientTimeoutMS();

  private ClientPoolFactory clientPoolFactory;
  private MockClientManager mockClientManager;

  @Before
  public void setUp() {
    clusterConfig.setMaxClientPerNodePerMember(mockMaxClientPerMember);
    clusterConfig.setWaitClientTimeoutMS(mockMaxWaitTimeoutMs);
    clientPoolFactory = new ClientPoolFactory();
    mockClientManager =
        new MockClientManager() {
          @Override
          public void returnAsyncClient(
              RaftService.AsyncClient client, Node node, ClientCategory category) {
            Assert.assertSame(client, asyncClient);
          }

          @Override
          public void returnSyncClient(
              RaftService.Client client, Node node, ClientCategory category) {
            Assert.assertSame(client, syncClient);
          }
        };
    clientPoolFactory.setClientManager(mockClientManager);
  }

  @After
  public void tearDown() {
    clusterConfig.setMaxClientPerNodePerMember(maxClientPerNodePerMember);
    clusterConfig.setWaitClientTimeoutMS(waitClientTimeoutMS);
  }

  @Test
  public void poolConfigTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.AsyncClient> pool =
        clientPoolFactory.createAsyncDataPool(ClientCategory.DATA);
    Node node = constructDefaultNode();

    for (int i = 0; i < mockMaxClientPerMember; i++) {
      RaftService.AsyncClient client = pool.borrowObject(node);
      Assert.assertNotNull(client);
    }

    long timeStart = System.currentTimeMillis();
    try {
      RaftService.AsyncClient client = pool.borrowObject(node);
      Assert.assertNull(client);
    } catch (Exception e) {
      Assert.assertTrue(e instanceof NoSuchElementException);
    } finally {
      Assert.assertTrue(System.currentTimeMillis() - timeStart + 10 > mockMaxWaitTimeoutMs);
    }
  }

  @Test
  public void poolRecycleTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.AsyncClient> pool =
        clientPoolFactory.createAsyncDataPool(ClientCategory.DATA);

    Node node = constructDefaultNode();
    List<RaftService.AsyncClient> clientList = new ArrayList<>();
    for (int i = 0; i < pool.getMaxIdlePerKey(); i++) {
      RaftService.AsyncClient client = pool.borrowObject(node);
      Assert.assertNotNull(client);
      clientList.add(client);
    }

    for (RaftService.AsyncClient client : clientList) {
      pool.returnObject(node, client);
    }

    for (int i = 0; i < pool.getMaxIdlePerKey(); i++) {
      RaftService.AsyncClient client = pool.borrowObject(node);
      Assert.assertNotNull(client);
      Assert.assertTrue(clientList.contains(client));
    }
  }

  @Test
  public void createAsyncDataClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.AsyncClient> pool =
        clientPoolFactory.createAsyncDataPool(ClientCategory.DATA);

    Assert.assertEquals(pool.getMaxTotalPerKey(), mockMaxClientPerMember);
    Assert.assertEquals(pool.getMaxWaitDuration(), Duration.ofMillis(mockMaxWaitTimeoutMs));

    RaftService.AsyncClient asyncClient = null;

    Node node = constructDefaultNode();

    asyncClient = pool.borrowObject(node);
    Assert.assertNotNull(asyncClient);
    Assert.assertTrue(asyncClient instanceof AsyncDataClient);
  }

  @Test
  public void createAsyncMetaClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.AsyncClient> pool =
        clientPoolFactory.createAsyncMetaPool(ClientCategory.META);

    Assert.assertEquals(pool.getMaxTotalPerKey(), mockMaxClientPerMember);
    Assert.assertEquals(pool.getMaxWaitDuration(), Duration.ofMillis(mockMaxWaitTimeoutMs));

    Node node = constructDefaultNode();

    RaftService.AsyncClient asyncClient = null;
    asyncClient = pool.borrowObject(node);
    mockClientManager.setAsyncClient(asyncClient);
    Assert.assertNotNull(asyncClient);
    Assert.assertTrue(asyncClient instanceof AsyncMetaClient);
  }

  @Test
  public void createSyncDataClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.Client> pool =
        clientPoolFactory.createSyncDataPool(ClientCategory.DATA_HEARTBEAT);

    Assert.assertEquals(pool.getMaxTotalPerKey(), mockMaxClientPerMember);
    Assert.assertEquals(pool.getMaxWaitDuration(), Duration.ofMillis(mockMaxWaitTimeoutMs));

    Node node = constructDefaultNode();

    RaftService.Client client = null;
    ServerSocket serverSocket =
        new ServerSocket(ClientUtils.getPort(node, ClientCategory.DATA_HEARTBEAT));
    Thread listenThread = null;
    try {
      listenThread =
          new Thread(
              () -> {
                while (!Thread.interrupted()) {
                  try {
                    serverSocket.accept();
                  } catch (IOException e) {
                    return;
                  }
                }
              });
      listenThread.start();

      client = pool.borrowObject(node);
      mockClientManager.setSyncClient(client);
      Assert.assertNotNull(client);
      Assert.assertTrue(client instanceof SyncDataClient);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      ((SyncDataClient) client).returnSelf();
      if (serverSocket != null) {
        serverSocket.close();
        listenThread.interrupt();
        listenThread.join();
      }
    }
  }

  @Test
  public void createSyncMetaClientTest() throws Exception {
    GenericKeyedObjectPool<Node, RaftService.Client> pool =
        clientPoolFactory.createSyncMetaPool(ClientCategory.META_HEARTBEAT);

    Assert.assertEquals(pool.getMaxTotalPerKey(), mockMaxClientPerMember);
    Assert.assertEquals(pool.getMaxWaitDuration(), Duration.ofMillis(mockMaxWaitTimeoutMs));

    Node node = constructDefaultNode();

    RaftService.Client client = null;
    ServerSocket serverSocket =
        new ServerSocket(ClientUtils.getPort(node, ClientCategory.META_HEARTBEAT));
    Thread listenThread = null;
    try {
      listenThread =
          new Thread(
              () -> {
                while (!Thread.interrupted()) {
                  try {
                    serverSocket.accept();
                  } catch (IOException e) {
                    return;
                  }
                }
              });
      listenThread.start();

      client = pool.borrowObject(node);
      mockClientManager.setSyncClient(client);
      Assert.assertNotNull(client);
      Assert.assertTrue(client instanceof SyncMetaClient);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      ((SyncMetaClient) client).returnSelf();
      if (serverSocket != null) {
        serverSocket.close();
        listenThread.interrupt();
        listenThread.join();
      }
    }
  }

  private Node constructDefaultNode() {
    Node node = new Node();
    node.setMetaPort(9003).setInternalIp("localhost").setClientIp("localhost");
    node.setDataPort(40010);
    return node;
  }
}
