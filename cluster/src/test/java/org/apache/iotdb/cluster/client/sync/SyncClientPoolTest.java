/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.common.TestSyncClient;
import org.apache.iotdb.cluster.common.TestSyncClientFactory;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SyncClientPoolTest {

  @Mock private SyncClientFactory testSyncClientFactory;

  @Before
  public void setUp() {
    testSyncClientFactory = new TestSyncClientFactory();
  }

  @After
  public void tearDown() {
    testSyncClientFactory = null;
  }

  @Test
  public void testTestClient() {
    getClient();
    putClient();
  }

  private void getClient() {
    SyncClientPool syncClientPool = new SyncClientPool(testSyncClientFactory);
    for (int i = 0; i < 10; i++) {
      Client client = syncClientPool.getClient(TestUtils.getNode(i));
      if (client instanceof TestSyncClient) {
        TestSyncClient testSyncClient = (TestSyncClient) client;
        assertEquals(i, testSyncClient.getSerialNum());
      }
    }
  }

  private void putClient() {
    SyncClientPool syncClientPool = new SyncClientPool(testSyncClientFactory);
    List<Client> testClients = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Client client = syncClientPool.getClient(TestUtils.getNode(i));
      testClients.add(client);
    }
    for (int i = 0; i < 10; i++) {
      syncClientPool.putClient(TestUtils.getNode(i), testClients.get(i));
    }

    for (int i = 0; i < 10; i++) {
      Client poolClient = syncClientPool.getClient(TestUtils.getNode(i));
      assertEquals(testClients.get(i), poolClient);
    }
  }

  @Test
  public void testPutBadClient() {
    SyncClientPool syncClientPool = new SyncClientPool(testSyncClientFactory);
    Client client = syncClientPool.getClient(TestUtils.getNode(0));
    client.getInputProtocol().getTransport().close();
    syncClientPool.putClient(TestUtils.getNode(0), client);
    Client newClient = syncClientPool.getClient(TestUtils.getNode(0));
    assertNotEquals(client, newClient);
  }

  @Test
  public void testMaxClient() {
    int maxClientNum = ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(5);
    SyncClientPool syncClientPool = new SyncClientPool(testSyncClientFactory);

    for (int i = 0; i < 5; i++) {
      syncClientPool.getClient(TestUtils.getNode(0));
    }
    AtomicReference<Client> reference = new AtomicReference<>();
    Thread t = new Thread(() -> reference.set(syncClientPool.getClient(TestUtils.getNode(0))));
    t.start();
    t.interrupt();
    assertNull(reference.get());
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(maxClientNum);
  }

  @Test
  public void testWaitClient() {
    int maxClientPerNodePerMember =
        ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
    try {
      ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(10);
      SyncClientPool syncClientPool = new SyncClientPool(testSyncClientFactory);

      Node node = TestUtils.getNode(0);
      List<Client> clients = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        clients.add(syncClientPool.getClient(node));
      }

      AtomicBoolean waitStart = new AtomicBoolean(false);
      new Thread(
              () -> {
                while (!waitStart.get()) {
                  // wait until we start to for wait for a client
                }
                synchronized (syncClientPool) {
                  for (Client client : clients) {
                    syncClientPool.putClient(node, client);
                  }
                }
              })
          .start();

      Client client;
      synchronized (syncClientPool) {
        waitStart.set(true);
        // getClient() will wait on syncClientPool, so the thread above can return clients
        client = syncClientPool.getClient(node);
      }
      assertNotNull(client);
    } finally {
      ClusterDescriptor.getInstance()
          .getConfig()
          .setMaxClientPerNodePerMember(maxClientPerNodePerMember);
    }
  }

  @Test
  public void testWaitClientTimeOut() {
    int maxClientPerNodePerMember =
        ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
    try {
      ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(1);
      SyncClientPool syncClientPool = new SyncClientPool(testSyncClientFactory);

      Node node = TestUtils.getNode(0);
      List<Client> clients = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        clients.add(syncClientPool.getClient(node));
      }

      assertNotEquals(clients.get(0), clients.get(1));
    } finally {
      ClusterDescriptor.getInstance()
          .getConfig()
          .setMaxClientPerNodePerMember(maxClientPerNodePerMember);
    }
  }
}
