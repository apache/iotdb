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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.async.AsyncClientFactory;
import org.apache.iotdb.cluster.client.async.AsyncDataClient.FactoryAsync;
import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestAsyncClientFactory;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;
import org.mockito.Mock;

public class AsyncClientPoolTest {

  @Mock
  private AsyncClientFactory testAsyncClientFactory;

  @Test
  public void testTestClient() throws IOException {
    testAsyncClientFactory = new TestAsyncClientFactory();
    getClient();
    putClient();
  }

  @Test
  public void testDataClient() throws IOException {
    testAsyncClientFactory = new FactoryAsync(new TBinaryProtocol.Factory());
    getClient();
    putClient();
  }

  @Test
  public void testMetaClient() throws IOException {
    testAsyncClientFactory = new AsyncMetaClient.FactoryAsync(new TBinaryProtocol.Factory());
    getClient();
    putClient();
  }

  private void getClient() throws IOException {
    AsyncClientPool asyncClientPool = new AsyncClientPool(testAsyncClientFactory);
    for (int i = 0; i < 10; i++) {
      AsyncClient client = asyncClientPool.getClient(TestUtils.getNode(i));
      if (client instanceof TestClient) {
        TestClient testClient = (TestClient) client;
        assertEquals(i, testClient.getSerialNum());
      }
    }
  }

  private void putClient() throws IOException {
    AsyncClientPool asyncClientPool = new AsyncClientPool(testAsyncClientFactory);
    List<AsyncClient> testClients = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      AsyncClient client = asyncClientPool.getClient(TestUtils.getNode(i));
      testClients.add(client);
    }
    if (testAsyncClientFactory instanceof TestAsyncClientFactory) {
      for (int i = 0; i < 10; i++) {
        asyncClientPool.putClient(TestUtils.getNode(i), testClients.get(i));
      }
    } else if (testAsyncClientFactory instanceof AsyncMetaClient.FactoryAsync){
      for (AsyncClient testClient : testClients) {
        ((AsyncMetaClient) testClient).onComplete();
      }
    } else if (testAsyncClientFactory instanceof FactoryAsync){
      for (AsyncClient testClient : testClients) {
        ((AsyncDataClient) testClient).onComplete();
      }
    }

    for (int i = 0; i < 10; i++) {
      AsyncClient poolClient = asyncClientPool.getClient(TestUtils.getNode(i));
      assertEquals(testClients.get(i), poolClient);
    }
  }

  @Test
  public void testMaxClient() throws IOException {
    int maxClientNum = ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(5);
    testAsyncClientFactory = new TestAsyncClientFactory();
    AsyncClientPool asyncClientPool = new AsyncClientPool(testAsyncClientFactory);

    for (int i = 0; i < 5; i++) {
      asyncClientPool.getClient(TestUtils.getNode(0));
    }
    AtomicReference<AsyncClient> reference = new AtomicReference<>();
    Thread t = new Thread(() -> {
      try {
        reference.set(asyncClientPool.getClient(TestUtils.getNode(0)));
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
    t.start();
    t.interrupt();
    assertNull(reference.get());
    ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(maxClientNum);
  }

}