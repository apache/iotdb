/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.client.ClientFactory;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.client.MetaClient;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestClientFactory;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;
import org.mockito.Mock;

public class ClientPoolTest {

  @Mock
  private ClientFactory testClientFactory;

  @Test
  public void testTestClient() throws IOException {
    testClientFactory = new TestClientFactory();
    getClient();
    putClient();
  }

  @Test
  public void testDataClient() throws IOException {
    testClientFactory = new DataClient.Factory(new TAsyncClientManager(), new TBinaryProtocol.Factory());
    getClient();
    putClient();
  }

  @Test
  public void testMetaClient() throws IOException {
    testClientFactory = new MetaClient.Factory(new TAsyncClientManager(), new TBinaryProtocol.Factory());
    getClient();
    putClient();
  }

  private void getClient() throws IOException {
    ClientPool clientPool = new ClientPool(testClientFactory);
    for (int i = 0; i < 10; i++) {
      AsyncClient client = clientPool.getClient(TestUtils.getNode(i));
      if (client instanceof TestClient) {
        TestClient testClient = (TestClient) client;
        assertEquals(i, testClient.getSerialNum());
      }
    }
  }

  private void putClient() throws IOException {
    ClientPool clientPool = new ClientPool(testClientFactory);
    List<AsyncClient> testClients = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      AsyncClient client = clientPool.getClient(TestUtils.getNode(i));
      testClients.add(client);
    }
    if (testClientFactory instanceof TestClientFactory) {
      for (int i = 0; i < 10; i++) {
        clientPool.putClient(TestUtils.getNode(i), testClients.get(i));
      }
    } else if (testClientFactory instanceof MetaClient.Factory){
      for (AsyncClient testClient : testClients) {
        ((MetaClient) testClient).onComplete();
      }
    } else if (testClientFactory instanceof DataClient.Factory){
      for (AsyncClient testClient : testClients) {
        ((DataClient) testClient).onComplete();
      }
    }

    for (int i = 0; i < 10; i++) {
      AsyncClient poolClient = clientPool.getClient(TestUtils.getNode(i));
      assertEquals(testClients.get(i), poolClient);
    }

  }

}