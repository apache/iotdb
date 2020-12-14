/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.cluster.client.sync;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncDataClient.SingleManagerFactory;
import org.apache.iotdb.cluster.client.sync.SyncDataClient.FactorySync;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.junit.Test;

public class SyncDataClientTest {

  @Test
  public void test() throws IOException, InterruptedException {
    Node node = new Node();
    node.setDataPort(40010).setIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread = new Thread(() -> {
      while (!Thread.interrupted()) {
        try {
          serverSocket.accept();
        } catch (IOException e) {
          return;
        }
      }
    });
    listenThread.start();

    try {
      SyncClientPool syncClientPool = new SyncClientPool(new FactorySync(new Factory()));
      SyncDataClient client;
      client = (SyncDataClient) syncClientPool.getClient(node);

      assertEquals(node, client.getNode());

      client.setTimeout(1000);
      assertEquals(1000, client.getTimeout());

      client.putBack();
      Client newClient = syncClientPool.getClient(node);
      assertEquals(client, newClient);
      assertTrue(client.getInputProtocol().getTransport().isOpen());

      assertEquals("DataClient{node=ClusterNode{ ip='localhost', metaPort=0, nodeIdentifier=0,"
          + " dataPort=40010, clientPort=0}}", client.toString());

      client = new SyncDataClient(new TBinaryProtocol(new TSocket(node.getIp(),
          node.getDataPort())));
      // client without a belong pool will be closed after putBack()
      client.putBack();
      assertFalse(client.getInputProtocol().getTransport().isOpen());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }
}
