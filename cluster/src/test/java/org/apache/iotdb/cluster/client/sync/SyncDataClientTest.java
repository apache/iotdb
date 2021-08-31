/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.client.sync.SyncDataClient.FactorySync;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.rpc.TSocketWrapper;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SyncDataClientTest {

  @Test
  public void test() throws IOException, InterruptedException {
    Node node = new Node();
    node.setDataPort(40010).setInternalIp("localhost").setClientIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread =
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

      assertEquals(
          "DataClient{node=ClusterNode{ internalIp='localhost', metaPort=0, nodeIdentifier=0,"
              + " dataPort=40010, clientPort=0, clientIp='localhost'}}",
          client.toString());

      client =
          new SyncDataClient(
              new TBinaryProtocol(TSocketWrapper.wrap(node.getInternalIp(), node.getDataPort())));
      // client without a belong pool will be closed after putBack()
      client.putBack();
      assertFalse(client.getInputProtocol().getTransport().isOpen());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }

  @Test
  public void testTryClose() throws IOException, InterruptedException {
    Node node = new Node();
    node.setDataPort(40010).setInternalIp("localhost").setClientIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread =
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

    try {
      SyncClientPool syncClientPool = new SyncClientPool(new FactorySync(new Factory()));
      SyncDataClient clientOut;
      try (SyncDataClient clientIn = (SyncDataClient) syncClientPool.getClient(node)) {
        assertEquals(node, clientIn.getNode());
        clientIn.setTimeout(1000);
        clientOut = clientIn;
        assertEquals(1000, clientIn.getTimeout());
      }
      assertTrue(clientOut.getInputProtocol().getTransport().isOpen());

      try (SyncDataClient newClient = (SyncDataClient) syncClientPool.getClient(node)) {
        assertEquals(clientOut, newClient);
        assertEquals(
            "DataClient{node=ClusterNode{ internalIp='localhost', metaPort=0, nodeIdentifier=0,"
                + " dataPort=40010, clientPort=0, clientIp='localhost'}}",
            newClient.toString());
      }

      try (SyncDataClient clientIn =
          new SyncDataClient(
              new TBinaryProtocol(TSocketWrapper.wrap(node.getInternalIp(), node.getDataPort())))) {
        clientOut = clientIn;
      }
      // client without a belong pool will be closed after putBack()
      assertFalse(clientOut.getInputProtocol().getTransport().isOpen());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }
}
