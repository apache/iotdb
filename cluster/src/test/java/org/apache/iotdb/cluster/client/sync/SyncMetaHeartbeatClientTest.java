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

package org.apache.iotdb.cluster.client.sync;

import org.apache.iotdb.cluster.client.sync.SyncMetaHeartbeatClient.FactorySync;
import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

public class SyncMetaHeartbeatClientTest {

  @Test
  public void test() throws IOException, TTransportException, InterruptedException {
    Node node = new Node();
    node.setMetaPort(9003).setInternalIp("localhost").setClientIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getMetaPort() + 1);
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
      FactorySync factoryAsync = new FactorySync(new Factory());
      SyncMetaHeartbeatClient syncClient = factoryAsync.getSyncClient(node, null);
      Assert.assertEquals(
          "SyncMetaHeartbeatClient{node=Node(internalIp:localhost, metaPort:9003,"
              + " nodeIdentifier:0, dataPort:0, clientPort:0, clientIp:localhost),metaHeartbeatPort=9004}",
          syncClient.toString());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }
}
