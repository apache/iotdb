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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;

import org.apache.iotdb.cluster.utils.ClusterNode;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.Assert;
import org.junit.Test;

public class DataClientProviderTest {

  @Test
  public void testAsync() throws IOException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    DataClientProvider provider = new DataClientProvider(new Factory());

    assertNotNull(provider.getAsyncDataClient(TestUtils.getNode(0), 100));
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
  }

  @Test
  public void testSync() throws IOException, InterruptedException {
    Node node = new ClusterNode();
    node.setDataPort(9003).setIp("localhost");
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
      boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
      ClusterDescriptor.getInstance().getConfig().setMaxClientPerNodePerMember(2);
      DataClientProvider provider = new DataClientProvider(new Factory());
      SyncDataClient client = null;
      try {
        client = provider.getSyncDataClient(node, 100);
      } catch (TException e) {
        Assert.fail(e.getMessage());
      }
      assertNotNull(client);

      //now try to test multi thread
      ExecutorService service = Executors.newFixedThreadPool(10);
      for (int i = 0; i < 4; i++) {
        service.submit(() -> provider.getSyncDataClient(node, 100));
      }

      //wait time should be great then 5000ms
      Thread.currentThread().sleep(10000);
      int totalNumber = provider.getDataSyncClientPool().getNodeClientNumMap().get(node);
      assertEquals(5, totalNumber);

      for (int i = 0; i < 4; i++) {
        service.submit(() -> provider.getSyncDataClient(node, 100));
      }

      Thread.currentThread().sleep(1000);
      //return one client to pool
      provider.getDataSyncClientPool().putClient(node, client);
      //wait all finish
      Thread.currentThread().sleep(10000);
      totalNumber = provider.getDataSyncClientPool().getNodeClientNumMap().get(node);

      //5 + 4 - 1
      assertEquals(8, totalNumber);

      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }
}
