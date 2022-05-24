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

package org.apache.iotdb.commons;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolProperty;
import org.apache.iotdb.commons.client.ClientPoolProperty.DefaultProperty;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class ClientManagerTest {

  private final TEndPoint endPoint = new TEndPoint("localhost", 9003);

  private ServerSocket metaServer;
  private Thread metaServerListeningThread;

  @Before
  public void setUp() throws IOException {
    startServer();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    stopServer();
  }

  @Test
  public void normalSyncClientManagersTest() throws Exception {
    // init syncClientManager
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(new TestSyncDataNodeInternalServiceClientPoolFactory());

    // get one sync client
    SyncDataNodeInternalServiceClient syncClient1 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient1);
    Assert.assertEquals(syncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient1.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // get another sync client
    SyncDataNodeInternalServiceClient syncClient2 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient2);
    Assert.assertEquals(syncClient2.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient2.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient2.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(2, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // return one sync client
    syncClient1.close();
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // return another sync client
    syncClient2.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(2, syncClusterManager.getPool().getNumIdle(endPoint));

    // close syncClientManager, syncClientManager should destroy all client
    syncClusterManager.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertFalse(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertFalse(syncClient2.getInputProtocol().getTransport().isOpen());
  }

  @Test
  public void normalAsyncClientManagersTest() throws Exception {
    // init asyncClientManager
    ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> asyncClusterManager =
        (ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
                .createClientManager(new TestAsyncDataNodeInternalServiceClientPoolFactory());

    // get one async client
    AsyncDataNodeInternalServiceClient asyncClient1 = asyncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(asyncClient1);
    Assert.assertEquals(asyncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(asyncClient1.getClientManager(), asyncClusterManager);
    Assert.assertTrue(asyncClient1.isReady());
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));

    // get another async client
    AsyncDataNodeInternalServiceClient asyncClient2 = asyncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(asyncClient2);
    Assert.assertEquals(asyncClient2.getTEndpoint(), endPoint);
    Assert.assertEquals(asyncClient2.getClientManager(), asyncClusterManager);
    Assert.assertTrue(asyncClient2.isReady());
    Assert.assertEquals(2, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));

    // return one async client
    asyncClient1.onComplete();
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumIdle(endPoint));

    // return another async client
    asyncClient2.onComplete();
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(2, asyncClusterManager.getPool().getNumIdle(endPoint));

    // close asyncClientManager, asyncClientManager should destroy all client
    asyncClusterManager.close();
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));
  }

  @Test
  public void MaxIdleClientManagersTest() throws Exception {
    int maxIdleClientForEachNode = 1;

    // init syncClientManager and set maxIdleClientForEachNode to 1
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    new TestSyncDataNodeInternalServiceClientPoolFactory() {
                      @Override
                      public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient>
                          createClientPool(
                              ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
                        return new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager, new ClientFactoryProperty.Builder().build()),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .setMaxIdleClientForEachNode(maxIdleClientForEachNode)
                                .build()
                                .getConfig());
                      }
                    });

    // get one sync client
    SyncDataNodeInternalServiceClient syncClient1 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient1);
    Assert.assertEquals(syncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient1.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // get another sync client
    SyncDataNodeInternalServiceClient syncClient2 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient2);
    Assert.assertEquals(syncClient2.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient2.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient2.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(2, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // return one sync client
    syncClient1.close();
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // return another sync client, clientManager should destroy this client
    syncClient2.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertFalse(syncClient2.getInputProtocol().getTransport().isOpen());

    // close syncClientManager, syncClientManager should destroy all client
    syncClusterManager.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertFalse(syncClient1.getInputProtocol().getTransport().isOpen());
  }

  @Test
  public void MaxTotalClientManagersTest() throws Exception {
    int maxTotalClientForEachNode = 1;

    // init syncClientManager and set maxTotalClientForEachNode to 1
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    new TestSyncDataNodeInternalServiceClientPoolFactory() {
                      @Override
                      public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient>
                          createClientPool(
                              ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
                        return new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager, new ClientFactoryProperty.Builder().build()),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .setMaxTotalClientForEachNode(maxTotalClientForEachNode)
                                .build()
                                .getConfig());
                      }
                    });

    // get one sync client
    SyncDataNodeInternalServiceClient syncClient1 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient1);
    Assert.assertEquals(syncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient1.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // get another sync client, should wait waitClientTimeoutMS ms, throw error
    SyncDataNodeInternalServiceClient syncClient2 = null;
    long start = 0, end;
    try {
      start = System.nanoTime();
      syncClient2 = syncClusterManager.borrowClient(endPoint);
    } catch (IOException e) {
      end = System.nanoTime();
      Assert.assertTrue(end - start >= DefaultProperty.WAIT_CLIENT_TIMEOUT_MS * 1_000_000);
      Assert.assertTrue(e.getMessage().startsWith("Borrow client from pool for node"));
    }
    Assert.assertNull(syncClient2);

    // return one sync client
    syncClient1.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // get sync client again, should return the only client
    syncClient2 = syncClusterManager.borrowClient(endPoint);
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertEquals(syncClient1, syncClient2);

    // return the only client
    syncClient2.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // close syncClientManager, syncClientManager should destroy all client
    syncClusterManager.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertFalse(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertFalse(syncClient2.getInputProtocol().getTransport().isOpen());
  }

  @Test
  public void MaxWaitClientTimeoutClientManagersTest() throws Exception {
    long waitClientTimeoutMS = DefaultProperty.WAIT_CLIENT_TIMEOUT_MS * 2;
    int maxTotalClientForEachNode = 1;

    // init syncClientManager and set maxTotalClientForEachNode to 1, set waitClientTimeoutMS to
    // DefaultProperty.WAIT_CLIENT_TIMEOUT_MS * 2
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    new TestSyncDataNodeInternalServiceClientPoolFactory() {
                      @Override
                      public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient>
                          createClientPool(
                              ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
                        return new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager, new ClientFactoryProperty.Builder().build()),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .setWaitClientTimeoutMS(waitClientTimeoutMS)
                                .setMaxTotalClientForEachNode(maxTotalClientForEachNode)
                                .build()
                                .getConfig());
                      }
                    });

    // get one sync client
    SyncDataNodeInternalServiceClient syncClient1 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient1);
    Assert.assertEquals(syncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient1.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // get another sync client, should wait waitClientTimeoutMS ms, throw error
    long start = 0, end;
    try {
      start = System.nanoTime();
      syncClusterManager.borrowClient(endPoint);
    } catch (IOException e) {
      end = System.nanoTime();
      Assert.assertTrue(end - start >= waitClientTimeoutMS * 1_000_000);
      Assert.assertTrue(e.getMessage().startsWith("Borrow client from pool for node"));
    }

    // return one sync client
    syncClient1.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // close syncClientManager, syncClientManager should destroy all client
    syncClusterManager.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertFalse(syncClient1.getInputProtocol().getTransport().isOpen());
  }

  @Test
  public void InvalidSyncClientReturnClientManagersTest() throws Exception {
    // init syncClientManager
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(new TestSyncDataNodeInternalServiceClientPoolFactory());

    // get one sync client
    SyncDataNodeInternalServiceClient syncClient1 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient1);
    Assert.assertEquals(syncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient1.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // get another sync client
    SyncDataNodeInternalServiceClient syncClient2 = syncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(syncClient2);
    Assert.assertEquals(syncClient2.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient2.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient2.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(2, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // return one sync client
    syncClient1.close();
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // invalid another sync client and return
    syncClient2.getInputProtocol().getTransport().close();
    syncClient2.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // close syncClientManager, syncClientManager should destroy all client
    syncClusterManager.close();
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
    Assert.assertFalse(syncClient2.getInputProtocol().getTransport().isOpen());
  }

  @Test
  public void InvalidAsyncClientReturnClientManagersTest() throws Exception {
    // init asyncClientManager
    ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> asyncClusterManager =
        (ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
                .createClientManager(new TestAsyncDataNodeInternalServiceClientPoolFactory());

    // get one async client
    AsyncDataNodeInternalServiceClient asyncClient1 = asyncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(asyncClient1);
    Assert.assertEquals(asyncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(asyncClient1.getClientManager(), asyncClusterManager);
    Assert.assertTrue(asyncClient1.isReady());
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));

    // get another async client
    AsyncDataNodeInternalServiceClient asyncClient2 = asyncClusterManager.borrowClient(endPoint);
    Assert.assertNotNull(asyncClient2);
    Assert.assertEquals(asyncClient2.getTEndpoint(), endPoint);
    Assert.assertEquals(asyncClient2.getClientManager(), asyncClusterManager);
    Assert.assertTrue(asyncClient2.isReady());
    Assert.assertEquals(2, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));

    // return one async client
    asyncClient1.onComplete();
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumIdle(endPoint));

    // invalid another async client and return
    asyncClient2.onError(new Exception("socket time out"));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumIdle(endPoint));

    // close asyncClientManager, asyncClientManager should destroy all client
    asyncClusterManager.close();
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));
  }

  public void startServer() throws IOException {
    metaServer = new ServerSocket();
    // reuse the port to avoid `Bind Address already in use` which is caused by TIME_WAIT state
    // because port won't be usable immediately after we close it.
    metaServer.setReuseAddress(true);
    metaServer.bind(new InetSocketAddress(endPoint.getIp(), endPoint.getPort()), 0);

    metaServerListeningThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  metaServer.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    metaServerListeningThread.start();
  }

  public void stopServer() throws InterruptedException, IOException {
    if (metaServer != null) {
      metaServer.close();
      metaServer = null;
    }
    if (metaServerListeningThread != null) {
      metaServerListeningThread.interrupt();
      metaServerListeningThread.join();
      metaServerListeningThread = null;
    }
  }

  public static class TestSyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeInternalServiceClient.Factory(
              manager, new ClientFactoryProperty.Builder().build()),
          new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>().build().getConfig());
    }
  }

  public static class TestAsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(
              manager, new ClientFactoryProperty.Builder().build()),
          new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>().build().getConfig());
    }
  }
}
