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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeInternalServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.BorrowNullClientManagerException;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.mock.MockInternalRPCService;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.schema.cache.CacheClearOptions;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClientManagerTest {

  private final TEndPoint endPoint = new TEndPoint("localhost", 10730);

  private static final int CONNECTION_TIMEOUT = 5_000;

  private MockInternalRPCService service;

  @SuppressWarnings("java:S2925")
  @Before
  public void setUp() throws StartupException, TException {
    service = new MockInternalRPCService(endPoint);
    IDataNodeRPCService.Iface processor = mock(IDataNodeRPCService.Iface.class);
    // timeout method
    when(processor.clearCache(Collections.singleton(CacheClearOptions.DEFAULT.ordinal())))
        .thenAnswer(
            invocation -> {
              Thread.sleep(CONNECTION_TIMEOUT + 1000);
              return new TSStatus();
            });
    // normal method
    when(processor.merge())
        .thenAnswer(
            invocation -> {
              Thread.sleep(1000);
              return new TSStatus();
            });
    service.initSyncedServiceImpl(processor);
    service.start();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    service.waitAndStop(10_000L);
  }

  /**
   * We put all tests together to avoid frequent restarts of thrift Servers, which can cause "bind
   * address already used" problems in macOS CI environments. The reason for this may be about this
   * <a
   * href="https://stackoverflow.com/questions/51998042/macos-so-reuseaddr-so-reuseport-not-consistent-with-linux">blog</a>
   */
  @Test
  public void allTest() throws Exception {
    normalSyncTest();
    normalAsyncTest();
    evictionTest();
    maxTotalTest();
    maxWaitClientTimeoutTest();
    invalidSyncClientReturnTest();
    invalidAsyncClientReturnTest();
    borrowNullTest();
    asyncFailureReporterTest();
    clientFactoryConstructionFailureReporterTest();
    syncFailureReporterTest();
    syncConfigNodeFailureReporterTest();
    auditClientPoolShouldNotReuseHeartbeatFailureReporterTest();
    legacyFailureReporterFactoryConstructorsTest();
    syncClientTimeoutTest();
    asyncClientTimeoutTest();
  }

  public void normalSyncTest() throws Exception {
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

  public void normalAsyncTest() throws Exception {
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

  public void evictionTest() throws Exception {
    List<SyncDataNodeInternalServiceClient> evictionTestClients = new ArrayList<>();
    int maxClientForEachNode = 2;
    long minIdleDuration = TimeUnit.SECONDS.toMillis(10);
    long evictionRunsDuration = TimeUnit.SECONDS.toMillis(2);

    // init syncClientManager and set minIdleDuation and evictionRunsDuration
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    new TestSyncDataNodeInternalServiceClientPoolFactory() {
                      @Override
                      public GenericKeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient>
                          createClientPool(
                              ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
                        return new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager, new ThriftClientProperty.Builder().build()),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .setMaxClientNumForEachNode(maxClientForEachNode)
                                .setMaxIdleClientNumForEachNode(maxClientForEachNode)
                                .setMinIdleTimeForClient(minIdleDuration)
                                .setTimeBetweenEvictionRuns(evictionRunsDuration)
                                .build()
                                .getConfig());
                      }
                    });

    // get one sync client
    SyncDataNodeInternalServiceClient syncClient1 = syncClusterManager.borrowClient(endPoint);
    evictionTestClients.add(syncClient1);
    Assert.assertNotNull(syncClient1);
    Assert.assertEquals(syncClient1.getTEndpoint(), endPoint);
    Assert.assertEquals(syncClient1.getClientManager(), syncClusterManager);
    Assert.assertTrue(syncClient1.getInputProtocol().getTransport().isOpen());
    Assert.assertEquals(1, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    // get another sync client
    SyncDataNodeInternalServiceClient syncClient2 = syncClusterManager.borrowClient(endPoint);
    evictionTestClients.add(syncClient2);
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

    long start = System.currentTimeMillis();
    while (syncClusterManager.getPool().getNumIdle() > 0
        || (syncClient1.getInputProtocol().getTransport().isOpen()
            || syncClient2.getInputProtocol().getTransport().isOpen())) {
      for (SyncDataNodeInternalServiceClient evictionTestClient : evictionTestClients) {
        // if this client is evicted, skip it
        if (!evictionTestClient.getInputProtocol().getTransport().isOpen()) continue;
        // test eviction
        long current = System.currentTimeMillis();
        // for each idle client, its theoretical max idle time is `minIdleDuration` +
        // `evictionRunsDuration`. Taking into account the difference in thread scheduling rates of
        // different machines, here we multiply by 6
        if ((current - start) > (minIdleDuration + evictionRunsDuration) * 6) {
          Assert.fail("Evict invalid client failed");
        }
      }
      Thread.sleep(100);
    }
    // since the two clients are idle for more than 10s, which exceeds `minIdleDuration`, they
    // should be destroyed.
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));
  }

  public void maxTotalTest() throws Exception {
    int maxTotalClientForEachNode = 1;
    long waitClientTimeoutMs = TimeUnit.SECONDS.toMillis(1);

    // init syncClientManager and set maxTotalClientForEachNode to 1
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    new TestSyncDataNodeInternalServiceClientPoolFactory() {
                      @Override
                      public GenericKeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient>
                          createClientPool(
                              ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
                        return new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager, new ThriftClientProperty.Builder().build()),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .setMaxClientNumForEachNode(maxTotalClientForEachNode)
                                .setMaxIdleClientNumForEachNode(maxTotalClientForEachNode)
                                .setWaitClientTimeoutMs(waitClientTimeoutMs)
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
    long start = 0;
    try {
      start = System.nanoTime();
      syncClient2 = syncClusterManager.borrowClient(endPoint);
      Assert.fail();
    } catch (ClientManagerException e) {
      long end = System.nanoTime();
      Assert.assertTrue(end - start >= waitClientTimeoutMs * 1_000_000);
      Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
      Assert.assertTrue(e.getMessage().contains("Timeout waiting for idle object"));
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

  public void maxWaitClientTimeoutTest() throws Exception {
    long waitClientTimeoutMS = TimeUnit.SECONDS.toMillis(2);
    int maxTotalClientForEachNode = 1;

    // init syncClientManager and set maxTotalClientForEachNode to 1, set waitClientTimeoutMS to
    // DefaultProperty.WAIT_CLIENT_TIMEOUT_MS * 2
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    new TestSyncDataNodeInternalServiceClientPoolFactory() {
                      @Override
                      public GenericKeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient>
                          createClientPool(
                              ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
                        return new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager, new ThriftClientProperty.Builder().build()),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .setWaitClientTimeoutMs(waitClientTimeoutMS)
                                .setMaxClientNumForEachNode(maxTotalClientForEachNode)
                                .setMaxIdleClientNumForEachNode(maxTotalClientForEachNode)
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
    long start = 0;
    try {
      start = System.nanoTime();
      syncClient1 = syncClusterManager.borrowClient(endPoint);
      Assert.fail();
    } catch (ClientManagerException e) {
      long end = System.nanoTime();
      Assert.assertTrue(end - start >= waitClientTimeoutMS * 1_000_000);
      Assert.assertTrue(e.getCause() instanceof NoSuchElementException);
      Assert.assertTrue(e.getMessage().contains("Timeout waiting for idle object"));
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

  public void invalidSyncClientReturnTest() throws Exception {
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

  public void invalidAsyncClientReturnTest() throws Exception {
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

  public void borrowNullTest() {
    // init asyncClientManager
    ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> asyncClusterManager =
        (ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
                .createClientManager(new TestAsyncDataNodeInternalServiceClientPoolFactory());

    try {
      asyncClusterManager.borrowClient(null);
      Assert.fail();
    } catch (ClientManagerException e) {
      Assert.assertTrue(e instanceof BorrowNullClientManagerException);
      Assert.assertTrue(e.getMessage().contains("Can not borrow client for node null"));
    }

    // close asyncClientManager, asyncClientManager should destroy all client
    asyncClusterManager.close();
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));
  }

  public void syncClientTimeoutTest() throws Exception {
    // init syncClientManager
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(new TestSyncDataNodeInternalServiceClientPoolFactory());

    // normal RPC
    try (SyncDataNodeInternalServiceClient syncClient = syncClusterManager.borrowClient(endPoint)) {
      syncClient.merge();
    } catch (Exception e) {
      Assert.fail("There should be no timeout here");
    }
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, syncClusterManager.getPool().getNumIdle(endPoint));

    // timeout RPC
    try (SyncDataNodeInternalServiceClient syncClient = syncClusterManager.borrowClient(endPoint)) {
      syncClient.clearCache(Collections.singleton(CacheClearOptions.DEFAULT.ordinal()));
      Assert.fail("A timeout exception should occur here");
    } catch (Exception ignored) {
      // no handling
    }
    Assert.assertEquals(0, syncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, syncClusterManager.getPool().getNumIdle(endPoint));

    syncClusterManager.close();
  }

  public void asyncClientTimeoutTest() throws Exception {
    // init asyncClientManager
    ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> asyncClusterManager =
        (ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
                .createClientManager(new TestAsyncDataNodeInternalServiceClientPoolFactory());

    // normal RPC
    AsyncDataNodeInternalServiceClient asyncClient = asyncClusterManager.borrowClient(endPoint);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean failed = new AtomicBoolean(false);
    CountDownLatch finalLatch = latch;
    AtomicBoolean finalFailed = failed;
    asyncClient.merge(
        new AsyncMethodCallback<TSStatus>() {
          @Override
          public void onComplete(TSStatus response) {
            finalLatch.countDown();
          }

          @Override
          public void onError(Exception exception) {
            finalFailed.set(true);
            finalLatch.countDown();
          }
        });
    latch.await();
    if (failed.get()) {
      Assert.fail("There should be no timeout here");
    }
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(1, asyncClusterManager.getPool().getNumIdle(endPoint));

    // timeout RPC
    asyncClient = asyncClusterManager.borrowClient(endPoint);
    latch = new CountDownLatch(1);
    failed = new AtomicBoolean(false);
    AtomicBoolean finalFailed1 = failed;
    CountDownLatch finalLatch1 = latch;
    asyncClient.clearCache(
        Collections.singleton(CacheClearOptions.DEFAULT.ordinal()),
        new AsyncMethodCallback<TSStatus>() {
          @Override
          public void onComplete(TSStatus response) {
            finalFailed1.set(true);
            finalLatch1.countDown();
          }

          @Override
          public void onError(Exception exception) {
            finalLatch1.countDown();
          }
        });
    latch.await();
    if (failed.get()) {
      Assert.fail("A timeout exception should occur here");
    }
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumActive(endPoint));
    Assert.assertEquals(0, asyncClusterManager.getPool().getNumIdle(endPoint));

    asyncClusterManager.close();
  }

  public void asyncFailureReporterTest() throws Exception {
    TAsyncClientManager thriftClientManager = new TAsyncClientManager();
    try {
      ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> dataNodeClientManager =
          mock(ClientManager.class);
      AtomicReference<Throwable> dataNodeFailure = new AtomicReference<>();
      AtomicReference<TEndPoint> dataNodeTarget = new AtomicReference<>();
      AsyncDataNodeInternalServiceClient dataNodeClient =
          new AsyncDataNodeInternalServiceClient(
              new ThriftClientProperty.Builder().build(),
              endPoint,
              thriftClientManager,
              dataNodeClientManager,
              (failure, target) -> {
                dataNodeFailure.set(failure);
                dataNodeTarget.set(target);
              });
      Exception dataNodeException = new IOException("DataNode async failure");

      dataNodeClient.onError(dataNodeException);

      Assert.assertSame(dataNodeException, dataNodeFailure.get());
      Assert.assertSame(endPoint, dataNodeTarget.get());
      verify(dataNodeClientManager).returnClient(endPoint, dataNodeClient);

      ClientManager<TEndPoint, AsyncConfigNodeInternalServiceClient> configNodeClientManager =
          mock(ClientManager.class);
      AtomicReference<Throwable> configNodeFailure = new AtomicReference<>();
      AtomicReference<TEndPoint> configNodeTarget = new AtomicReference<>();
      AsyncConfigNodeInternalServiceClient configNodeClient =
          new AsyncConfigNodeInternalServiceClient(
              new ThriftClientProperty.Builder().build(),
              endPoint,
              thriftClientManager,
              configNodeClientManager,
              (failure, target) -> {
                configNodeFailure.set(failure);
                configNodeTarget.set(target);
              });
      Exception configNodeException = new IOException("ConfigNode async failure");

      configNodeClient.onError(configNodeException);

      Assert.assertSame(configNodeException, configNodeFailure.get());
      Assert.assertSame(endPoint, configNodeTarget.get());
      verify(configNodeClientManager).returnClient(endPoint, configNodeClient);
    } finally {
      thriftClientManager.stop();
    }
  }

  public void clientFactoryConstructionFailureReporterTest() throws Exception {
    TEndPoint invalidEndpoint = new TEndPoint();
    invalidEndpoint.setPort(endPoint.getPort());
    ThriftClientProperty asyncProperty =
        new ThriftClientProperty.Builder().setSelectorNumOfAsyncClientManager(1).build();

    AtomicReference<Throwable> dataNodeFailure = new AtomicReference<>();
    AtomicReference<TEndPoint> dataNodeTarget = new AtomicReference<>();
    AsyncDataNodeInternalServiceClient.Factory dataNodeFactory =
        new AsyncDataNodeInternalServiceClient.Factory(
            mock(ClientManager.class),
            asyncProperty,
            "test-async-datanode-client",
            (failure, target) -> {
              dataNodeFailure.set(failure);
              dataNodeTarget.set(target);
            });
    try {
      Exception failure =
          Assert.assertThrows(Exception.class, () -> dataNodeFactory.makeObject(invalidEndpoint));
      Assert.assertSame(failure, dataNodeFailure.get());
      Assert.assertSame(invalidEndpoint, dataNodeTarget.get());
    } finally {
      dataNodeFactory.close();
    }

    AtomicReference<Throwable> configNodeFailure = new AtomicReference<>();
    AtomicReference<TEndPoint> configNodeTarget = new AtomicReference<>();
    AsyncConfigNodeInternalServiceClient.Factory configNodeFactory =
        new AsyncConfigNodeInternalServiceClient.Factory(
            mock(ClientManager.class),
            asyncProperty,
            "test-async-confignode-client",
            (failure, target) -> {
              configNodeFailure.set(failure);
              configNodeTarget.set(target);
            });
    try {
      Exception failure =
          Assert.assertThrows(Exception.class, () -> configNodeFactory.makeObject(invalidEndpoint));
      Assert.assertSame(failure, configNodeFailure.get());
      Assert.assertSame(invalidEndpoint, configNodeTarget.get());
    } finally {
      configNodeFactory.close();
    }

    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClientManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(new TestSyncDataNodeInternalServiceClientPoolFactory());
    AtomicReference<Throwable> syncFailure = new AtomicReference<>();
    AtomicReference<TEndPoint> syncTarget = new AtomicReference<>();
    try {
      SyncDataNodeInternalServiceClient.Factory syncFactory =
          new SyncDataNodeInternalServiceClient.Factory(
              syncClientManager,
              new ThriftClientProperty.Builder().build(),
              (failure, target) -> {
                syncFailure.set(failure);
                syncTarget.set(target);
              });
      Exception failure =
          Assert.assertThrows(Exception.class, () -> syncFactory.makeObject(invalidEndpoint));
      Assert.assertSame(failure, syncFailure.get());
      Assert.assertSame(invalidEndpoint, syncTarget.get());
    } finally {
      syncClientManager.close();
    }
  }

  public void syncFailureReporterTest() throws Exception {
    AtomicInteger failureCount = new AtomicInteger();
    AtomicReference<TEndPoint> failureTarget = new AtomicReference<>();
    ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClientManager =
        (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
                .createClientManager(
                    manager ->
                        new GenericKeyedObjectPool<>(
                            new SyncDataNodeInternalServiceClient.Factory(
                                manager,
                                new ThriftClientProperty.Builder()
                                    .setConnectionTimeoutMs(CONNECTION_TIMEOUT)
                                    .build(),
                                (failure, target) -> {
                                  failureCount.incrementAndGet();
                                  failureTarget.set(target);
                                }),
                            new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                                .build()
                                .getConfig()));

    SyncDataNodeInternalServiceClient client = null;
    try {
      client = syncClientManager.borrowClient(endPoint);
      client.invalidate();

      Assert.assertThrows(TException.class, client::merge);

      Assert.assertEquals(1, failureCount.get());
      Assert.assertSame(endPoint, failureTarget.get());
    } finally {
      if (client != null) {
        client.close();
      }
      syncClientManager.close();
    }
  }

  public void syncConfigNodeFailureReporterTest() throws Exception {
    AtomicInteger failureCount = new AtomicInteger();
    AtomicReference<Throwable> reportedFailure = new AtomicReference<>();
    AtomicReference<TEndPoint> failureTarget = new AtomicReference<>();
    RuntimeException reportingFailure = new RuntimeException("reporting failure");
    ClientManager<TEndPoint, SyncConfigNodeIServiceClient> syncClientManager =
        (ClientManager<TEndPoint, SyncConfigNodeIServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory(
                        (failure, target) -> {
                          failureCount.incrementAndGet();
                          reportedFailure.set(failure);
                          failureTarget.set(target);
                          throw reportingFailure;
                        }));

    SyncConfigNodeIServiceClient client = null;
    try {
      client = syncClientManager.borrowClient(endPoint);
      client.invalidate();

      TException failure = Assert.assertThrows(TException.class, client::testConnectionEmptyRPC);

      Assert.assertEquals(1, failureCount.get());
      Assert.assertSame(endPoint, failureTarget.get());
      Assert.assertNotSame(reportingFailure, failure);
      Assert.assertTrue(containsSuppressed(failure, reportingFailure));
    } finally {
      if (client != null) {
        client.close();
      }
      syncClientManager.close();
    }

    TEndPoint invalidEndpoint = new TEndPoint();
    invalidEndpoint.setPort(endPoint.getPort());
    failureCount.set(0);
    reportedFailure.set(null);
    failureTarget.set(null);
    ClientManager<TEndPoint, SyncConfigNodeIServiceClient> constructionClientManager =
        (ClientManager<TEndPoint, SyncConfigNodeIServiceClient>)
            new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory(
                        (failure, target) -> {
                          failureCount.incrementAndGet();
                          reportedFailure.set(failure);
                          failureTarget.set(target);
                          throw reportingFailure;
                        }));
    try {
      SyncConfigNodeIServiceClient.Factory factory =
          (SyncConfigNodeIServiceClient.Factory) constructionClientManager.getPool().getFactory();
      Exception failure =
          Assert.assertThrows(Exception.class, () -> factory.makeObject(invalidEndpoint));

      Assert.assertEquals(1, failureCount.get());
      Assert.assertSame(failure, reportedFailure.get());
      Assert.assertSame(invalidEndpoint, failureTarget.get());
      Assert.assertEquals(1, failure.getSuppressed().length);
      Assert.assertSame(reportingFailure, failure.getSuppressed()[0]);
    } finally {
      constructionClientManager.close();
    }
  }

  private static boolean containsSuppressed(Throwable failure, Throwable expectedSuppressed) {
    Throwable current = failure;
    while (current != null) {
      for (Throwable suppressed : current.getSuppressed()) {
        if (suppressed == expectedSuppressed) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  public void auditClientPoolShouldNotReuseHeartbeatFailureReporterTest() {
    AtomicInteger heartbeatFailureCount = new AtomicInteger();
    ClientPoolFactory.AsyncDataNodeHeartbeatServiceClientPoolFactory heartbeatPoolFactory =
        new ClientPoolFactory.AsyncDataNodeHeartbeatServiceClientPoolFactory(
            1, (failure, target) -> heartbeatFailureCount.incrementAndGet());
    ClientPoolFactory.AsyncDataNodeAuditServiceClientPoolFactory auditPoolFactory =
        new ClientPoolFactory.AsyncDataNodeAuditServiceClientPoolFactory(1);
    ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> heartbeatClientManager =
        (ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
                .createClientManager(heartbeatPoolFactory);
    ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> auditClientManager =
        (ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>)
            new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
                .createClientManager(auditPoolFactory);
    TEndPoint invalidEndpoint = new TEndPoint();
    invalidEndpoint.setPort(endPoint.getPort());

    try {
      Assert.assertThrows(
          ClientManagerException.class, () -> heartbeatClientManager.borrowClient(invalidEndpoint));
      int heartbeatReports = heartbeatFailureCount.get();
      Assert.assertTrue(heartbeatReports > 0);

      Assert.assertThrows(
          ClientManagerException.class, () -> auditClientManager.borrowClient(invalidEndpoint));

      Assert.assertEquals(heartbeatReports, heartbeatFailureCount.get());
      Assert.assertNotSame(heartbeatClientManager.getPool(), auditClientManager.getPool());
      Assert.assertNotEquals(
          heartbeatPoolFactory.getClass().getSimpleName(),
          auditPoolFactory.getClass().getSimpleName());
    } finally {
      heartbeatClientManager.close();
      auditClientManager.close();
    }
  }

  public void legacyFailureReporterFactoryConstructorsTest() {
    ThriftClientProperty asyncProperty =
        new ThriftClientProperty.Builder().setSelectorNumOfAsyncClientManager(1).build();
    AsyncDataNodeInternalServiceClient.Factory dataNodeFactory =
        new AsyncDataNodeInternalServiceClient.Factory(
            mock(ClientManager.class), asyncProperty, "legacy-async-datanode-client");
    AsyncConfigNodeInternalServiceClient.Factory configNodeFactory =
        new AsyncConfigNodeInternalServiceClient.Factory(
            mock(ClientManager.class), asyncProperty, "legacy-async-confignode-client");
    try {
      Assert.assertNotNull(
          new SyncDataNodeInternalServiceClient.Factory(
              mock(ClientManager.class), new ThriftClientProperty.Builder().build()));
      Assert.assertNotNull(
          new SyncConfigNodeIServiceClient.Factory(
              mock(ClientManager.class), new ThriftClientProperty.Builder().build()));
      Assert.assertNotNull(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    } finally {
      dataNodeFactory.close();
      configNodeFactory.close();
    }
  }

  public static class TestSyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeInternalServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(CONNECTION_TIMEOUT)
                  .build()),
          new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
              .setMaxClientNumForEachNode(
                  ClientPoolProperty.DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE)
              .setMaxIdleClientNumForEachNode(
                  ClientPoolProperty.DefaultProperty.MAX_IDLE_CLIENT_NUM_FOR_EACH_NODE)
              .build()
              .getConfig());
    }
  }

  public static class TestAsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(
              manager,
              new ThriftClientProperty.Builder().setConnectionTimeoutMs(CONNECTION_TIMEOUT).build(),
              ThreadName.ASYNC_DATANODE_CLIENT_POOL.getName()),
          new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>()
              .setMaxClientNumForEachNode(
                  ClientPoolProperty.DefaultProperty.MAX_CLIENT_NUM_FOR_EACH_NODE)
              .setMaxIdleClientNumForEachNode(
                  ClientPoolProperty.DefaultProperty.MAX_IDLE_CLIENT_NUM_FOR_EACH_NODE)
              .build()
              .getConfig());
    }
  }
}
