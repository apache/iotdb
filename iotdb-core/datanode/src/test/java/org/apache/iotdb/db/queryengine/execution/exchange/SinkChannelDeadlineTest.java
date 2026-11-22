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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.sync.MPPDataExchangeClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SinkChannelDeadlineTest {
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final SinkListener listener = mock(SinkListener.class);
  private final LocalMemoryManager memory = mock(LocalMemoryManager.class);

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  private SinkChannel channel(
      TEndPoint endpoint, long deadline, MPPDataExchangeClientManager clients) {
    return channel(endpoint, deadline, clients, executor);
  }

  private SinkChannel channel(
      TEndPoint endpoint,
      long deadline,
      MPPDataExchangeClientManager clients,
      ExecutorService workers) {
    doReturn(Utils.createMockNonBlockedMemoryPool()).when(memory).getQueryPool();
    return new SinkChannel(
        endpoint,
        new TFragmentInstanceId("query", 0, "0"),
        "exchange",
        "sink",
        new TFragmentInstanceId("query", 1, "0"),
        memory,
        workers,
        Utils.createMockTsBlockSerde(1024),
        listener,
        false,
        deadline,
        clients);
  }

  private void awaitWorker() throws Exception {
    executor.submit(() -> {}).get(3, SECONDS);
  }

  @Test
  public void queuedEventsExpireWithoutBorrowingAndReleaseMemory() throws Exception {
    MPPDataExchangeClientManager clients = mock(MPPDataExchangeClientManager.class);
    CountDownLatch dequeue = new CountDownLatch(1);
    executor.submit(
        () -> {
          dequeue.await();
          return null;
        });
    long deadline = System.currentTimeMillis() + 200;
    SinkChannel channel = channel(new TEndPoint("unused", 1), deadline, clients);
    doAnswer(call -> channel.abort())
        .when(listener)
        .onFailure(eq(channel), isA(QueryTimeoutException.class));
    channel.open();
    channel.send(Utils.createMockTsBlocks(1, 1024).get(0));
    channel.setNoMoreTsBlocks();
    long remaining;
    while ((remaining = deadline - System.currentTimeMillis()) > 0) {
      MILLISECONDS.sleep(remaining);
    }
    dequeue.countDown();
    awaitWorker();
    verify(clients, never()).borrowClient(any(), anyLong());
    verify(listener).onFailure(eq(channel), isA(QueryTimeoutException.class));
    verify(listener, never()).onEndOfBlocks(channel);
    assertEquals(0, channel.getBufferRetainedSizeInBytes());
  }

  @Test
  public void cancelledFragmentSkipsQueuedEndEventBeforeChannelCleanup() throws Exception {
    MPPDataExchangeClientManager clients = mock(MPPDataExchangeClientManager.class);
    CountDownLatch dequeue = new CountDownLatch(1);
    executor.submit(
        () -> {
          dequeue.await();
          return null;
        });
    doReturn(Utils.createMockNonBlockedMemoryPool()).when(memory).getQueryPool();
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("query"), 1), "0");
    FragmentInstanceStateMachine state = new FragmentInstanceStateMachine(id, Runnable::run);
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContext(id, state);
    MPPDataExchangeManager exchange =
        new MPPDataExchangeManager(memory, new TsBlockSerdeFactory(), executor, clients);
    ISinkHandle sink =
        exchange.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint("unused", 1),
                    new TFragmentInstanceId("query", 0, "0"),
                    "exchange")),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            id.toThrift(),
            "sink",
            context);
    SinkChannel channel = (SinkChannel) sink.getChannel(0);
    channel.setNoMoreTsBlocks();
    state.cancel();
    dequeue.countDown();
    awaitWorker();
    verify(clients, never()).borrowClient(any(), anyLong());
    assertFalse(channel.isNoMoreTsBlocks());
    assertTrue(state.getFailureCauses().isEmpty());
    sink.abort();
  }

  @Test
  public void retryBackoffSharesTheDeadline() throws Exception {
    TEndPoint endpoint = new TEndPoint("unused", 1);
    MPPDataExchangeClientManager clients = mock(MPPDataExchangeClientManager.class);
    SyncDataNodeMPPDataExchangeServiceClient client =
        mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    when(clients.borrowClient(eq(endpoint), anyLong())).thenReturn(client);
    doThrow(new TException("transient failure")).when(client).onNewDataBlockEvent(any());
    SinkChannel channel = channel(endpoint, System.currentTimeMillis() + 800, clients);
    channel.setRetryIntervalInMs(30_000);
    channel.open();
    channel.send(Utils.createMockTsBlocks(1, 1024).get(0));
    awaitWorker();
    verify(client).onNewDataBlockEvent(any());
    verify(listener).onFailure(eq(channel), isA(QueryTimeoutException.class));
    channel.abort();
  }

  @Test
  public void abortInterruptsPoolWaitWithoutDiscardingTheHeldConnection() throws Exception {
    MPPDataExchangeClientManager clients = new MPPDataExchangeClientManager();
    clients.getPool().setMaxTotalPerKey(1);
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      TEndPoint endpoint =
          new TEndPoint(server.getInetAddress().getHostAddress(), server.getLocalPort());
      try (SyncDataNodeMPPDataExchangeServiceClient held = clients.borrowClient(endpoint);
          Socket peer = server.accept()) {
        SinkChannel channel = channel(endpoint, System.currentTimeMillis() + 20_000, clients);
        channel.setNoMoreTsBlocks();
        long start = System.nanoTime();
        while (clients.getPool().getNumWaiters() == 0
            && System.nanoTime() - start < SECONDS.toNanos(3)) {
          Thread.sleep(10);
        }
        assertEquals(1, clients.getPool().getNumWaiters());
        channel.abort();
        awaitWorker();
        verify(listener, never()).onFailure(any(), any());
        assertEquals(1, clients.getPool().getNumActive());
      }
      assertEquals(1, clients.getPool().getNumIdle());
    } finally {
      clients.close();
    }
  }

  @Test
  public void deadlineInterruptIsClearedBeforeListenerAndWorkerReturn() throws Exception {
    verifyWorkerInterrupt(false);
  }

  @Test
  public void executorShutdownInterruptSurvivesNotificationCleanup() throws Exception {
    verifyWorkerInterrupt(true);
  }

  private void verifyWorkerInterrupt(boolean shutdownNow) throws Exception {
    CompletableFuture<Boolean> afterTask = new CompletableFuture<>();
    CompletableFuture<Boolean> inListener = new CompletableFuture<>();
    ThreadPoolExecutor workers =
        new ThreadPoolExecutor(1, 1, 0, SECONDS, new LinkedBlockingQueue<>()) {
          @Override
          protected void afterExecute(Runnable task, Throwable error) {
            afterTask.complete(Thread.currentThread().isInterrupted());
          }
        };
    CountDownLatch sending = new CountDownLatch(1);
    try {
      TEndPoint endpoint = new TEndPoint("unused", 1);
      MPPDataExchangeClientManager clients = mock(MPPDataExchangeClientManager.class);
      SyncDataNodeMPPDataExchangeServiceClient client =
          mock(SyncDataNodeMPPDataExchangeServiceClient.class);
      when(clients.borrowClient(eq(endpoint), anyLong())).thenReturn(client);
      doAnswer(
              call -> {
                sending.countDown();
                while (!Thread.currentThread().isInterrupted()) {
                  LockSupport.park();
                }
                throw new TException(new ClosedByInterruptException());
              })
          .when(client)
          .onEndOfDataBlockEvent(any());
      doAnswer(call -> inListener.complete(Thread.currentThread().isInterrupted()))
          .when(listener)
          .onFailure(any(), any());
      long deadline = shutdownNow ? Long.MAX_VALUE : System.currentTimeMillis() + 1_000;
      SinkChannel channel = channel(endpoint, deadline, clients, workers);
      channel.setNoMoreTsBlocks();
      assertTrue(sending.await(5, SECONDS));
      if (shutdownNow) {
        workers.shutdownNow();
      } else {
        // Graceful shutdown lets the running task reach its own deadline.
        workers.shutdown();
      }
      assertEquals(shutdownNow, inListener.get(5, SECONDS));
      assertEquals(shutdownNow, afterTask.get(5, SECONDS));
      if (!shutdownNow) {
        verify(listener).onFailure(eq(channel), isA(QueryTimeoutException.class));
      }
      verify(listener, never()).onEndOfBlocks(channel);
    } finally {
      workers.shutdownNow();
    }
  }

  @Test
  public void failureWinsAgainstLateEndAndNormalClose() throws Exception {
    verifyTerminalCallbacks(false);
  }

  @Test
  public void acknowledgedEndWinsAgainstLateNotificationFailure() throws Exception {
    verifyTerminalCallbacks(true);
  }

  private void verifyTerminalCallbacks(boolean finishFirst) throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    CountDownLatch bothSending = new CountDownLatch(2);
    CountDownLatch releaseData = new CountDownLatch(1);
    CountDownLatch releaseEnd = new CountDownLatch(1);
    CountDownLatch normalEnd = new CountDownLatch(1);
    CountDownLatch failure = new CountDownLatch(1);
    try {
      TEndPoint endpoint = new TEndPoint("unused", 1);
      MPPDataExchangeClientManager clients = mock(MPPDataExchangeClientManager.class);
      SyncDataNodeMPPDataExchangeServiceClient client =
          mock(SyncDataNodeMPPDataExchangeServiceClient.class);
      when(clients.borrowClient(eq(endpoint), anyLong())).thenReturn(client);
      doAnswer(
              call -> {
                bothSending.countDown();
                Uninterruptibles.awaitUninterruptibly(releaseData);
                throw new TApplicationException("injected data notification failure");
              })
          .when(client)
          .onNewDataBlockEvent(any());
      doAnswer(
              call -> {
                bothSending.countDown();
                Uninterruptibles.awaitUninterruptibly(releaseEnd);
                return null;
              })
          .when(client)
          .onEndOfDataBlockEvent(any());
      doAnswer(
              call -> {
                normalEnd.countDown();
                return null;
              })
          .when(listener)
          .onEndOfBlocks(any());
      doAnswer(
              call -> {
                failure.countDown();
                return null;
              })
          .when(listener)
          .onFailure(any(), any());
      SinkChannel channel = channel(endpoint, Long.MAX_VALUE, clients, workers);
      channel.open();
      channel.send(Utils.createMockTsBlocks(1, 1024).get(0));
      channel.setNoMoreTsBlocks();
      assertTrue(bothSending.await(5, SECONDS));
      channel.acknowledgeTsBlock(0, 1);
      if (finishFirst) {
        releaseEnd.countDown();
        assertTrue(normalEnd.await(5, SECONDS));
        releaseData.countDown();
      } else {
        releaseData.countDown();
        assertTrue(failure.await(5, SECONDS));
        channel.close();
        releaseEnd.countDown();
      }
      workers.shutdown();
      assertTrue(workers.awaitTermination(5, SECONDS));
      if (finishFirst) {
        verify(listener).onFinish(channel);
        verify(listener, never()).onFailure(any(), any());
      } else {
        verify(listener, never()).onFinish(channel);
        verify(listener, never()).onEndOfBlocks(channel);
      }
    } finally {
      releaseData.countDown();
      releaseEnd.countDown();
      workers.shutdownNow();
    }
  }

  @Test
  public void silentPeerReleasesWorkerAtDeadline() throws Exception {
    verifyBlockedRead(false);
  }

  @Test
  public void abortInterruptsReadAndLeavesOtherConnectionUsable() throws Exception {
    verifyBlockedRead(true);
  }

  private void verifyBlockedRead(boolean abort) throws Exception {
    MPPDataExchangeClientManager clients = new MPPDataExchangeClientManager();
    try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(3_000);
      TEndPoint endpoint =
          new TEndPoint(server.getInetAddress().getHostAddress(), server.getLocalPort());
      // Warm up the connection and keep another idle connection to the same endpoint.
      try (SyncDataNodeMPPDataExchangeServiceClient first = clients.borrowClient(endpoint);
          SyncDataNodeMPPDataExchangeServiceClient second = clients.borrowClient(endpoint)) {}
      long borrowed = clients.getPool().getBorrowedCount();
      // The pool is LIFO, and the first client was returned last.
      try (Socket peer = server.accept();
          Socket other = server.accept()) {
        peer.setSoTimeout(3_000);
        long deadline = System.currentTimeMillis() + (abort ? 20_000 : 800);
        SinkChannel channel = channel(endpoint, deadline, clients);
        channel.setNoMoreTsBlocks();
        assertTrue(peer.getInputStream().read() >= 0);
        if (abort) {
          channel.abort();
        }
        awaitWorker();
        assertEquals(borrowed + 1, clients.getPool().getBorrowedCount());
        assertEquals(0, clients.getPool().getNumActive());
        assertEquals(1, clients.getPool().getNumIdle());
        verify(listener, never()).onEndOfBlocks(channel);
        if (abort) {
          verify(listener, never()).onFailure(any(), any());
        } else {
          verify(listener).onFailure(eq(channel), isA(QueryTimeoutException.class));
        }
      }
    } finally {
      clients.close();
    }
  }
}
