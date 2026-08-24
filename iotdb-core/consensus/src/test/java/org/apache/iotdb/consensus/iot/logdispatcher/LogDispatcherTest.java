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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.client.DispatchLogHandler;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LogDispatcherTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWaitForBatchAccumulationAfterFirstRequest() throws Exception {
    final Peer localPeer = createPeer(1, 6667);
    final Peer remotePeer = createPeer(2, 6668);
    final IoTConsensusConfig config = IoTConsensusConfig.newBuilder().build();
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    LogDispatcher.LogDispatcherThread dispatcherThread = null;
    Future<?> dispatcherFuture = null;
    try {
      final IoTConsensusServerImpl server =
          createServer(
              localPeer, Collections.singletonList(localPeer), config, backgroundTaskService);
      final Batch batch = createBatch(config, 1);
      final CountDownLatch accumulationWaitInvoked = new CountDownLatch(1);
      final AtomicInteger getBatchInvocations = new AtomicInteger();
      dispatcherThread =
          server.getLogDispatcher().new LogDispatcherThread(remotePeer, config, 0) {
            @Override
            public Batch getBatch() {
              return getBatchInvocations.getAndIncrement() == 0 ? new Batch(config) : batch;
            }

            @Override
            void waitForBatchAccumulation(long waitingTimeInMs) {
              accumulationWaitInvoked.countDown();
            }

            @Override
            public void sendBatchAsync(Batch sentBatch, DispatchLogHandler handler) {
              getSyncStatus().removeBatch(sentBatch);
              Thread.currentThread().interrupt();
            }
          };
      assertTrue(
          dispatcherThread.offer(
              new IndexedConsensusRequest(
                  1, Collections.singletonList(new TestEntry(1, localPeer)))));

      dispatcherFuture = executorService.submit(dispatcherThread);

      assertTrue(accumulationWaitInvoked.await(5, TimeUnit.SECONDS));
      dispatcherFuture.get(5, TimeUnit.SECONDS);
    } finally {
      if (dispatcherFuture != null) {
        dispatcherFuture.cancel(true);
      }
      executorService.shutdownNow();
      executorService.awaitTermination(5, TimeUnit.SECONDS);
      if (dispatcherThread != null) {
        dispatcherThread.stop();
      }
      backgroundTaskService.shutdownNow();
    }
  }

  @Test
  public void testReloadConfigUpdatesExistingDispatcherPipeline() throws Exception {
    final Peer localPeer = createPeer(1, 6677);
    final Peer remotePeer = createPeer(2, 6678);
    final IoTConsensusConfig initialConfig =
        IoTConsensusConfig.newBuilder()
            .setReplication(
                IoTConsensusConfig.Replication.newBuilder()
                    .setMaxLogEntriesNumPerBatch(1)
                    .setMaxPendingBatchesNum(1)
                    .build())
            .build();
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    LogDispatcher dispatcher = null;
    Future<?> secondBatchFuture = null;
    try {
      final IoTConsensusServerImpl server =
          createServer(
              localPeer,
              Arrays.asList(localPeer, remotePeer),
              initialConfig,
              backgroundTaskService);
      dispatcher = server.getLogDispatcher();
      final LogDispatcher.LogDispatcherThread dispatcherThread = getOnlyThread(dispatcher);
      dispatcher.start();

      final SyncStatus syncStatus = dispatcherThread.getSyncStatus();
      syncStatus.addNextBatch(createBatch(initialConfig, 1));
      final CountDownLatch secondBatchAttempted = new CountDownLatch(1);
      secondBatchFuture =
          executorService.submit(
              () -> {
                secondBatchAttempted.countDown();
                syncStatus.addNextBatch(createBatch(initialConfig, 2));
                return null;
              });
      assertTrue(secondBatchAttempted.await(5, TimeUnit.SECONDS));
      Thread.sleep(100);
      assertFalse(secondBatchFuture.isDone());

      final IoTConsensusConfig reloadedConfig =
          IoTConsensusConfig.newBuilder()
              .setReplication(
                  IoTConsensusConfig.Replication.newBuilder()
                      .setMaxLogEntriesNumPerBatch(2)
                      .setMaxPendingBatchesNum(2)
                      .build())
              .build();
      server.reloadConsensusConfig(reloadedConfig);

      secondBatchFuture.get(5, TimeUnit.SECONDS);
      assertSame(reloadedConfig, dispatcherThread.getConfig());
      assertEquals(2, syncStatus.getPendingBatches().size());
    } finally {
      if (secondBatchFuture != null) {
        secondBatchFuture.cancel(true);
      }
      executorService.shutdownNow();
      executorService.awaitTermination(5, TimeUnit.SECONDS);
      if (dispatcher != null) {
        dispatcher.stop();
      }
      backgroundTaskService.shutdownNow();
    }
  }

  private IoTConsensusServerImpl createServer(
      Peer localPeer,
      List<Peer> configuration,
      IoTConsensusConfig config,
      ScheduledExecutorService backgroundTaskService)
      throws Exception {
    return new IoTConsensusServerImpl(
        temporaryFolder.newFolder().getAbsolutePath(),
        localPeer,
        new TreeSet<>(configuration),
        new TestStateMachine(),
        backgroundTaskService,
        null,
        null,
        config);
  }

  private static Peer createPeer(int nodeId, int port) {
    return new Peer(new DataRegionId(1), nodeId, new TEndPoint("127.0.0.1", port));
  }

  private static Batch createBatch(IoTConsensusConfig config, long searchIndex) {
    final Batch batch = new Batch(config);
    batch.addTLogEntry(new TLogEntry().setSearchIndex(searchIndex).setMemorySize(1));
    batch.buildIndex();
    return batch;
  }

  @SuppressWarnings("unchecked")
  private static LogDispatcher.LogDispatcherThread getOnlyThread(LogDispatcher dispatcher)
      throws Exception {
    final Field threadsField = LogDispatcher.class.getDeclaredField("threads");
    threadsField.setAccessible(true);
    final List<LogDispatcher.LogDispatcherThread> threads =
        (List<LogDispatcher.LogDispatcherThread>) threadsField.get(dispatcher);
    assertEquals(1, threads.size());
    return threads.get(0);
  }
}
