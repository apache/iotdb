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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.listener.AbstractResultListener.Status;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutor;
import org.apache.iotdb.db.subscription.task.execution.ConsensusSubscriptionPrefetchExecutorManager;
import org.apache.iotdb.db.subscription.task.subtask.ConsensusPrefetchSubtask;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueReplicationTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();
  private final ConsensusSubscriptionPrefetchExecutorManager manager =
      ConsensusSubscriptionPrefetchExecutorManager.getInstance();
  private final AtomicReference<ConsensusPrefetchSubtask> subtask = new AtomicReference<>();
  private final AtomicInteger enqueueCount = new AtomicInteger();
  private final AtomicBoolean blockNextEmptyScan = new AtomicBoolean();
  private final CountDownLatch emptyScanChecked = new CountDownLatch(1);
  private final CountDownLatch releaseEmptyScan = new CountDownLatch(1);
  private volatile boolean hideEntriesForCurrentRound;
  private int previousBatchDelay;
  private int previousPrefetchThreads;
  private long previousWatermarkInterval;
  private String previousSystemDir;
  private String previousConsensus;
  private WALMode previousWalMode;
  private WALNode walNode;
  private ConsensusPrefetchingQueue queue;

  @Before
  public void setUp() throws Exception {
    previousBatchDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    previousPrefetchThreads = config.getSubscriptionConsensusPrefetchExecutorMaxThreadNum();
    previousWatermarkInterval = config.getSubscriptionConsensusWatermarkIntervalMs();
    previousSystemDir = dbConfig.getSystemDir();
    previousConsensus = dbConfig.getDataRegionConsensusProtocolClass();
    previousWalMode = dbConfig.getWalMode();
    config.setSubscriptionConsensusBatchMaxDelayInMs(0);
    config.setSubscriptionConsensusPrefetchExecutorMaxThreadNum(1);
    config.setSubscriptionConsensusWatermarkIntervalMs(0L);
    dbConfig.setSystemDir(temporaryFolder.newFolder("system").getAbsolutePath());
    dbConfig.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    dbConfig.setWalMode(WALMode.SYNC);
    final File walDirectory = temporaryFolder.newFolder("wal");
    walNode = new WALNode("subscription-replication", walDirectory.getAbsolutePath());

    assertFalse(manager.isStarted());
    final ConsensusSubscriptionPrefetchExecutor executor =
        new ConsensusSubscriptionPrefetchExecutor() {
          @Override
          public synchronized boolean register(final ConsensusPrefetchSubtask task) {
            if (!super.register(task)) {
              return false;
            }
            subtask.set(task);
            return true;
          }

          @Override
          public void enqueue(final ConsensusPrefetchSubtask task) {
            enqueueCount.incrementAndGet();
            super.enqueue(task);
          }
        };
    final Field executorField =
        ConsensusSubscriptionPrefetchExecutorManager.class.getDeclaredField("executor");
    executorField.setAccessible(true);
    executorField.set(manager, executor);
    manager.start();
    // These subscription tests run with the subscription source switch enabled.
    assertTrue(manager.isStarted());

    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getConsensusReqReader()).thenReturn(walNode);
    when(server.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
    final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
    when(converter.getDatabaseName()).thenReturn("db");
    when(converter.isTableModel()).thenReturn(true);
    when(converter.convert(any()))
        .thenAnswer(
            invocation ->
                Collections.singletonList(
                    createTablet(((InsertRowNode) invocation.getArgument(0)).getTime())));
    queue =
        new ConsensusPrefetchingQueue(
            "consumerGroup",
            "topic",
            TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
            new DataRegionId(1),
            server,
            new SubscriptionWalRetentionPolicy("topic", -1L, -1L),
            converter,
            new ConsensusSubscriptionCommitManager(
                (consumerGroupId, topicName, regionId) ->
                    ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent()),
            new RegionProgress(Collections.emptyMap()),
            1L,
            1L,
            true) {
          @Override
          public PrefetchRoundResult drivePrefetchOnce() {
            hideEntriesForCurrentRound = false;
            return super.drivePrefetchOnce();
          }

          @Override
          protected ProgressWALIterator createSubscriptionWALIterator(final long startSearchIndex) {
            return new ProgressWALIterator(walNode, startSearchIndex) {
              @Override
              boolean hasNext(final ScanBudget scanBudget) {
                if (hideEntriesForCurrentRound) {
                  return false;
                }
                final boolean available = super.hasNext(scanBudget);
                if (!available && blockNextEmptyScan.compareAndSet(true, false)) {
                  // Keep the exhausted scan's result for this round while replication completes.
                  hideEntriesForCurrentRound = true;
                  emptyScanChecked.countDown();
                  try {
                    assertTrue(releaseEmptyScan.await(5L, TimeUnit.SECONDS));
                  } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                }
                return available;
              }
            };
          }
        };
    queue.setSubscriptionMemoryManager(new SubscriptionMemoryManager(16L * 1024 * 1024));
    queue.seekToBeginning();
    awaitIdle();
  }

  @After
  public void tearDown() {
    releaseEmptyScan.countDown();
    try {
      if (queue != null) {
        queue.close();
      }
      manager.stop();
      if (walNode != null) {
        walNode.close();
      }
    } finally {
      config.setSubscriptionConsensusBatchMaxDelayInMs(previousBatchDelay);
      config.setSubscriptionConsensusPrefetchExecutorMaxThreadNum(previousPrefetchThreads);
      config.setSubscriptionConsensusWatermarkIntervalMs(previousWatermarkInterval);
      dbConfig.setSystemDir(previousSystemDir);
      dbConfig.setDataRegionConsensusProtocolClass(previousConsensus);
      dbConfig.setWalMode(previousWalMode);
    }
  }

  @Test(timeout = 20_000L)
  public void testEmptyPollOnlyWakesForNewReplicatedWal() {
    assertEmptyPollDoesNotEnqueue();
    replicate(10L);
    assertEquals(0L, queue.getRawWalGap());
    pollAndAck(10L);
    awaitIdle();
    assertEmptyPollDoesNotEnqueue();
    // A later replicated batch must remain detectable after the first version has been consumed.
    replicate(20L);
    pollAndAck(20L);
    assertEquals(1L, queue.getCurrentReadSearchIndex());
    assertEquals(0L, queue.getWalGapSkippedEntries());
  }

  @Test(timeout = 20_000L)
  public void testReplicationPublishedDuringEmptyScanIsNotLost() throws Exception {
    final ExecutorService scanExecutor = Executors.newSingleThreadExecutor();
    try {
      blockNextEmptyScan.set(true);
      final Future<PrefetchRoundResult> scan = scanExecutor.submit(queue::drivePrefetchOnce);
      assertTrue(emptyScanChecked.await(5L, TimeUnit.SECONDS));
      replicate(10L);
      releaseEmptyScan.countDown();
      assertEquals(PrefetchRoundResult.Type.DORMANT, scan.get(5L, TimeUnit.SECONDS).getType());
      pollAndAck(10L);
    } finally {
      releaseEmptyScan.countDown();
      scanExecutor.shutdownNow();
      assertTrue(scanExecutor.awaitTermination(5L, TimeUnit.SECONDS));
    }
  }

  private void assertEmptyPollDoesNotEnqueue() {
    final int previousEnqueueCount = enqueueCount.get();
    for (int i = 0; i < 3; i++) {
      assertNull(queue.poll("consumer"));
    }
    assertEquals(previousEnqueueCount, enqueueCount.get());
  }

  private void replicate(final long localSeq) {
    final InsertRowNode node = StatementTestUtils.genInsertRowNode(Math.toIntExact(localSeq));
    node.setSearchIndex(-1L);
    node.setSyncIndex(localSeq);
    node.setPhysicalTime(1000L + localSeq);
    node.setNodeId(8);
    assertEquals(Status.SUCCESS, walNode.log(1L, node).waitForResult());
  }

  private void pollAndAck(final long timestamp) {
    final AtomicReference<SubscriptionEvent> received = new AtomicReference<>();
    await()
        .atMost(5L, TimeUnit.SECONDS)
        .until(
            () -> {
              received.set(queue.poll("consumer"));
              return received.get() != null;
            });
    final SubscriptionEvent event = received.get();
    assertEquals(
        SubscriptionPollResponseType.TABLETS.getType(),
        event.getCurrentResponse().getResponseType());
    final List<Tablet> tablets =
        ((TabletsPayload) event.getCurrentResponse().getPayload()).getTablets();
    assertEquals(1, tablets.size());
    assertEquals(1, tablets.get(0).getRowSize());
    assertEquals(timestamp, tablets.get(0).getTimestamps()[0]);
    assertTrue(queue.ack("consumer", event.getCommitContext()));
  }

  private void awaitIdle() {
    await()
        .atMost(5L, TimeUnit.SECONDS)
        .until(() -> subtask.get() != null && !subtask.get().isScheduledOrRunning());
  }

  private static Tablet createTablet(final long timestamp) {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "temperature"),
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, 36.5);
    tablet.setRowSize(1);
    return tablet;
  }
}
