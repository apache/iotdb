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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.ProgressWALReader;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testInitializationAndActivationUseIndependentMonitors() throws Exception {
    assertFalse(
        Modifier.isSynchronized(
            ConsensusPrefetchingQueue.class
                .getDeclaredMethod("initPrefetch", RegionProgress.class)
                .getModifiers()));
    assertFalse(
        Modifier.isSynchronized(
            ConsensusPrefetchingQueue.class
                .getDeclaredMethod("setActive", boolean.class)
                .getModifiers()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAdmissionClearCannotLeaveEntryEnqueuedAfterFence() throws Exception {
    final Class<?> queueClass =
        Class.forName(ConsensusPrefetchingQueue.class.getName() + "$WakeableIndexedConsensusQueue");
    final Constructor<?> constructor =
        queueClass.getDeclaredConstructor(int.class, Runnable.class, BooleanSupplier.class);
    constructor.setAccessible(true);

    final AtomicBoolean admissionEnabled = new AtomicBoolean(true);
    final CountDownLatch admissionChecked = new CountDownLatch(1);
    final CountDownLatch finishAdmissionCheck = new CountDownLatch(1);
    final CountDownLatch clearInvoked = new CountDownLatch(1);
    final CountDownLatch clearCompleted = new CountDownLatch(1);
    final AtomicReference<Boolean> offered = new AtomicReference<>();
    final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
    final BooleanSupplier admissionSupplier =
        () -> {
          final boolean admitted = admissionEnabled.get();
          admissionChecked.countDown();
          try {
            if (!finishAdmissionCheck.await(5, TimeUnit.SECONDS)) {
              throw new AssertionError();
            }
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
          return admitted;
        };
    final BlockingQueue<IndexedConsensusRequest> queue =
        (BlockingQueue<IndexedConsensusRequest>)
            constructor.newInstance(8, (Runnable) () -> {}, admissionSupplier);

    final Thread offerThread =
        new Thread(
            () -> {
              try {
                offered.set(queue.offer(createRequest(1L)));
              } catch (final Throwable t) {
                asyncFailure.compareAndSet(null, t);
              }
            });
    final Thread clearThread =
        new Thread(
            () -> {
              try {
                clearInvoked.countDown();
                queue.clear();
              } catch (final Throwable t) {
                asyncFailure.compareAndSet(null, t);
              } finally {
                clearCompleted.countDown();
              }
            });
    offerThread.setDaemon(true);
    clearThread.setDaemon(true);

    offerThread.start();
    try {
      assertTrue(admissionChecked.await(5, TimeUnit.SECONDS));
      admissionEnabled.set(false);
      clearThread.start();
      assertTrue(clearInvoked.await(5, TimeUnit.SECONDS));

      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (clearCompleted.getCount() > 0
          && clearThread.getState() != Thread.State.BLOCKED
          && System.nanoTime() < deadlineNanos) {
        Thread.yield();
      }
      assertTrue(clearCompleted.getCount() == 0 || clearThread.getState() == Thread.State.BLOCKED);
    } finally {
      finishAdmissionCheck.countDown();
      offerThread.join(TimeUnit.SECONDS.toMillis(5));
      clearThread.join(TimeUnit.SECONDS.toMillis(5));
    }

    assertFalse(offerThread.isAlive());
    assertFalse(clearThread.isAlive());
    if (asyncFailure.get() != null) {
      throw new AssertionError(asyncFailure.get());
    }
    assertTrue(Boolean.TRUE.equals(offered.get()));
    assertTrue(queue.isEmpty());
  }

  @Test
  public void testLagIncludesLingeringBatchUntilCommitted() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalBatchMaxDelay =
        CommonDescriptor.getInstance().getConfig().getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("lagWithLingeringBatch");
    ConsensusPrefetchingQueue queue = null;
    try {
      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(60_000);
      final DataRegionId regionId = new DataRegionId(9);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      reader.currentSearchIndex = 1L;
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any())).thenReturn(Collections.singletonList(createTablet()));
      when(converter.getDatabaseName()).thenReturn("db");
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final IndexedConsensusRequest request =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(1000L)
              .setNodeId(7);

      assertNull(queue.poll("consumer"));
      pendingEntries(queue).offer(request);
      queue.drivePrefetchOnce();

      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(1L, queue.getLag());

      CommonDescriptor.getInstance().getConfig().setSubscriptionConsensusBatchMaxDelayInMs(0);
      queue.drivePrefetchOnce();
      assertEquals(1, queue.getPrefetchedEventCount());
      assertEquals(1L, queue.getLag());

      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertEquals(1L, queue.getLag());
      assertTrue(queue.ack("consumer", event.getCommitContext()));
      assertEquals(0L, queue.getLag());
    } finally {
      if (queue != null) {
        queue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testFilteredEmptyEntryAdvancesProgressWithoutEvent() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any())).thenReturn(Collections.emptyList());

      final ConsensusSubscriptionCommitManager commitManager = newCommitManager(systemDir);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              commitManager,
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final IndexedConsensusRequest request =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(1000L)
              .setNodeId(7);
      reader.currentSearchIndex = 1L;
      pendingEntries(queue).offer(request);

      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();

      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertEquals(
          new WriterProgress(1000L, 1L),
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .get(new WriterId(regionId.toString(), 7)));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testFilteredEmptyEntryDoesNotAdvanceAcrossUncommittedData() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system-with-data-before-empty");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenReturn(Collections.singletonList(createTablet()), Collections.emptyList());
      when(converter.getDatabaseName()).thenReturn("db");

      final ConsensusSubscriptionCommitManager commitManager = newCommitManager(systemDir);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              commitManager,
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final IndexedConsensusRequest dataRequest =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(1000L)
              .setNodeId(7);
      final IndexedConsensusRequest requestWithEmptyConversionResult =
          new IndexedConsensusRequest(
                  2L, Collections.singletonList(StatementTestUtils.genInsertRowNode(2)))
              .setPhysicalTime(1001L)
              .setNodeId(7);
      reader.currentSearchIndex = 2L;

      assertNull(queue.poll("consumer"));
      pendingEntries(queue).offer(dataRequest);
      pendingEntries(queue).offer(requestWithEmptyConversionResult);
      queue.drivePrefetchOnce();

      final WriterId writerId = new WriterId(regionId.toString(), 7);
      assertEquals(1, queue.getPrefetchedEventCount());
      assertEquals(3L, queue.getCurrentReadSearchIndex());
      assertFalse(
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .containsKey(writerId));

      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertTrue(queue.ack("consumer", event.getCommitContext()));
      assertEquals(
          new WriterProgress(1001L, 2L),
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .get(writerId));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testPendingInitialProposalKeepsFirstPollDormantUntilExplicitProgress()
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("pendingInitialQueue");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(2);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      reader.currentSearchIndex = 49L;
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      final ConsensusSubscriptionCommitManager commitManager =
          newCommitManager(systemDir, (consumerGroupId, topicName, ignored) -> queryResult.get());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              commitManager,
              null,
              50L,
              1L,
              true);
      commitManager.initializeStateFromTailProposal(
          "consumerGroup", "topic", regionId, new RegionProgress(Collections.emptyMap()));

      assertNull(queue.poll("consumer"));
      assertEquals(50L, queue.getCurrentReadSearchIndex());

      queryResult.set(
          ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
              new RegionProgress(Collections.emptyMap())));
      assertNull(queue.poll("consumer"));
      assertEquals(1L, queue.getCurrentReadSearchIndex());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testAtEndReplayLookupPreservesRequestedWriterFrontier() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("atEndWriterFrontier");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(3);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      reader.currentSearchIndex = 5L;
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(
                  systemDir,
                  (consumerGroupId, topicName, ignored) ->
                      ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult
                          .unavailable()),
              null,
              6L,
              1L,
              true);
      final RegionProgress requestedProgress =
          new RegionProgress(
              Collections.singletonMap(
                  new WriterId(regionId.toString(), 9), new WriterProgress(1000L, 100L)));

      final ConsensusPrefetchingQueue.ReplayLocateDecision decision =
          queue.scanReplayStartForRequests(Collections.emptyIterator(), requestedProgress, true);

      assertEquals(ConsensusPrefetchingQueue.ReplayLocateStatus.AT_END, decision.getStatus());
      assertEquals(requestedProgress, decision.getRecoveryRegionProgress());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testAtEndInitializationPreservesPendingQueueRegistrationBoundary() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("atEndPendingBoundary");
    final File walDirectory = temporaryFolder.newFolder("atEndPendingWal");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(7);
      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn(4L);
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any())).thenReturn(Collections.emptyList());
      final RegionProgress committedProgress =
          new RegionProgress(
              Collections.singletonMap(
                  new WriterId(regionId.toString(), 7), new WriterProgress(1000L, 2L)));
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              committedProgress,
              3L,
              1L,
              true) {
            @Override
            protected ReplayLocateDecision locateReplayStartForRegionProgress(
                final RegionProgress regionProgress, final boolean seekAfter) {
              return ReplayLocateDecision.atEnd(
                  walNode.getCurrentSearchIndex(), regionProgress, "empty sealed WAL");
            }
          };

      pendingEntries(queue)
          .offer(
              new IndexedConsensusRequest(
                      3L, Collections.singletonList(StatementTestUtils.genInsertRowNode(3)))
                  .setPhysicalTime(1000L)
                  .setNodeId(7));
      pendingEntries(queue)
          .offer(
              new IndexedConsensusRequest(
                      4L, Collections.singletonList(StatementTestUtils.genInsertRowNode(4)))
                  .setPhysicalTime(1000L)
                  .setNodeId(7));

      assertNull(queue.poll("consumer"));
      assertEquals(3L, queue.getCurrentReadSearchIndex());

      queue.drivePrefetchOnce();
      assertEquals(2L, queue.getPendingPathAcceptedEntries());
      assertEquals(5L, queue.getCurrentReadSearchIndex());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testZeroPhysicalTimeUsesWriterLocalSequenceForReplayLookup() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("zeroPhysicalTimeReplay");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(4);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      reader.currentSearchIndex = 2L;
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              null,
              1L,
              1L,
              true);

      final IndexedConsensusRequest committedRequest =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(0L)
              .setNodeId(7);
      final IndexedConsensusRequest nextRequest =
          new IndexedConsensusRequest(
                  2L, Collections.singletonList(StatementTestUtils.genInsertRowNode(2)))
              .setPhysicalTime(0L)
              .setNodeId(7);
      final RegionProgress committedProgress =
          new RegionProgress(
              Collections.singletonMap(
                  new WriterId(regionId.toString(), 7), new WriterProgress(0L, 1L)));

      final ConsensusPrefetchingQueue.ReplayLocateDecision decision =
          queue.scanReplayStartForRequests(
              Arrays.asList(committedRequest, nextRequest).iterator(), committedProgress, true);

      assertEquals(ConsensusPrefetchingQueue.ReplayLocateStatus.FOUND, decision.getStatus());
      assertEquals(2L, decision.getStartSearchIndex());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testTailProgressFlushesPendingWalMetadataBeforeSnapshot() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("tailProgressAfterWalRoll");
    final File walDirectory = temporaryFolder.newFolder("tailProgressWal");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(6);
      final WALNode walNode = mock(WALNode.class);
      final AtomicBoolean rolled = new AtomicBoolean(false);
      final WALMetaData flushedMetadata = new WALMetaData();
      flushedMetadata.add(Integer.BYTES, 1L, 0L, 0L, 7, 1L);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentWALFileVersion()).thenReturn(0L);
      when(walNode.getCurrentWALMetaDataSnapshot())
          .thenAnswer(ignored -> rolled.get() ? flushedMetadata.copy() : new WALMetaData());
      doAnswer(
              ignored -> {
                rolled.set(true);
                return null;
              })
          .when(walNode)
          .rollWALFile();

      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              null,
              2L,
              1L,
              false);

      final RegionProgress tailProgress = queue.computeTailRegionProgress();

      verify(walNode).rollWALFile();
      assertEquals(
          new WriterProgress(0L, 1L),
          tailProgress.getWriterPositions().get(new WriterId(regionId.toString(), 7)));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testTailProgressSkipsHeaderOnlyWalFile() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("tailProgressHeaderOnly");
    final File walDirectory = temporaryFolder.newFolder("tailProgressHeaderOnlyWal");
    final File headerOnlyWal =
        new File(
            walDirectory, WALFileUtils.getLogFileName(0L, 0L, WALFileStatus.CONTAINS_SEARCH_INDEX));
    ConsensusPrefetchingQueue queue = null;
    try {
      try (WALWriter ignored = new WALWriter(headerOnlyWal, WALFileVersion.V3)) {
        // The WAL version header is intentionally the only content.
      }
      assertTrue(ProgressWALIterator.isHeaderOnlyWalFile(headerOnlyWal));

      final DataRegionId regionId = new DataRegionId(8);
      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(new WALMetaData());
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              null,
              1L,
              1L,
              false) {
            @Override
            ProgressWALReader openProgressWALReader(final File walFile) throws IOException {
              throw new AssertionError("header-only WAL must not be parsed");
            }
          };

      assertTrue(queue.computeTailRegionProgress().getWriterPositions().isEmpty());
      verify(walNode).rollWALFile();
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testActivationRetriesUntilConfigNodeProgressIsExplicitlyAvailable() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("activationWithoutAuthority");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(4);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final AtomicReference<ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult>
          queryResult =
              new AtomicReference<>(
                  ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(
                  systemDir, (consumerGroupId, topicName, ignored) -> queryResult.get()),
              null,
              1L,
              1L,
              false);

      queue.applyRuntimeState(
          new ConsensusRegionRuntimeState(2L, 7, true, Collections.singleton(7)));
      assertFalse(queue.isActive());
      assertEquals(1L, queue.getEpochChangeCount());
      assertEquals(Collections.singleton(7), queue.getActiveWriterNodeIds());
      assertEquals("7", queue.coreReportMessage().get("preferredWriterNodeId"));

      queryResult.set(
          ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.available(
              new RegionProgress(Collections.emptyMap())));
      assertNull(queue.poll("consumer"));
      assertTrue(queue.isActive());
      assertEquals(1L, queue.getCurrentReadSearchIndex());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testActivationInstallsRuntimeStateBeforeRefreshingAuthoritativeProgress()
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("activationStateOrdering");
    final AtomicReference<ConsensusPrefetchingQueue> queueReference = new AtomicReference<>();
    final AtomicBoolean progressRequested = new AtomicBoolean(false);
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(5);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(
                  systemDir,
                  (consumerGroupId, topicName, ignored) -> {
                    final ConsensusPrefetchingQueue activatingQueue = queueReference.get();
                    assertNotNull(activatingQueue);
                    assertFalse(activatingQueue.isActive());
                    assertEquals(1L, activatingQueue.getEpochChangeCount());
                    assertEquals(
                        Collections.singleton(7), activatingQueue.getActiveWriterNodeIds());
                    assertEquals(
                        "7", activatingQueue.coreReportMessage().get("preferredWriterNodeId"));
                    progressRequested.set(true);
                    return ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult
                        .available(new RegionProgress(Collections.emptyMap()));
                  }),
              null,
              1L,
              1L,
              false);
      queueReference.set(queue);

      queue.applyRuntimeState(
          new ConsensusRegionRuntimeState(2L, 7, true, Collections.singleton(7)));

      assertTrue(progressRequested.get());
      assertTrue(queue.isActive());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testTabletMemoryReleasedAfterAck() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system-memory-release");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenReturn(Collections.singletonList(createTablet()), Collections.emptyList());
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final IndexedConsensusRequest dataRequest =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(1000L)
              .setNodeId(7);
      final IndexedConsensusRequest requestWithEmptyConversionResult =
          new IndexedConsensusRequest(
                  2L, Collections.singletonList(StatementTestUtils.genInsertRowNode(2)))
              .setPhysicalTime(1001L)
              .setNodeId(7);
      reader.currentSearchIndex = 2L;
      pendingEntries(queue).offer(dataRequest);
      pendingEntries(queue).offer(requestWithEmptyConversionResult);

      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();

      final long retainedBytes = queue.getRetainedTabletBytes();
      assertTrue(retainedBytes > 0L);
      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertEquals(retainedBytes, queue.getRetainedTabletBytes());

      assertTrue(queue.ack("consumer", event.getCommitContext()));
      assertEquals(0L, queue.getRetainedTabletBytes());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testLateAckDoesNotWaitForPrefetchReadLock() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("late-ack-read-lock");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenReturn(Collections.singletonList(createTablet()), Collections.emptyList());
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final IndexedConsensusRequest dataRequest =
          new IndexedConsensusRequest(
                  1L, Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
              .setPhysicalTime(1000L)
              .setNodeId(7);
      final IndexedConsensusRequest requestWithEmptyConversionResult =
          new IndexedConsensusRequest(
                  2L, Collections.singletonList(StatementTestUtils.genInsertRowNode(2)))
              .setPhysicalTime(1001L)
              .setNodeId(7);
      reader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(queue).offer(dataRequest));
      assertTrue(pendingEntries(queue).offer(requestWithEmptyConversionResult));
      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();

      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertTrue(queue.ackSilent("consumer", event.getCommitContext()));

      final ConsensusPrefetchingQueue activeQueue = queue;
      final ReentrantReadWriteLock queueLock = queueLock(queue);
      final CountDownLatch ackStarted = new CountDownLatch(1);
      final CountDownLatch ackCompleted = new CountDownLatch(1);
      final AtomicReference<Boolean> lateAckResult = new AtomicReference<>();
      final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
      final Thread lateAckThread =
          new Thread(
              () -> {
                ackStarted.countDown();
                try {
                  lateAckResult.set(activeQueue.ackSilent("consumer", event.getCommitContext()));
                } catch (final Throwable t) {
                  asyncFailure.set(t);
                } finally {
                  ackCompleted.countDown();
                }
              });
      lateAckThread.setDaemon(true);

      final boolean completedWhileReadLocked;
      queueLock.readLock().lock();
      try {
        lateAckThread.start();
        assertTrue(ackStarted.await(5, TimeUnit.SECONDS));
        completedWhileReadLocked = ackCompleted.await(5, TimeUnit.SECONDS);
      } finally {
        queueLock.readLock().unlock();
      }
      lateAckThread.join(TimeUnit.SECONDS.toMillis(5));

      assertTrue(completedWhileReadLocked);
      assertFalse(lateAckThread.isAlive());
      if (asyncFailure.get() != null) {
        throw new AssertionError(asyncFailure.get());
      }
      assertTrue(Boolean.TRUE.equals(lateAckResult.get()));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testWalCatchUpReusesIteratorAcrossRounds() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("reuse-wal-iterator");
    ConsensusPrefetchingQueue queue = null;
    try {
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              new DataRegionId(1),
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final ProgressWALIterator iterator = mock(ProgressWALIterator.class);
      when(iterator.hasNext()).thenReturn(false);
      setSubscriptionWalIterator(queue, iterator);

      final Method tryCatchUpFromWAL =
          ConsensusPrefetchingQueue.class.getDeclaredMethod("tryCatchUpFromWAL", long.class);
      tryCatchUpFromWAL.setAccessible(true);
      tryCatchUpFromWAL.invoke(queue, queue.getCurrentSeekGeneration());
      tryCatchUpFromWAL.invoke(queue, queue.getCurrentSeekGeneration());

      assertSame(iterator, subscriptionWalIterator(queue));
      verify(iterator, times(2)).refresh();
      verify(iterator, never()).close();
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testReadableWalIteratorSkipsFileListRefresh() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("skip-readable-wal-refresh");
    ConsensusPrefetchingQueue queue = null;
    try {
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              new DataRegionId(1),
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final ProgressWALIterator iterator = mock(ProgressWALIterator.class);
      when(iterator.hasNext()).thenReturn(true);
      setSubscriptionWalIterator(queue, iterator);

      invokeEnsureSubscriptionWalReadable(queue);

      verify(iterator).hasNext();
      verify(iterator, never()).refresh();
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testWalRollDoesNotRefreshNewIteratorTwice() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("skip-new-iterator-refresh");
    final File walDirectory = temporaryFolder.newFolder("skip-new-iterator-refresh-wal");
    ConsensusPrefetchingQueue queue = null;
    try {
      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn(1L);
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ProgressWALIterator replacementIterator = mock(ProgressWALIterator.class);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              new DataRegionId(1),
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true) {
            @Override
            protected ProgressWALIterator createSubscriptionWALIterator(
                final long startSearchIndex) {
              assertEquals(1L, startSearchIndex);
              return replacementIterator;
            }
          };

      final ProgressWALIterator exhaustedIterator = mock(ProgressWALIterator.class);
      when(exhaustedIterator.hasNext()).thenReturn(false);
      setSubscriptionWalIterator(queue, exhaustedIterator);

      invokeEnsureSubscriptionWalReadable(queue);

      verify(exhaustedIterator, times(2)).hasNext();
      verify(exhaustedIterator).refresh();
      verify(exhaustedIterator).close();
      verify(walNode).rollWALFile();
      verify(replacementIterator, never()).refresh();
      assertSame(replacementIterator, subscriptionWalIterator(queue));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testPendingCursorAdvanceFastForwardsWalIteratorInPlace() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("deferred-wal-realignment");
    ConsensusPrefetchingQueue queue = null;
    try {
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              new DataRegionId(1),
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);

      final ProgressWALIterator staleIterator = mock(ProgressWALIterator.class);
      setSubscriptionWalIterator(queue, staleIterator);

      final Method advancePendingCursor =
          ConsensusPrefetchingQueue.class.getDeclaredMethod(
              "advanceLocalCursorFromPendingIfPresent", IndexedConsensusRequest.class, long.class);
      advancePendingCursor.setAccessible(true);
      advancePendingCursor.invoke(
          queue,
          new IndexedConsensusRequest(7L, Collections.emptyList()),
          queue.getCurrentSeekGeneration());

      assertEquals(8L, queue.getCurrentReadSearchIndex());
      verify(staleIterator)
          .advanceTo(anyLong(), any(ProgressWALIterator.WriterProgressCoverage.class));
      verify(staleIterator, never()).close();
      assertSame(staleIterator, subscriptionWalIterator(queue));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testCleanupReconcilesUnindexedTabletReservation() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system-orphan-memory-release");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              mock(ConsensusLogToTabletConverter.class),
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(1024L);
      queue.setSubscriptionMemoryManager(memoryManager);

      final Method reserveTabletMemory =
          ConsensusPrefetchingQueue.class.getDeclaredMethod("tryReserveTabletMemory", long.class);
      reserveTabletMemory.setAccessible(true);
      assertTrue((Boolean) reserveTabletMemory.invoke(queue, 256L));
      assertEquals(256L, queue.getRetainedTabletBytes());
      assertEquals(256L, memoryManager.getUsedMemorySizeInBytes());

      queue.cleanUp();

      assertEquals(0L, queue.getRetainedTabletBytes());
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testMemoryBackpressureStopsFurtherMaterializationUntilAck() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system-memory-backpressure");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final AtomicInteger conversionCount = new AtomicInteger();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenAnswer(
              ignored -> {
                conversionCount.incrementAndGet();
                return Collections.singletonList(createTablet());
              });
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final long oneTabletBytes = createTablet().ramBytesUsed();
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(oneTabletBytes + Math.max(1L, oneTabletBytes / 2L));
      queue.setSubscriptionMemoryManager(memoryManager);

      final IndexedConsensusRequest firstRequest = createRequest(1L);
      final IndexedConsensusRequest secondRequest = createRequest(2L);
      final IndexedConsensusRequest thirdRequest = createRequest(3L);
      assertTrue(pendingEntries(queue).offer(firstRequest));
      assertTrue(pendingEntries(queue).offer(secondRequest));
      assertTrue(pendingEntries(queue).offer(thirdRequest));
      reader.currentSearchIndex = 3L;

      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();

      assertEquals(2, conversionCount.get());
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertEquals(oneTabletBytes, queue.getRetainedTabletBytes());
      assertTrue(memoryManager.getFreeMemorySizeInBytes() > 0L);
      assertEquals(1, queue.getPrefetchedEventCount());
      assertTrue(pendingEntries(queue).isEmpty());
      assertEquals("true", queue.coreReportMessage().get("realtimeAdmissionBlocked"));
      assertFalse(pendingEntries(queue).offer(createRequest(4L)));

      queue.drivePrefetchOnce();
      assertEquals(2, conversionCount.get());
      assertEquals(oneTabletBytes, queue.getRetainedTabletBytes());

      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertTrue(queue.ack("consumer", event.getCommitContext()));
      assertEquals(0L, queue.getRetainedTabletBytes());

      queue.drivePrefetchOnce();
      assertEquals("false", queue.coreReportMessage().get("realtimeAdmissionBlocked"));
      assertTrue(pendingEntries(queue).offer(secondRequest));
      assertTrue(pendingEntries(queue).offer(thirdRequest));
      queue.drivePrefetchOnce();
      assertEquals(4, conversionCount.get());
      assertEquals(3L, queue.getCurrentReadSearchIndex());
      assertEquals(oneTabletBytes, queue.getRetainedTabletBytes());
      assertEquals(1, queue.getPrefetchedEventCount());

      final SubscriptionEvent secondEvent = queue.poll("consumer");
      assertNotNull(secondEvent);
      assertTrue(queue.ack("consumer", secondEvent.getCommitContext()));
      queue.drivePrefetchOnce();
      assertTrue(pendingEntries(queue).offer(thirdRequest));
      queue.drivePrefetchOnce();

      assertEquals(5, conversionCount.get());
      assertEquals(4L, queue.getCurrentReadSearchIndex());
      assertEquals("0", queue.coreReportMessage().get("pendingEntriesSize"));
      assertEquals("0", queue.coreReportMessage().get("bufferedRealtimeEntryCount"));
      assertEquals(oneTabletBytes, queue.getRetainedTabletBytes());

      queue.close();
      queue = null;
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testWideTablePausedConsumerKeepsMaterializedMemoryBounded() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system-wide-table-memory-bound");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final int columnCount = 128;
      final int rowCount = 64;
      final long oneTabletBytes = createWideTablet(columnCount, rowCount).ramBytesUsed();
      final AtomicInteger conversionCount = new AtomicInteger();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenAnswer(
              ignored -> {
                conversionCount.incrementAndGet();
                return Collections.singletonList(createWideTablet(columnCount, rowCount));
              });
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final long memoryLimit = oneTabletBytes * 2L + oneTabletBytes / 2L;
      final SubscriptionMemoryManager memoryManager = new SubscriptionMemoryManager(memoryLimit);
      queue.setSubscriptionMemoryManager(memoryManager);

      final int writeCount = 256;
      // One scheduling attempt per submitted write proves repeated prefetch cannot bypass
      // backpressure.
      final int backpressureVerificationRounds = writeCount;
      for (long searchIndex = 1L; searchIndex <= writeCount; searchIndex++) {
        assertTrue(pendingEntries(queue).offer(createRequest(searchIndex)));
      }
      reader.currentSearchIndex = writeCount;

      assertNull(queue.poll("pausedConsumer"));
      queue.drivePrefetchOnce();

      assertEquals(3, conversionCount.get());
      assertEquals(3L, queue.getCurrentReadSearchIndex());
      assertEquals(oneTabletBytes * 2L, queue.getRetainedTabletBytes());
      assertTrue(queue.getRetainedTabletBytes() <= queue.getSubscriptionMemoryLimitInBytes());
      assertEquals(1, queue.getPrefetchedEventCount());
      assertEquals("0", queue.coreReportMessage().get("pendingEntriesSize"));
      assertEquals("0", queue.coreReportMessage().get("bufferedRealtimeEntryCount"));
      assertEquals("true", queue.coreReportMessage().get("realtimeAdmissionBlocked"));

      for (int round = 0; round < backpressureVerificationRounds; round++) {
        queue.drivePrefetchOnce();
      }
      assertEquals(3, conversionCount.get());
      assertEquals(oneTabletBytes * 2L, queue.getRetainedTabletBytes());
      assertFalse(pendingEntries(queue).offer(createRequest(writeCount + 1L)));

      final SubscriptionEvent pausedEvent = queue.poll("pausedConsumer");
      assertNotNull(pausedEvent);
      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(1L, queue.getSubscriptionUncommittedEventCount());
      for (int round = 0; round < backpressureVerificationRounds; round++) {
        queue.drivePrefetchOnce();
      }
      assertEquals(3, conversionCount.get());
      assertEquals(oneTabletBytes * 2L, queue.getRetainedTabletBytes());

      queue.close();
      queue = null;
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testLateAckAfterRecycleReleasesQueuedTabletMemory() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalRecycleInterval =
        CommonDescriptor.getInstance()
            .getConfig()
            .getSubscriptionRecycleUncommittedEventIntervalMs();
    final File systemDir = temporaryFolder.newFolder("system-late-ack-memory-release");
    ConsensusPrefetchingQueue queue = null;
    try {
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionRecycleUncommittedEventIntervalMs(-1);
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenReturn(Collections.singletonList(createTablet()), Collections.emptyList());
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(createTablet().ramBytesUsed() * 2L);
      queue.setSubscriptionMemoryManager(memoryManager);

      reader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(queue).offer(createRequest(1L)));
      assertTrue(pendingEntries(queue).offer(createRequest(2L)));

      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();
      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertTrue(queue.getRetainedTabletBytes() > 0L);
      assertEquals(1L, queue.getSubscriptionUncommittedEventCount());

      queue.drivePrefetchOnce();
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(1, queue.getPrefetchedEventCount());

      assertTrue(queue.ack("consumer", event.getCommitContext()));
      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(0L, queue.getRetainedTabletBytes());
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (queue != null) {
        queue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionRecycleUncommittedEventIntervalMs(originalRecycleInterval);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testLateAckDoesNotStealRecycledEventFromNewConsumer() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final int originalRecycleInterval =
        CommonDescriptor.getInstance()
            .getConfig()
            .getSubscriptionRecycleUncommittedEventIntervalMs();
    final File systemDir = temporaryFolder.newFolder("system-late-ack-new-owner");
    ConsensusPrefetchingQueue queue = null;
    try {
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionRecycleUncommittedEventIntervalMs(-1);
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenReturn(Collections.singletonList(createTablet()), Collections.emptyList());
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(createTablet().ramBytesUsed() * 2L);
      queue.setSubscriptionMemoryManager(memoryManager);

      reader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(queue).offer(createRequest(1L)));
      assertTrue(pendingEntries(queue).offer(createRequest(2L)));

      assertNull(queue.poll("oldConsumer"));
      queue.drivePrefetchOnce();
      final SubscriptionEvent event = queue.poll("oldConsumer");
      assertNotNull(event);
      final long retainedBytes = queue.getRetainedTabletBytes();
      assertTrue(retainedBytes > 0L);

      queue.drivePrefetchOnce();
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(1, queue.getPrefetchedEventCount());

      final SubscriptionEvent redeliveredEvent = queue.poll("newConsumer");
      assertTrue(redeliveredEvent == event);
      assertFalse(queue.ack("oldConsumer", event.getCommitContext()));
      assertEquals(1L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(retainedBytes, queue.getRetainedTabletBytes());
      assertTrue(memoryManager.getUsedMemorySizeInBytes() > 0L);

      assertTrue(queue.ack("newConsumer", event.getCommitContext()));
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0L, queue.getRetainedTabletBytes());
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      if (queue != null) {
        queue.close();
      }
      CommonDescriptor.getInstance()
          .getConfig()
          .setSubscriptionRecycleUncommittedEventIntervalMs(originalRecycleInterval);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testDeactivationReleasesMaterializedTabletMemory() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final File systemDir = temporaryFolder.newFolder("system-deactivation-memory-release");
    ConsensusPrefetchingQueue queue = null;
    try {
      final DataRegionId regionId = new DataRegionId(1);
      final FakeConsensusReqReader reader = new FakeConsensusReqReader();
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(reader);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenReturn(Collections.singletonList(createTablet()), Collections.emptyList());
      when(converter.getDatabaseName()).thenReturn("db");

      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
              regionId,
              serverImpl,
              new SubscriptionWalRetentionPolicy(
                  "topic",
                  SubscriptionWalRetentionPolicy.UNBOUNDED,
                  SubscriptionWalRetentionPolicy.UNBOUNDED),
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              1L,
              1L,
              true);
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(createTablet().ramBytesUsed() * 2L);
      queue.setSubscriptionMemoryManager(memoryManager);

      reader.currentSearchIndex = 2L;
      assertTrue(pendingEntries(queue).offer(createRequest(1L)));
      assertTrue(pendingEntries(queue).offer(createRequest(2L)));

      assertNull(queue.poll("consumer"));
      queue.drivePrefetchOnce();
      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      assertTrue(queue.getRetainedTabletBytes() > 0L);
      assertEquals(1L, queue.getSubscriptionUncommittedEventCount());
      final long previousSeekGeneration = queue.getCurrentSeekGeneration();

      queue.setActive(false);

      assertFalse(queue.isActive());
      assertEquals(0L, queue.getInitializedStatus());
      assertEquals(previousSeekGeneration + 1L, queue.getCurrentSeekGeneration());
      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0L, queue.getRetainedTabletBytes());
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      assertFalse(pendingEntries(queue).offer(createRequest(3L)));
    } finally {
      if (queue != null) {
        queue.close();
      }
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @SuppressWarnings("unchecked")
  private static BlockingQueue<IndexedConsensusRequest> pendingEntries(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }

  private static ReentrantReadWriteLock queueLock(final ConsensusPrefetchingQueue queue)
      throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("lock");
    field.setAccessible(true);
    return (ReentrantReadWriteLock) field.get(queue);
  }

  private static ProgressWALIterator subscriptionWalIterator(final ConsensusPrefetchingQueue queue)
      throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("subscriptionWALIterator");
    field.setAccessible(true);
    return (ProgressWALIterator) field.get(queue);
  }

  private static void setSubscriptionWalIterator(
      final ConsensusPrefetchingQueue queue, final ProgressWALIterator iterator) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("subscriptionWALIterator");
    field.setAccessible(true);
    field.set(queue, iterator);
  }

  private static void invokeEnsureSubscriptionWalReadable(final ConsensusPrefetchingQueue queue)
      throws Exception {
    final Method method =
        ConsensusPrefetchingQueue.class.getDeclaredMethod("ensureSubscriptionWalReadable");
    method.setAccessible(true);
    method.invoke(queue);
  }

  private static Tablet createTablet() {
    final List<String> columnNames = Arrays.asList("device", "temperature");
    final List<TSDataType> dataTypes = Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE);
    final List<ColumnCategory> categories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
    final Tablet tablet = new Tablet("sensors", columnNames, dataTypes, categories, 1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, 36.5);
    tablet.setRowSize(1);
    return tablet;
  }

  private static Tablet createWideTablet(final int columnCount, final int rowCount) {
    final String[] columnNames = new String[columnCount];
    final TSDataType[] dataTypes = new TSDataType[columnCount];
    final ColumnCategory[] categories = new ColumnCategory[columnCount];
    Arrays.fill(dataTypes, TSDataType.DOUBLE);
    Arrays.fill(categories, ColumnCategory.FIELD);
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      columnNames[columnIndex] = "field_" + columnIndex;
    }

    final Tablet tablet =
        new Tablet(
            "wide_table",
            Arrays.asList(columnNames),
            Arrays.asList(dataTypes),
            Arrays.asList(categories),
            rowCount);
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      tablet.addTimestamp(rowIndex, rowIndex);
      for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        tablet.addValue(rowIndex, columnIndex, (double) (rowIndex + columnIndex));
      }
    }
    tablet.setRowSize(rowCount);
    return tablet;
  }

  private static IndexedConsensusRequest createRequest(final long searchIndex) {
    return new IndexedConsensusRequest(
            searchIndex,
            Collections.singletonList(
                StatementTestUtils.genInsertRowNode(Math.toIntExact(searchIndex))))
        .setPhysicalTime(1000L + searchIndex)
        .setNodeId(7);
  }

  private static ConsensusSubscriptionCommitManager newCommitManager(final File systemDir)
      throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
    final Constructor<ConsensusSubscriptionCommitManager> constructor =
        ConsensusSubscriptionCommitManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static ConsensusSubscriptionCommitManager newCommitManager(
      final File systemDir,
      final ConsensusSubscriptionCommitManager.ConfigNodeProgressFetcher progressFetcher) {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
    return new ConsensusSubscriptionCommitManager(progressFetcher);
  }

  private static final class FakeConsensusReqReader implements ConsensusReqReader {

    private long currentSearchIndex;

    @Override
    public void setSafelyDeletedSearchIndex(final long safelyDeletedSearchIndex) {
      // no-op
    }

    @Override
    public ReqIterator getReqIterator(final long startIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getCurrentSearchIndex() {
      return currentSearchIndex;
    }

    @Override
    public long getCurrentWALFileVersion() {
      return 0;
    }

    @Override
    public long getTotalSize() {
      return 0;
    }

    @Override
    public Pair<Long, Long> getDeletionBoundToFreeAtLeast(final long bytesToFree) {
      return new Pair<>(DEFAULT_SAFELY_DELETED_SEARCH_INDEX, 0L);
    }
  }
}
