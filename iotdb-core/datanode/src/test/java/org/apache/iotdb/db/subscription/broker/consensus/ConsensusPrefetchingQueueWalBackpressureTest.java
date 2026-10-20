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
import org.apache.iotdb.commons.memory.AtomicLongMemoryBlock;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.logdispatcher.IoTConsensusMemoryManager;
import org.apache.iotdb.consensus.iot.subscription.SubscriptionQueueRejectionReason;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
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
import org.apache.tsfile.write.record.Tablet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueWalBackpressureTest {

  private static final int WRITER_NODE_ID = 7;
  private static final int REQUEST_COUNT = 3;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testYieldedGapScanKeepsIteratorAndDeliversEachRequestOnce() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalBatchMaxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    final int originalBatchMaxTabletCount = config.getSubscriptionConsensusBatchMaxTabletCount();
    final int originalBatchMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("system-yielded-wal-gap");
    final File walDirectory = temporaryFolder.newFolder("yielded-wal-gap");
    ConsensusPrefetchingQueue queue = null;
    try {
      config.setSubscriptionConsensusBatchMaxWalEntries(2);
      config.setSubscriptionConsensusBatchMaxTabletCount(1);
      config.setSubscriptionConsensusBatchMaxDelayInMs(0);
      final File dataWal =
          new File(
              walDirectory,
              WALFileUtils.getLogFileName(0L, 0L, WALFileStatus.CONTAINS_SEARCH_INDEX));
      final File liveWal =
          new File(
              walDirectory,
              WALFileUtils.getLogFileName(1L, 3L, WALFileStatus.CONTAINS_SEARCH_INDEX));
      try (WALWriter writer = new WALWriter(dataWal, WALFileVersion.V3)) {
        // The replay cursor is 2, but its WAL file has a fragmented prefix for local index 1.
        for (int fragment = 0; fragment < 9; fragment++) {
          writeWalEntry(writer, 1L);
        }
        writeWalEntry(writer, 2L);
        writeWalEntry(writer, 3L);
      }
      try (WALWriter ignored = new WALWriter(liveWal, WALFileVersion.V3)) {
        // Keep a sealed successor so the fragmented prefix can be read.
      }

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn(3L);
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getWALMetaDataSnapshot(anyLong())).thenReturn(new WALMetaData());
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final List<Long> conversions = new ArrayList<>();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenAnswer(
              invocation -> {
                conversions.add(((InsertNode) invocation.getArgument(0)).getSearchIndex());
                return Collections.singletonList(createTablet());
              });
      when(converter.getDatabaseName()).thenReturn("db");
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
              converter,
              newCommitManager(systemDir),
              new RegionProgress(Collections.emptyMap()),
              2L,
              2L,
              true);
      final SubscriptionMemoryManager memoryManager =
          new SubscriptionMemoryManager(createTablet().ramBytesUsed() * 8L);
      queue.setSubscriptionMemoryManager(memoryManager);
      assertNull(queue.poll("consumer"));
      assertTrue(pendingEntries(queue).offer(createRequest(3L)));

      assertEquals(PrefetchRoundResult.Type.RESCHEDULE_NOW, queue.drivePrefetchOnce().getType());
      final ProgressWALIterator iterator = subscriptionWalIterator(queue);
      assertTrue(iterator.hasYieldedScan());
      assertTrue(conversions.isEmpty());
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      verify(walNode, never()).rollWALFile();

      // Repeated realtime jumps must continue the yielded scan rather than reset it to index 2.
      for (int round = 0; round < 20 && queue.getCurrentReadSearchIndex() < 4L; round++) {
        if (queue.getCurrentReadSearchIndex() < 3L) {
          assertTrue(pendingEntries(queue).offer(createRequest(3L)));
        }
        queue.drivePrefetchOnce();
        assertSame(iterator, subscriptionWalIterator(queue));
      }
      assertEquals(4L, queue.getCurrentReadSearchIndex());
      assertEquals(Arrays.asList(2L, 3L), conversions);
      for (long expected = 2L; expected <= 3L; expected++) {
        final SubscriptionEvent event = queue.poll("consumer");
        assertNotNull(event);
        assertEquals(expected, event.getCommitContext().getWriterProgress().getLocalSeq());
        assertTrue(queue.ack("consumer", event.getCommitContext()));
      }
      queue.drivePrefetchOnce();
      assertNull(queue.poll("consumer"));
      for (int round = 0; round < 10 && queue.getPendingEventCount() > 0; round++) {
        queue.drivePrefetchOnce();
      }
      assertEquals(0, queue.getPendingEventCount());

      // Rebuilding at the unchanged cursor must position the reader past covered history. The
      // fragmented prefix is larger than a whole scan budget and must not delay WAL takeover.
      try (WALWriter writer = new WALWriter(liveWal, WALFileVersion.V3)) {
        writeWalEntry(writer, 4L);
        writeWalEntry(writer, 5L);
      }
      try (WALWriter ignored =
          new WALWriter(
              new File(
                  walDirectory,
                  WALFileUtils.getLogFileName(2L, 5L, WALFileStatus.CONTAINS_SEARCH_INDEX)),
              WALFileVersion.V3)) {
        // Seal the new data file so replay can take over at index 4.
      }
      when(walNode.getCurrentSearchIndex()).thenReturn(5L);
      when(walNode.getCurrentWALFileVersion()).thenReturn(2L);
      // The factory must return an iterator that is ready for bounded replay at the requested
      // cursor, without requiring a separate advanceTo call from its caller.
      try (ProgressWALIterator createdIterator = queue.createSubscriptionWALIterator(4L)) {
        final ProgressWALIterator.ScanBudget budget =
            new ProgressWALIterator.ScanBudget(2, Long.MAX_VALUE);
        assertTrue(createdIterator.hasNext(budget));
        assertEquals(4L, createdIterator.next().getSearchIndex());
        assertFalse(budget.hasYielded());
      }
      final Method reset =
          ConsensusPrefetchingQueue.class.getDeclaredMethod(
              "resetSubscriptionWALPosition", long.class);
      reset.setAccessible(true);
      reset.invoke(queue, 4L);
      assertTrue(pendingEntries(queue).offer(createRequest(5L)));
      queue.drivePrefetchOnce();
      assertTrue(queue.toString(), queue.getCurrentReadSearchIndex() >= 5L);
      for (int round = 0; round < 10 && queue.getCurrentReadSearchIndex() < 6L; round++) {
        queue.drivePrefetchOnce();
      }
      assertEquals(6L, queue.getCurrentReadSearchIndex());
      assertEquals(Arrays.asList(2L, 3L, 4L, 5L), conversions);
      for (long expected = 4L; expected <= 5L; expected++) {
        final SubscriptionEvent event = queue.poll("consumer");
        assertNotNull(event);
        assertEquals(expected, event.getCommitContext().getWriterProgress().getLocalSeq());
        assertTrue(queue.ack("consumer", event.getCommitContext()));
      }
      assertEquals(0L, queue.getWalGapSkippedEntries());
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      verify(walNode, never()).rollWALFile();
    } finally {
      if (queue != null) {
        queue.close();
      }
      config.setSubscriptionConsensusBatchMaxWalEntries(originalBatchMaxWalEntries);
      config.setSubscriptionConsensusBatchMaxTabletCount(originalBatchMaxTabletCount);
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testWalGapRecoveryResumesRealtimeAfterConsensusMemoryRejection() throws Exception {
    assertWalGapRecoveryResumesRealtime(false, false, false);
  }

  @Test
  public void testWalGapRecoveryResumesRealtimeAfterAck() throws Exception {
    assertWalGapRecoveryResumesRealtime(true, false, false);
  }

  @Test
  public void testWalGapRecoveryResumesPendingWhenWalStartsAfterGap() throws Exception {
    assertWalGapRecoveryResumesRealtime(false, true, false);
  }

  @Test
  public void testWalGapRecoveryPreservesFollowerBeforePendingBoundary() throws Exception {
    assertWalGapRecoveryResumesRealtime(false, false, true);
  }

  private void assertWalGapRecoveryResumesRealtime(
      final boolean memoryBackpressure,
      final boolean missingWalGap,
      final boolean followerBeforePending)
      throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalBatchMaxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    final int originalBatchMaxTabletCount = config.getSubscriptionConsensusBatchMaxTabletCount();
    final int originalBatchMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    final long originalWatermarkInterval = config.getSubscriptionConsensusWatermarkIntervalMs();
    final IoTConsensusMemoryManager requestMemoryManager = IoTConsensusMemoryManager.getInstance();
    final IMemoryBlock originalRequestMemoryBlock = requestMemoryManager.getMemoryBlock();
    final double originalQueueRatio = requestMemoryManager.getMaxMemoryRatioForQueue();
    final AtomicLongMemoryBlock requestMemoryBlock =
        new AtomicLongMemoryBlock("subscription-gap-recovery", null, 1_000_000L);
    final File systemDir = temporaryFolder.newFolder("system-gap-recovery");
    final File walDirectory = temporaryFolder.newFolder("wal-gap-recovery");
    final int requestCount = 8;
    ConsensusPrefetchingQueue queue = null;
    try {
      config.setSubscriptionConsensusBatchMaxWalEntries(128);
      config.setSubscriptionConsensusBatchMaxTabletCount(1);
      config.setSubscriptionConsensusBatchMaxDelayInMs(0);
      config.setSubscriptionConsensusWatermarkIntervalMs(0L);
      writeSealedWal(walDirectory, requestCount, missingWalGap ? 2L : 1L, followerBeforePending);

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn((long) requestCount);
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getWALMetaDataSnapshot(anyLong())).thenReturn(new WALMetaData());
      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());
      final List<Long> conversions = new ArrayList<>();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenAnswer(
              invocation -> {
                conversions.add(((InsertNode) invocation.getArgument(0)).getSearchIndex());
                return Collections.singletonList(createTablet());
              });
      when(converter.getDatabaseName()).thenReturn("db");
      final DataRegionId regionId = new DataRegionId(1);
      final ConsensusSubscriptionCommitManager commitManager = newCommitManager(systemDir);
      queue =
          new ConsensusPrefetchingQueue(
              "consumerGroup",
              "topic",
              followerBeforePending
                  ? TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE
                  : TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
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
      if (followerBeforePending) {
        queue.setActiveWriterNodeIds(Set.of(WRITER_NODE_ID, WRITER_NODE_ID + 1));
      }
      final long oneTabletBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(createTablet());
      final TrackingSubscriptionMemoryManager memoryManager =
          new TrackingSubscriptionMemoryManager(
              memoryBackpressure ? oneTabletBytes * 4L : oneTabletBytes * requestCount * 4L);
      queue.setSubscriptionMemoryManager(memoryManager);
      assertNull(queue.poll("consumer"));

      // A rejected request creates a WAL-only hole. Once consensus memory is released, subsequent
      // requests are admitted again and must be delivered from pending after that hole is filled.
      requestMemoryManager.init(requestMemoryBlock, 1.0);
      requestMemoryBlock.forceAllocateWithoutLimitation(1_000_000L);
      assertFalse(pendingEntries(queue).offer(createRequest(1L)));
      assertEquals(
          SubscriptionQueueRejectionReason.CONSENSUS_REQUEST_MEMORY_LIMIT.getCode(),
          queue.getLastAdmissionRejectionCode());
      requestMemoryBlock.release(1_000_000L);
      for (long index = 2L; index <= requestCount; index++) {
        assertTrue(pendingEntries(queue).offer(createRequest(index)));
      }

      long deliveredLocalSeq = missingWalGap ? 1L : 0L;
      for (int round = 0; round < 32 && deliveredLocalSeq < requestCount; round++) {
        queue.drivePrefetchOnce();
        assertMemoryBounded(queue, memoryManager);
        SubscriptionEvent event;
        while ((event = queue.poll("consumer")) != null) {
          assertEquals(WRITER_NODE_ID, event.getCommitContext().getWriterId().getNodeId());
          deliveredLocalSeq++;
          assertEquals(
              deliveredLocalSeq, event.getCommitContext().getWriterProgress().getLocalSeq());
          assertTrue(queue.ack("consumer", event.getCommitContext()));
        }
        assertEquals(
            followerBeforePending ? oneTabletBytes : 0L, memoryManager.getUsedMemorySizeInBytes());
      }
      assertEquals(requestCount, deliveredLocalSeq);
      if (followerBeforePending) {
        // The replicated writer's later timestamp lets local requests drain in order. Removing
        // the local writer then releases that final follower entry without reading it again.
        queue.setActiveWriterNodeIds(Collections.singleton(WRITER_NODE_ID + 1));
        queue.drivePrefetchOnce();
        final SubscriptionEvent follower = queue.poll("consumer");
        assertNotNull(follower);
        assertEquals(WRITER_NODE_ID + 1, follower.getCommitContext().getWriterId().getNodeId());
        assertEquals(
            new WriterProgress(2000L, 1L), follower.getCommitContext().getWriterProgress());
        assertTrue(queue.ack("consumer", follower.getCommitContext()));
        assertNull(queue.poll("consumer"));
        assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      }
      assertEquals(
          missingWalGap ? 0L : (followerBeforePending ? 2L : 1L),
          queue.getWalPathAcceptedEntries());
      assertEquals(requestCount - 1L, queue.getPendingPathAcceptedEntries());
      assertEquals(1L, queue.getRealtimeAdmissionRejectionCount());
      assertEquals(missingWalGap ? 1L : 0L, queue.getWalGapSkippedEntries());
      assertEquals(0L, queue.getRetainedRequestBytes());
      assertEquals(0L, requestMemoryBlock.getUsedMemoryInBytes());
      assertTrue(pendingEntries(queue).isEmpty());
      assertEquals(
          new WriterProgress(1000L + requestCount, requestCount),
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .get(new WriterId(regionId.toString(), WRITER_NODE_ID)));
      if (!memoryBackpressure) {
        assertEquals(
            missingWalGap
                ? Arrays.asList(2L, 3L, 4L, 5L, 6L, 7L, 8L)
                : (followerBeforePending
                    ? Arrays.asList(1L, -1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)
                    : Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)),
            conversions);
      }
    } finally {
      if (queue != null) {
        queue.close();
      }
      requestMemoryManager.init(originalRequestMemoryBlock, originalQueueRatio);
      config.setSubscriptionConsensusBatchMaxWalEntries(originalBatchMaxWalEntries);
      config.setSubscriptionConsensusBatchMaxTabletCount(originalBatchMaxTabletCount);
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      config.setSubscriptionConsensusWatermarkIntervalMs(originalWatermarkInterval);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testAckResumesPendingSuffixWithoutWalReplay() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalBatchMaxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    final int originalBatchMaxTabletCount = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long originalBatchMaxSize = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    final int originalBatchMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("system-wal-backpressure-recovery");
    final File walDirectory = temporaryFolder.newFolder("wal-backpressure-recovery");
    ConsensusPrefetchingQueue queue = null;
    try {
      final Tablet sampleTablet = createTablet();
      final long oneTabletBytes = sampleTablet.ramBytesUsed();
      config.setSubscriptionConsensusBatchMaxWalEntries(128);
      config.setSubscriptionConsensusBatchMaxTabletCount(128);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(oneTabletBytes * 16L);
      config.setSubscriptionConsensusBatchMaxDelayInMs(0);

      writeSealedWal(walDirectory);

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn((long) REQUEST_COUNT);
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getWALMetaDataSnapshot(anyLong())).thenReturn(new WALMetaData());

      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final List<Long> conversionAttempts = new ArrayList<>();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenAnswer(
              invocation -> {
                final InsertNode insertNode = (InsertNode) invocation.getArgument(0);
                conversionAttempts.add(insertNode.getSearchIndex());
                return Collections.singletonList(createTablet());
              });
      when(converter.getDatabaseName()).thenReturn("db");

      final DataRegionId regionId = new DataRegionId(1);
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
      final long memoryLimit = oneTabletBytes + Math.max(1L, oneTabletBytes / 2L);
      final TrackingSubscriptionMemoryManager memoryManager =
          new TrackingSubscriptionMemoryManager(memoryLimit);
      queue.setSubscriptionMemoryManager(memoryManager);

      assertNull(queue.poll("consumer"));
      for (long searchIndex = 1L; searchIndex <= REQUEST_COUNT; searchIndex++) {
        assertTrue(pendingEntries(queue).offer(createRequest(searchIndex)));
      }

      queue.drivePrefetchOnce();

      assertEquals(Arrays.asList(1L, 2L), conversionAttempts);
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertEquals(1, queue.getPrefetchedEventCount());
      assertEquals(2, pendingEntries(queue).size());
      assertTrue(queue.getRetainedRequestBytes() > 0L);
      assertEquals("false", queue.coreReportMessage().get("realtimeAdmissionBlocked"));
      assertEquals(oneTabletBytes, memoryManager.getUsedMemorySizeInBytes());
      assertMemoryBounded(queue, memoryManager);

      final List<Long> deliveredLocalSequences = new ArrayList<>();
      for (long expectedLocalSequence = 1L;
          expectedLocalSequence <= REQUEST_COUNT;
          expectedLocalSequence++) {
        final SubscriptionEvent event = queue.poll("consumer");
        assertNotNull(event);
        final long deliveredLocalSequence =
            event.getCommitContext().getWriterProgress().getLocalSeq();
        assertEquals(expectedLocalSequence, deliveredLocalSequence);
        deliveredLocalSequences.add(deliveredLocalSequence);

        assertTrue(queue.ack("consumer", event.getCommitContext()));
        assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
        assertMemoryBounded(queue, memoryManager);

        queue.drivePrefetchOnce();
        assertMemoryBounded(queue, memoryManager);
        if (expectedLocalSequence < REQUEST_COUNT) {
          assertEquals(1, queue.getPrefetchedEventCount());
          assertEquals(oneTabletBytes, memoryManager.getUsedMemorySizeInBytes());
        }
      }

      assertEquals(Arrays.asList(1L, 2L, 3L), deliveredLocalSequences);
      assertEquals(Arrays.asList(1L, 2L, 2L, 3L, 3L), conversionAttempts);
      assertEquals(3L, queue.getPendingPathAcceptedEntries());
      assertEquals(0L, queue.getWalPathAcceptedEntries());
      assertEquals(0L, queue.getRetainedRequestBytes());
      assertEquals(0L, queue.getRealtimeAdmissionRejectionCount());
      assertEquals(4L, queue.getCurrentReadSearchIndex());
      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertNull(queue.poll("consumer"));
      assertEquals(
          new WriterProgress(1000L + REQUEST_COUNT, REQUEST_COUNT),
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .get(new WriterId(regionId.toString(), WRITER_NODE_ID)));
      assertEquals(oneTabletBytes, memoryManager.getMaximumUsedMemorySizeInBytes());
      assertFalse(memoryManager.hasExceededBudget());
    } finally {
      if (queue != null) {
        queue.close();
      }
      config.setSubscriptionConsensusBatchMaxWalEntries(originalBatchMaxWalEntries);
      config.setSubscriptionConsensusBatchMaxTabletCount(originalBatchMaxTabletCount);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(originalBatchMaxSize);
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testWalMemoryBlockRetriesWithoutRecreatingIterator() throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalBatchMaxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    final int originalBatchMaxTabletCount = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long originalBatchMaxSize = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    final int originalBatchMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("system-wal-memory-retry");
    final File walDirectory = temporaryFolder.newFolder("wal-memory-retry");
    ConsensusPrefetchingQueue queue = null;
    try {
      final Tablet sampleTablet = createTablet();
      final long oneTabletBytes = sampleTablet.ramBytesUsed();
      config.setSubscriptionConsensusBatchMaxWalEntries(128);
      config.setSubscriptionConsensusBatchMaxTabletCount(128);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(oneTabletBytes * 16L);
      config.setSubscriptionConsensusBatchMaxDelayInMs(0);

      writeSealedWal(walDirectory);

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn((long) REQUEST_COUNT);
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getWALMetaDataSnapshot(anyLong())).thenReturn(new WALMetaData());

      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final List<Long> conversionAttempts = new ArrayList<>();
      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      when(converter.convert(any()))
          .thenAnswer(
              invocation -> {
                final InsertNode insertNode = (InsertNode) invocation.getArgument(0);
                conversionAttempts.add(insertNode.getSearchIndex());
                return Collections.singletonList(createTablet());
              });
      when(converter.getDatabaseName()).thenReturn("db");

      final DataRegionId regionId = new DataRegionId(1);
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
      final TrackingSubscriptionMemoryManager memoryManager =
          new TrackingSubscriptionMemoryManager(oneTabletBytes + Math.max(1L, oneTabletBytes / 2L));
      queue.setSubscriptionMemoryManager(memoryManager);

      assertNull(queue.poll("consumer"));
      final ProgressWALIterator iterator = subscriptionWalIterator(queue);

      queue.drivePrefetchOnce();
      assertEquals(Arrays.asList(1L, 2L), conversionAttempts);
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertTrue(iterator.hasBufferedNext());
      assertSame(iterator, subscriptionWalIterator(queue));
      assertMemoryBounded(queue, memoryManager);

      // No memory has been released yet: another round must neither convert nor lose the request.
      queue.drivePrefetchOnce();
      assertEquals(Arrays.asList(1L, 2L), conversionAttempts);
      assertEquals(2L, queue.getCurrentReadSearchIndex());
      assertSame(iterator, subscriptionWalIterator(queue));

      for (long expectedLocalSeq = 1L; expectedLocalSeq <= REQUEST_COUNT; expectedLocalSeq++) {
        final SubscriptionEvent event = queue.poll("consumer");
        assertNotNull(event);
        assertEquals(expectedLocalSeq, event.getCommitContext().getWriterProgress().getLocalSeq());
        assertEquals(
            1000L + expectedLocalSeq,
            event.getCommitContext().getWriterProgress().getPhysicalTime());
        assertTrue(queue.ack("consumer", event.getCommitContext()));
        assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());

        queue.drivePrefetchOnce();
        assertSame(iterator, subscriptionWalIterator(queue));
        assertMemoryBounded(queue, memoryManager);
      }

      assertEquals(Arrays.asList(1L, 2L, 2L, 3L, 3L), conversionAttempts);
      assertEquals(3L, queue.getWalPathAcceptedEntries());
      assertEquals(4L, queue.getCurrentReadSearchIndex());
      assertEquals(0, queue.getPrefetchedEventCount());
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0L, queue.getRetainedTabletBytes());
      assertNull(queue.poll("consumer"));
      assertEquals(
          new WriterProgress(1000L + REQUEST_COUNT, REQUEST_COUNT),
          commitManager
              .getCommittedRegionProgress("consumerGroup", "topic", regionId)
              .getWriterPositions()
              .get(new WriterId(regionId.toString(), WRITER_NODE_ID)));
    } finally {
      if (queue != null) {
        queue.close();
      }
      config.setSubscriptionConsensusBatchMaxWalEntries(originalBatchMaxWalEntries);
      config.setSubscriptionConsensusBatchMaxTabletCount(originalBatchMaxTabletCount);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(originalBatchMaxSize);
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testPendingGapReplayDoesNotRescanPrefixAcrossBoundedRounds() throws Exception {
    assertPendingGapReplay(false, false);
  }

  @Test
  public void testPendingGapReplayKeepsMemoryBlockedRequestAcrossRounds() throws Exception {
    assertPendingGapReplay(true, false);
  }

  @Test
  public void testPendingGapReplayRetriesRequestAfterConversionFailure() throws Exception {
    assertPendingGapReplay(false, true);
  }

  private void assertPendingGapReplay(
      final boolean memoryBackpressure, final boolean materializationFailure) throws Exception {
    final String originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int originalBatchMaxWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    final int originalBatchMaxTabletCount = config.getSubscriptionConsensusBatchMaxTabletCount();
    final long originalBatchMaxSize = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    final int originalBatchMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    final File systemDir = temporaryFolder.newFolder("system-pending-gap-replay");
    final File walDirectory = temporaryFolder.newFolder("wal-pending-gap-replay");
    ConsensusPrefetchingQueue queue = null;
    try {
      final long oneTabletBytes = createTablet().ramBytesUsed();
      config.setSubscriptionConsensusBatchMaxWalEntries(2);
      config.setSubscriptionConsensusBatchMaxTabletCount(1);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(oneTabletBytes * 16L);
      config.setSubscriptionConsensusBatchMaxDelayInMs(60_000);

      final int replayedRequests = materializationFailure ? 7 : 6;
      final int maxRounds = 20;
      final int requestCount = replayedRequests + 2 * maxRounds;
      writeSealedWal(walDirectory, requestCount);

      final WALNode walNode = mock(WALNode.class);
      when(walNode.getLogDirectory()).thenReturn(walDirectory);
      when(walNode.getCurrentSearchIndex()).thenReturn((long) requestCount);
      when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
      when(walNode.getWALMetaDataSnapshot(anyLong())).thenReturn(new WALMetaData());

      final IoTConsensusServerImpl serverImpl = mock(IoTConsensusServerImpl.class);
      when(serverImpl.getConsensusReqReader()).thenReturn(walNode);
      when(serverImpl.getWriterSafeFrontierTracker()).thenReturn(new WriterSafeFrontierTracker());

      final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
      final AtomicBoolean failureInjected = new AtomicBoolean();
      when(converter.convert(any()))
          .thenAnswer(
              invocation -> {
                final InsertNode insertNode = (InsertNode) invocation.getArgument(0);
                if (materializationFailure
                    && insertNode.getSearchIndex() == 2L
                    && failureInjected.compareAndSet(false, true)) {
                  throw new IllegalStateException("transient conversion failure");
                }
                return Collections.singletonList(createTablet());
              });
      when(converter.getDatabaseName()).thenReturn("db");

      final AtomicLong closedIteratorBodyReads = new AtomicLong();
      final DataRegionId regionId = new DataRegionId(1);
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
              true) {
            @Override
            protected ProgressWALIterator createSubscriptionWALIterator(
                final long startSearchIndex) {
              return new ProgressWALIterator(walNode, startSearchIndex) {
                @Override
                public void close() throws IOException {
                  closedIteratorBodyReads.addAndGet(walBodyReadCount(this));
                  super.close();
                }
              };
            }
          };
      final TrackingSubscriptionMemoryManager memoryManager =
          new TrackingSubscriptionMemoryManager(
              memoryBackpressure
                  ? oneTabletBytes + Math.max(1L, oneTabletBytes / 2L)
                  : oneTabletBytes * 16L);
      queue.setSubscriptionMemoryManager(memoryManager);

      assertNull(queue.poll("consumer"));
      // Admission failures leave a hole before newer pending requests. Keep a suffix queued so
      // every bounded round enters fillGapFromWAL, including after ACK releases Tablet memory.
      // Leave the gap unresolved through the last round so this test measures only WAL work.
      for (long index = replayedRequests + 3L; index <= requestCount; index++) {
        assertTrue(pendingEntries(queue).offer(createRequest(index)));
      }

      long deliveredLocalSeq = 0L;
      for (int round = 0; round < maxRounds && deliveredLocalSeq < replayedRequests; round++) {
        // Physical scan limits include lookahead and skipped prefixes; a deadline can also end a
        // round early. Bound the final logical batch so replay stops at the asserted frontier.
        final int maxWalEntries =
            memoryBackpressure ? 2 : (int) Math.min(2L, replayedRequests - deliveredLocalSeq);
        config.setSubscriptionConsensusBatchMaxWalEntries(maxWalEntries);
        final long memoryRejections = queue.getSubscriptionMemoryRejectionCount();
        queue.drivePrefetchOnce();
        final long expectedLocalSeq = queue.getCurrentReadSearchIndex() - 1L;
        assertTrue(expectedLocalSeq >= deliveredLocalSeq);
        assertTrue(expectedLocalSeq <= replayedRequests);
        assertTrue(expectedLocalSeq - deliveredLocalSeq <= maxWalEntries);
        assertMemoryBounded(queue, memoryManager);
        if (queue.getSubscriptionMemoryRejectionCount() > memoryRejections) {
          assertTrue(subscriptionWalIterator(queue).hasBufferedNext());
        }
        while (deliveredLocalSeq < expectedLocalSeq) {
          final SubscriptionEvent event = queue.poll("consumer");
          assertNotNull(event);
          deliveredLocalSeq++;
          assertEquals(
              deliveredLocalSeq, event.getCommitContext().getWriterProgress().getLocalSeq());
          assertEquals(
              1000L + deliveredLocalSeq,
              event.getCommitContext().getWriterProgress().getPhysicalTime());
          assertTrue(queue.ack("consumer", event.getCommitContext()));
        }
        assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
      }

      assertEquals(replayedRequests, deliveredLocalSeq);
      assertEquals(replayedRequests, queue.getWalPathAcceptedEntries());
      assertEquals(0L, queue.getPendingPathAcceptedEntries());
      assertEquals(0L, queue.getWalGapSkippedEntries());
      assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0L, queue.getRetainedTabletBytes());
      assertEquals(2 * maxRounds - 2, pendingEntries(queue).size());
      assertTrue(queue.getRetainedRequestBytes() > 0L);
      if (memoryBackpressure) {
        assertTrue(queue.getSubscriptionMemoryRejectionCount() > 0L);
      }
      // Grouping reads one request ahead; a blocked request can require one further lookahead.
      // Count actual body reads, including discarded iterators, rather than timing disk caches.
      // An exceptional conversion failure needs one reset, rereading its three consumed bodies.
      assertEquals(
          replayedRequests + (memoryBackpressure ? 2L : 1L) + (materializationFailure ? 3L : 0L),
          closedIteratorBodyReads.get() + walBodyReadCount(subscriptionWalIterator(queue)));
    } finally {
      if (queue != null) {
        queue.close();
      }
      config.setSubscriptionConsensusBatchMaxWalEntries(originalBatchMaxWalEntries);
      config.setSubscriptionConsensusBatchMaxTabletCount(originalBatchMaxTabletCount);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(originalBatchMaxSize);
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalBatchMaxDelay);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  private static int walBodyReadCount(final ProgressWALIterator iterator) {
    try {
      // All requests stay in one historical file, so this offset is the number of body reads.
      final Field field =
          ProgressWALIterator.class.getDeclaredField("consumedEntryCountInCurrentFile");
      field.setAccessible(true);
      return field.getInt(iterator);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static void writeSealedWal(final File walDirectory) throws Exception {
    writeSealedWal(walDirectory, REQUEST_COUNT);
  }

  private static void writeSealedWal(final File walDirectory, final int requestCount)
      throws Exception {
    writeSealedWal(walDirectory, requestCount, 1L);
  }

  private static void writeSealedWal(
      final File walDirectory, final int requestCount, final long firstSearchIndex)
      throws Exception {
    writeSealedWal(walDirectory, requestCount, firstSearchIndex, false);
  }

  private static void writeSealedWal(
      final File walDirectory,
      final int requestCount,
      final long firstSearchIndex,
      final boolean followerBeforePending)
      throws Exception {
    final File historicalWal =
        new File(
            walDirectory, WALFileUtils.getLogFileName(0L, 0L, WALFileStatus.CONTAINS_SEARCH_INDEX));
    final File liveWal =
        new File(
            walDirectory,
            WALFileUtils.getLogFileName(1L, requestCount, WALFileStatus.CONTAINS_SEARCH_INDEX));

    try (WALWriter writer = new WALWriter(historicalWal, WALFileVersion.V3)) {
      for (long searchIndex = firstSearchIndex; searchIndex <= requestCount; searchIndex++) {
        writeWalEntry(writer, searchIndex);
        if (searchIndex == 1L && followerBeforePending) {
          writeWalEntry(writer, -1L, 2000L, WRITER_NODE_ID + 1, 1L);
        }
      }
    }
    try (WALWriter ignored = new WALWriter(liveWal, WALFileVersion.V3)) {
      // Keep an empty live successor so the WAL containing the requests is historical and readable.
    }
  }

  private static void writeWalEntry(final WALWriter writer, final long searchIndex)
      throws Exception {
    writeWalEntry(writer, searchIndex, 1000L + searchIndex, WRITER_NODE_ID, searchIndex);
  }

  private static void writeWalEntry(
      final WALWriter writer,
      final long searchIndex,
      final long physicalTime,
      final int writerNodeId,
      final long localSeq)
      throws Exception {
    final RelationalInsertRowNode insertNode =
        StatementTestUtils.genInsertRowNode(Math.toIntExact(searchIndex));
    insertNode.setSearchIndex(searchIndex);
    insertNode.setLastFragment(true);
    final WALInfoEntry walEntry = new WALInfoEntry(1L, insertNode);
    final ByteBuffer buffer = ByteBuffer.allocate(walEntry.serializedSize());
    walEntry.serialize(new WALByteBufferForTest(buffer));

    final WALMetaData metaData = new WALMetaData();
    metaData.add(walEntry.serializedSize(), searchIndex, 1L, physicalTime, writerNodeId, localSeq);
    writer.write(buffer, metaData);
  }

  private static IndexedConsensusRequest createRequest(final long searchIndex) {
    return new IndexedConsensusRequest(
            searchIndex,
            Collections.singletonList(
                StatementTestUtils.genInsertRowNode(Math.toIntExact(searchIndex))))
        .setPhysicalTime(1000L + searchIndex)
        .setNodeId(WRITER_NODE_ID);
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

  private static void assertMemoryBounded(
      final ConsensusPrefetchingQueue queue,
      final TrackingSubscriptionMemoryManager memoryManager) {
    assertEquals(queue.getRetainedTabletBytes(), memoryManager.getUsedMemorySizeInBytes());
    assertTrue(
        memoryManager.getUsedMemorySizeInBytes() <= memoryManager.getTotalMemorySizeInBytes());
    assertFalse(memoryManager.hasExceededBudget());
  }

  @SuppressWarnings("unchecked")
  private static BlockingQueue<IndexedConsensusRequest> pendingEntries(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }

  private static ProgressWALIterator subscriptionWalIterator(final ConsensusPrefetchingQueue queue)
      throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("subscriptionWALIterator");
    field.setAccessible(true);
    return (ProgressWALIterator) field.get(queue);
  }

  private static ConsensusSubscriptionCommitManager newCommitManager(final File systemDir)
      throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(systemDir.getAbsolutePath());
    final Constructor<ConsensusSubscriptionCommitManager> constructor =
        ConsensusSubscriptionCommitManager.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  private static final class TrackingSubscriptionMemoryManager extends SubscriptionMemoryManager {

    private long maximumUsedMemorySizeInBytes;
    private boolean exceededBudget;

    private TrackingSubscriptionMemoryManager(final long totalMemorySizeInBytes) {
      super(totalMemorySizeInBytes);
    }

    @Override
    public synchronized long getUsedMemorySizeInBytes() {
      final long usedMemorySizeInBytes = super.getUsedMemorySizeInBytes();
      maximumUsedMemorySizeInBytes = Math.max(maximumUsedMemorySizeInBytes, usedMemorySizeInBytes);
      exceededBudget |= usedMemorySizeInBytes > getTotalMemorySizeInBytes();
      return usedMemorySizeInBytes;
    }

    private long getMaximumUsedMemorySizeInBytes() {
      return maximumUsedMemorySizeInBytes;
    }

    private boolean hasExceededBudget() {
      return exceededBudget;
    }
  }
}
