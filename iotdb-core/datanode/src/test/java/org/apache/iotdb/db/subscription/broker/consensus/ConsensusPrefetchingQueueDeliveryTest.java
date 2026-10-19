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
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
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
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueDeliveryTest {

  private static final int WRITER = 7;
  private static final String CONSUMER = "consumer";
  private static final DataRegionId REGION = new DataRegionId(1);

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final List<ConsensusPrefetchingQueue> queues = new ArrayList<>();
  private final AtomicLong walIteratorCreations = new AtomicLong();
  private String originalSystemDir;
  private int originalMaxDelay;
  private int originalMaxTablets;
  private int originalMaxEntries;
  private long originalMaxBytes;
  private File walDirectory;
  private WALNode walNode;
  private ConsensusLogToTabletConverter converter;
  private ConsensusSubscriptionCommitManager commitManager;
  private SubscriptionMemoryManager memoryManager;
  private WriterSafeFrontierTracker writerSafeFrontierTracker;
  private AtomicLong currentSearchIndex;
  private long tabletBytes;

  @Before
  public void setUp() throws Exception {
    originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    originalMaxDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    originalMaxTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    originalMaxEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    originalMaxBytes = config.getSubscriptionConsensusBatchMaxSizeInBytes();
    config.setSubscriptionConsensusBatchMaxDelayInMs(0);
    config.setSubscriptionConsensusBatchMaxTabletCount(1);
    config.setSubscriptionConsensusBatchMaxWalEntries(128);
    config.setSubscriptionConsensusBatchMaxSizeInBytes(16 * 1024 * 1024);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSystemDir(temporaryFolder.newFolder("system").getAbsolutePath());
    walDirectory = temporaryFolder.newFolder("wal");
    currentSearchIndex = new AtomicLong();
    walNode = mock(WALNode.class);
    when(walNode.getLogDirectory()).thenReturn(walDirectory);
    when(walNode.getCurrentSearchIndex()).thenAnswer(ignored -> currentSearchIndex.get());
    when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
    when(walNode.getCurrentWALMetaDataSnapshot()).thenReturn(new WALMetaData());
    when(walNode.getSortedWalFilesSnapshot())
        .thenAnswer(
            ignored -> {
              final File[] files = WALFileUtils.listAllWALFiles(walDirectory);
              WALFileUtils.ascSortByVersionId(files);
              return files;
            });
    converter = mock(ConsensusLogToTabletConverter.class);
    when(converter.getDatabaseName()).thenReturn("db");
    when(converter.isTableModel()).thenReturn(true);
    when(converter.convert(any()))
        .thenAnswer(
            invocation ->
                Collections.singletonList(
                    tablet(((InsertNode) invocation.getArgument(0)).getPhysicalTime())));
    tabletBytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet(1001));
    memoryManager = new SubscriptionMemoryManager(tabletBytes * 2);
    commitManager =
        new ConsensusSubscriptionCommitManager(
            (group, topic, region) ->
                ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent());
  }

  @After
  public void tearDown() {
    try {
      queues.forEach(ConsensusPrefetchingQueue::close);
    } finally {
      config.setSubscriptionConsensusBatchMaxDelayInMs(originalMaxDelay);
      config.setSubscriptionConsensusBatchMaxTabletCount(originalMaxTablets);
      config.setSubscriptionConsensusBatchMaxWalEntries(originalMaxEntries);
      config.setSubscriptionConsensusBatchMaxSizeInBytes(originalMaxBytes);
      IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
    }
  }

  @Test
  public void testTenDeliveriesWithTwoBatchBudgetAndDelayedCommit() throws Exception {
    writeWal(10, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    final List<SubscriptionCommitContext> contexts = new ArrayList<>();
    for (long index = 1; index <= 10; index++) {
      queue.drivePrefetchOnce();
      final SubscriptionEvent event = queue.poll(CONSUMER);
      assertNotNull(event);
      assertEquals(index, event.getCommitContext().getWriterProgress().getLocalSeq());
      assertEquals(Collections.singletonList(1000 + index), timestamps(deliver(event)));
      assertTrue(event.isPayloadDetached());
      assertTrue(((TabletsPayload) event.getCurrentResponse().getPayload()).getTablets().isEmpty());
      assertThrows(IOException.class, event::getCurrentResponseByteBuffer);
      contexts.add(event.getCommitContext());
      assertTrue(memoryManager.getUsedMemorySizeInBytes() <= tabletBytes * 2);
      assertEquals(queue.getRetainedTabletBytes(), memoryManager.getUsedMemorySizeInBytes());
      assertTrue(committedProgress().getWriterPositions().isEmpty());
      assertEquals(index, queue.getSubscriptionUncommittedEventCount());
      assertEquals(0L, requiredRetainedVersion(queue));
    }

    // An out-of-order ACK still waits for the preceding nine batches.
    assertTrue(queue.ack(CONSUMER, contexts.get(9)));
    assertTrue(committedProgress().getWriterPositions().isEmpty());
    for (int index = 0; index < 9; index++) {
      assertTrue(queue.ack(CONSUMER, contexts.get(index)));
    }
    assertEquals(
        new WriterProgress(1010, 10), committedProgress().getWriterPositions().get(writerId()));
    assertEquals(0L, queue.getSubscriptionUncommittedEventCount());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertEquals(1L, requiredRetainedVersion(queue));
    assertEquals(0, deliveryRecordCount(queue));
  }

  @Test
  public void testWalDeliveryReadsOnlyForRedelivery() throws Exception {
    writeWal(1, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final long initialReads = walIteratorCreations.get();
    final SubscriptionPollResponse first = deliver(event);
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertEquals(initialReads, walIteratorCreations.get());

    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(initialReads + 1, walIteratorCreations.get());
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertTrue(event.isPayloadDetached());
    // Restoration reads WAL once; delivering the restored response needs no validation scan.
    assertEquals(initialReads + 1, walIteratorCreations.get());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testPendingDeliveryReleasesPayloadWithoutWalValidation() throws Exception {
    writeWal(1, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    assertTrue(pendingEntries(queue).offer(request(1, 1, WRITER)));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final long initialReads = walIteratorCreations.get();
    deliver(event);
    assertEquals(initialReads, walIteratorCreations.get());
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());

    queue.drivePrefetchOnce();
    assertEquals(initialReads, walIteratorCreations.get());
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testMixedPendingAndWalBatchRetriesRedeliveryUntilAllMembersAreReadable()
      throws Exception {
    config.setSubscriptionConsensusBatchMaxTabletCount(2);
    writeWal(1, true);
    currentSearchIndex.set(2);
    // Keep the mixed batch deterministic when first-use class loading exhausts a scan slice.
    // Bounded scan yielding is covered by ConsensusPrefetchingQueueWalBackpressureTest.
    final ConsensusPrefetchingQueue queue = newQueue(true);
    assertNull(queue.poll(CONSUMER));
    assertTrue(pendingEntries(queue).offer(request(2, 2, WRITER)));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    assertNotNull(event);
    final long initialReads = walIteratorCreations.get();
    final SubscriptionPollResponse first = deliver(event);
    assertEquals(Arrays.asList(1001L, 1002L), timestamps(first));
    assertTrue(event.isPayloadDetached());
    queue.drivePrefetchOnce();
    assertEquals(initialReads, walIteratorCreations.get());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    assertNull(queue.poll(CONSUMER));
    assertTrue(event.isPayloadDetached());
    assertEquals(1, deliveryRecordCount(queue));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());

    writeWal(2, true);
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testLingerBatchRemainsReplayableAfterAddingActiveWriter() throws Exception {
    config.setSubscriptionConsensusBatchMaxTabletCount(2);
    config.setSubscriptionConsensusBatchMaxDelayInMs(60_000);
    writeWal(2, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    queue.setOrderMode(TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE);
    queue.setActiveWriterNodeIds(Collections.singleton(WRITER));
    assertNull(queue.poll(CONSUMER));
    assertTrue(pendingEntries(queue).offer(request(1, 1, WRITER)));
    assertTrue(pendingEntries(queue).offer(request(2, 2, WRITER)));
    queue.drivePrefetchOnce();
    assertNull(queue.poll(CONSUMER));
    assertEquals(tabletBytes * 2, queue.getRetainedTabletBytes());

    writerSafeFrontierTracker.recordAppliedProgress(1002, WRITER, 2);
    writerSafeFrontierTracker.observePendingWriterSafeTimeBarrier(2000, WRITER, 2);
    writerSafeFrontierTracker.observePendingWriterSafeTimeBarrier(2000, 8, 0);
    queue.setActiveWriterNodeIds(Set.of(WRITER, 8));
    config.setSubscriptionConsensusBatchMaxDelayInMs(0);
    for (int round = 0; round < 20 && queue.getPrefetchedEventCount() == 0; round++) {
      queue.drivePrefetchOnce();
    }
    final SubscriptionEvent event = queue.poll(CONSUMER);
    assertNotNull(event);
    final SubscriptionPollResponse first = deliver(event);
    assertEquals(Arrays.asList(1001L, 1002L), timestamps(first));
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());

    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testNackTimeoutDisconnectAndLateAckAfterReclamation() throws Exception {
    writeWal(1, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final SubscriptionCommitContext context = event.getCommitContext();
    final SubscriptionPollResponse first = deliver(event);
    assertEquals(0L, queue.getRetainedTabletBytes());

    assertTrue(queue.nack(CONSUMER, context));
    assertSame(event, queue.poll(CONSUMER));
    assertFalse(event.isPayloadDetached());
    assertEquals(tabletBytes, queue.getRetainedTabletBytes());
    final SubscriptionPollResponse second = deliver(event);
    assertEquals(first.getPayload(), second.getPayload());
    assertEquals(first.getCommitContext(), second.getCommitContext());

    // Force the timeout without sleeping or changing the process-wide recycle interval.
    final Field timestamp = SubscriptionEvent.class.getDeclaredField("lastPolledTimestamp");
    timestamp.setAccessible(true);
    ((AtomicLong) timestamp.get(event)).set(0L);
    queue.drivePrefetchOnce();
    assertSame(event, queue.poll("replacement"));
    assertEquals(first.getPayload(), deliver(event).getPayload());

    assertEquals(1, queue.requeueInFlightEvents("replacement"));
    assertSame(event, queue.poll("replacement"));
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertEquals(1, queue.requeueInFlightEvents("replacement"));
    // A late ACK may commit a queued, detached event without reading WAL again.
    assertTrue(queue.ack("replacement", context));
    assertNull(queue.poll(CONSUMER));
    assertEquals(0, deliveryRecordCount(queue));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
  }

  @Test
  public void testIncompleteOrNotYetVisibleWalRetriesRedeliveryWithoutRetainingPayload()
      throws Exception {
    currentSearchIndex.set(1);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    assertTrue(pendingEntries(queue).offer(request(1, 1, WRITER)));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final long initialReads = walIteratorCreations.get();
    final SubscriptionPollResponse first = deliver(event);
    assertEquals(initialReads, walIteratorCreations.get());
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    assertNull(queue.poll(CONSUMER));
    assertEquals(1, deliveryRecordCount(queue));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());

    writeWal(1, false);
    assertNull(queue.poll(CONSUMER));
    assertTrue(event.isPayloadDetached());
    assertEquals(1, deliveryRecordCount(queue));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());

    writeWal(1, true);
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testUnreadableReplayKeepsRecordAndRetriesWithoutAdvancingCommit() throws Exception {
    writeWal(1, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final SubscriptionPollResponse first = deliver(event);
    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    final File hidden = new File(temporaryFolder.getRoot(), "hidden.wal");
    Files.move(dataWal().toPath(), hidden.toPath());
    assertNull(queue.poll(CONSUMER));
    assertTrue(event.isPayloadDetached());
    assertEquals(1, deliveryRecordCount(queue));
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertTrue(committedProgress().getWriterPositions().isEmpty());
    Files.move(hidden.toPath(), dataWal().toPath());
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testSparseBatchReplaysOnlyItsOriginalMembers() throws Exception {
    config.setSubscriptionConsensusBatchMaxTabletCount(8);
    config.setSubscriptionConsensusBatchMaxDelayInMs(60_000);
    memoryManager = new SubscriptionMemoryManager(tabletBytes * 8);
    writeWal(3, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    // Build a batch with members 1 and 3, as writer reordering can do for interleaved WAL records.
    // Replaying the inclusive [1, 3] range would silently add entry 2 to the redelivery.
    final Field linger = ConsensusPrefetchingQueue.class.getDeclaredField("lingerBatch");
    linger.setAccessible(true);
    final Object batch = linger.get(queue);
    final Class<?> entryClass =
        Class.forName(ConsensusPrefetchingQueue.class.getName() + "$PreparedEntry");
    final Constructor<?> constructor =
        entryClass.getDeclaredConstructor(
            List.class, long.class, long.class, int.class, long.class, long.class);
    constructor.setAccessible(true);
    final Method append =
        batch
            .getClass()
            .getDeclaredMethod(
                "append",
                Class.forName(ConsensusPrefetchingQueue.class.getName() + "$WriterBufferedEntry"),
                long.class,
                boolean.class);
    append.setAccessible(true);
    final Method reserve =
        ConsensusPrefetchingQueue.class.getDeclaredMethod("tryReserveTabletMemory", long.class);
    reserve.setAccessible(true);
    for (final long index : new long[] {1, 3}) {
      assertTrue((boolean) reserve.invoke(queue, tabletBytes));
      append.invoke(
          batch,
          constructor.newInstance(
              Collections.singletonList(tablet(1000 + index)),
              tabletBytes,
              1000 + index,
              WRITER,
              index,
              index),
          tabletBytes,
          true);
    }
    final Method flush =
        ConsensusPrefetchingQueue.class.getDeclaredMethod(
            "flushBatch", batch.getClass(), long.class);
    flush.setAccessible(true);
    assertTrue((boolean) flush.invoke(queue, batch, queue.getCurrentSeekGeneration()));
    // Manual batch construction bypasses the WAL pump. Advance its local cursor past the consumed
    // range so normal prefetch does not enqueue it again.
    final Field cursor =
        ConsensusPrefetchingQueue.class.getDeclaredField("nextExpectedSearchIndex");
    cursor.setAccessible(true);
    ((AtomicLong) cursor.get(queue)).set(4L);
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final SubscriptionPollResponse first = deliver(event);
    assertEquals(Arrays.asList(1001L, 1003L), timestamps(first));
    queue.drivePrefetchOnce();
    assertTrue(event.isPayloadDetached());
    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(first.getPayload(), deliver(event).getPayload());
  }

  @Test
  public void testRestartReplaysAllUncommittedBatchesFromCommittedProgress() throws Exception {
    writeWal(10, true);
    ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    for (int index = 1; index <= 10; index++) {
      queue.drivePrefetchOnce();
      final SubscriptionEvent event = queue.poll(CONSUMER);
      deliver(event);
      if (index <= 2) {
        assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
      }
    }
    final RegionProgress committed = committedProgress();
    commitManager.persistAll();
    queue.close();
    assertEquals(0, deliveryRecordCount(queue));

    commitManager =
        new ConsensusSubscriptionCommitManager(
            (group, topic, region) ->
                ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.unavailable());
    queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    final List<Long> received = new ArrayList<>();
    for (int index = 3; index <= 10; index++) {
      queue.drivePrefetchOnce();
      final SubscriptionEvent event = queue.poll(CONSUMER);
      assertNotNull(event);
      received.addAll(timestamps(deliver(event)));
      assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
    }
    assertEquals(Arrays.asList(1003L, 1004L, 1005L, 1006L, 1007L, 1008L, 1009L, 1010L), received);
    assertEquals(new WriterProgress(1002, 2), committed.getWriterPositions().get(writerId()));
    assertEquals(
        new WriterProgress(1010, 10), committedProgress().getWriterPositions().get(writerId()));
  }

  @Test
  public void testReplicatedEntryWithoutLocalIndexReplaysByWriterPosition() throws Exception {
    writeWal(1, true, false);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final SubscriptionPollResponse first = deliver(event);
    assertTrue(event.isPayloadDetached());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertEquals(0L, requiredRetainedVersion(queue));
    assertTrue(queue.nack(CONSUMER, event.getCommitContext()));
    assertSame(event, queue.poll(CONSUMER));
    assertEquals(first.getPayload(), deliver(event).getPayload());
    assertTrue(queue.ack(CONSUMER, event.getCommitContext()));
  }

  @Test
  public void testSeekResetDiscardsDetachedStateAndRejectsOldCallbacksAndAcks() throws Exception {
    writeWal(1, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final SubscriptionPollResponse oldResponse = event.getCurrentResponse();
    deliver(event);
    assertTrue(event.isPayloadDetached());

    // Install the request a public seek hands to the serial worker. Drive it synchronously so the
    // test does not depend on a process-wide executor being enabled.
    final long oldGeneration = queue.getCurrentSeekGeneration();
    final Class<?> requestClass =
        Class.forName(ConsensusPrefetchingQueue.class.getName() + "$PendingSeekRequest");
    final Constructor<?> constructor =
        requestClass.getDeclaredConstructor(
            long.class, RegionProgress.class, String.class, boolean.class, long.class, long.class);
    constructor.setAccessible(true);
    final Field generation = ConsensusPrefetchingQueue.class.getDeclaredField("seekGeneration");
    generation.setAccessible(true);
    ((AtomicLong) generation.get(queue)).incrementAndGet();
    final Field pendingSeek =
        ConsensusPrefetchingQueue.class.getDeclaredField("pendingSeekRequest");
    pendingSeek.setAccessible(true);
    pendingSeek.set(
        queue,
        constructor.newInstance(
            1L,
            new RegionProgress(Collections.emptyMap()),
            "beginning",
            true,
            oldGeneration,
            oldGeneration + 1));
    queue.drivePrefetchOnce();

    assertEquals(0, deliveryRecordCount(queue));
    assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    event.markResponseDelivered(oldResponse);
    assertFalse(queue.ack(CONSUMER, event.getCommitContext()));
    queue.drivePrefetchOnce();
    final SubscriptionEvent replayed = queue.poll(CONSUMER);
    assertEquals(Collections.singletonList(1001L), timestamps(deliver(replayed)));
    assertTrue(queue.ack(CONSUMER, replayed.getCommitContext()));
  }

  @Test
  public void testLateDeliveryCallbackCannotDetachAnotherAttemptAndCloseWaitsForSerialization()
      throws Exception {
    writeWal(1, true);
    final ConsensusPrefetchingQueue queue = newQueue();
    assertNull(queue.poll(CONSUMER));
    queue.drivePrefetchOnce();
    final SubscriptionEvent event = queue.poll(CONSUMER);
    final SubscriptionPollResponse oldResponse = event.getCurrentResponse();
    event.getResponseByteBuffer(oldResponse);
    assertTrue(queue.requeue(CONSUMER, event.getCommitContext()));
    assertSame(event, queue.poll(CONSUMER));
    event.markResponseDelivered(oldResponse);
    assertFalse(event.isPayloadDetached());
    assertEquals(tabletBytes, queue.getRetainedTabletBytes());

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch started = new CountDownLatch(1);
    try {
      final Future<?> cleanup;
      synchronized (event) {
        cleanup =
            executor.submit(
                () -> {
                  started.countDown();
                  queue.cleanUp();
                });
        assertTrue(started.await(5, TimeUnit.SECONDS));
        assertFalse(cleanup.isDone());
        assertNotNull(event.getCurrentResponseByteBuffer());
      }
      cleanup.get(5, TimeUnit.SECONDS);
      event.markResponseDelivered(oldResponse);
      assertEquals(0, deliveryRecordCount(queue));
      assertEquals(0L, memoryManager.getUsedMemorySizeInBytes());
    } finally {
      executor.shutdownNow();
    }
  }

  private ConsensusPrefetchingQueue newQueue() {
    return newQueue(false);
  }

  private ConsensusPrefetchingQueue newQueue(final boolean unboundedWalFixture) {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getConsensusReqReader()).thenReturn(walNode);
    writerSafeFrontierTracker = new WriterSafeFrontierTracker();
    when(server.getWriterSafeFrontierTracker()).thenReturn(writerSafeFrontierTracker);
    final ConsensusPrefetchingQueue queue =
        new ConsensusPrefetchingQueue(
            "group",
            "topic",
            TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE,
            REGION,
            server,
            new SubscriptionWalRetentionPolicy(
                "topic",
                SubscriptionWalRetentionPolicy.UNBOUNDED,
                SubscriptionWalRetentionPolicy.UNBOUNDED),
            converter,
            commitManager,
            new RegionProgress(Collections.emptyMap()),
            1L,
            1L,
            true) {
          @Override
          protected ProgressWALIterator createSubscriptionWALIterator(final long startSearchIndex) {
            walIteratorCreations.incrementAndGet();
            if (unboundedWalFixture) {
              return new ProgressWALIterator(walNode, startSearchIndex) {
                @Override
                boolean hasNext(final ScanBudget scanBudget) {
                  return super.hasNext(null);
                }
              };
            }
            return super.createSubscriptionWALIterator(startSearchIndex);
          }
        };
    queue.setSubscriptionMemoryManager(memoryManager);
    queues.add(queue);
    return queue;
  }

  private void writeWal(final int count, final boolean complete) throws Exception {
    writeWal(count, complete, true);
  }

  private void writeWal(final int count, final boolean complete, final boolean local)
      throws Exception {
    currentSearchIndex.set(local ? count : 0);
    // WALWriter appends to existing files; replacing a visibility fixture needs a fresh segment.
    Files.deleteIfExists(dataWal().toPath());
    try (WALWriter writer = new WALWriter(dataWal(), WALFileVersion.V3)) {
      for (long index = 1; index <= count; index++) {
        final RelationalInsertRowNode node = StatementTestUtils.genInsertRowNode((int) index);
        node.setSearchIndex(local ? index : -1L);
        node.setLastFragment(complete);
        final WALInfoEntry entry = new WALInfoEntry(1L, node);
        final ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
        entry.serialize(new WALByteBufferForTest(buffer));
        final WALMetaData metadata = new WALMetaData();
        metadata.add(entry.serializedSize(), local ? index : -1L, 1L, 1000 + index, WRITER, index);
        writer.write(buffer, metadata);
      }
    }
    try (WALWriter ignored =
        new WALWriter(
            new File(
                walDirectory,
                WALFileUtils.getLogFileName(1L, count, WALFileStatus.CONTAINS_SEARCH_INDEX)),
            WALFileVersion.V3)) {
      // The data file is sealed; the live successor contains no requests.
    }
  }

  private File dataWal() {
    return new File(
        walDirectory, WALFileUtils.getLogFileName(0L, 0L, WALFileStatus.CONTAINS_SEARCH_INDEX));
  }

  private static IndexedConsensusRequest request(
      final long index, final long sequence, final int writer) {
    return new IndexedConsensusRequest(
            index,
            sequence,
            Collections.singletonList(StatementTestUtils.genInsertRowNode((int) sequence)))
        .setPhysicalTime(1000 + sequence)
        .setNodeId(writer);
  }

  private static Tablet tablet(final long timestamp) {
    final Tablet tablet =
        new Tablet(
            "sensors",
            Arrays.asList("device", "temperature"),
            Arrays.asList(TSDataType.STRING, TSDataType.DOUBLE),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue(0, 0, "d1");
    tablet.addValue(0, 1, (double) timestamp);
    tablet.setRowSize(1);
    return tablet;
  }

  private static SubscriptionPollResponse deliver(final SubscriptionEvent event) throws Exception {
    final SubscriptionPollResponse response = event.getCurrentResponse();
    final ByteBuffer bytes = event.getResponseByteBuffer(response);
    event.invalidateCurrentResponseByteBuffer();
    event.markResponseDelivered(response);
    return SubscriptionPollResponse.deserialize(bytes.duplicate());
  }

  private static List<Long> timestamps(final SubscriptionPollResponse response) {
    final List<Long> result = new ArrayList<>();
    for (final Tablet tablet : ((TabletsPayload) response.getPayload()).getTablets()) {
      result.add(tablet.getTimestamps()[0]);
    }
    return result;
  }

  private RegionProgress committedProgress() {
    return commitManager.getCommittedRegionProgress("group", "topic", REGION);
  }

  private static WriterId writerId() {
    return new WriterId(REGION.toString(), WRITER);
  }

  private static long requiredRetainedVersion(final ConsensusPrefetchingQueue queue)
      throws Exception {
    final Method method =
        ConsensusPrefetchingQueue.class.getDeclaredMethod("getRequiredRetainedMinVersionId");
    method.setAccessible(true);
    return (long) method.invoke(queue);
  }

  private static int deliveryRecordCount(final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("deliveryBatches");
    field.setAccessible(true);
    return ((java.util.Map<?, ?>) field.get(queue)).size();
  }

  @SuppressWarnings("unchecked")
  private static BlockingQueue<IndexedConsensusRequest> pendingEntries(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }
}
