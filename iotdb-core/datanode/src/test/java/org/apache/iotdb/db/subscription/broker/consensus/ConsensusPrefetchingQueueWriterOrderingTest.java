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
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALInfoEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALFileVersion;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALMetaData;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.recover.WALRepairWriter;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALByteBufferForTest;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueWriterOrderingTest {
  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private String originalSystemDir;
  private int originalWalEntries;
  private int originalTablets;
  private int originalDelay;
  private final WriterSafeFrontierTracker tracker = new WriterSafeFrontierTracker();
  private final AtomicInteger conversions = new AtomicInteger();
  private final AtomicInteger walReads = new AtomicInteger();
  private ConsensusPrefetchingQueue queue;

  @Before
  public void setUp() throws Exception {
    originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    originalWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    originalTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    originalDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    config.setSubscriptionConsensusBatchMaxWalEntries(1024);
    config.setSubscriptionConsensusBatchMaxTabletCount(512);
    config.setSubscriptionConsensusBatchMaxDelayInMs(0);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSystemDir(temporaryFolder.newFolder("system").getAbsolutePath());
  }

  @After
  public void tearDown() {
    if (queue != null) {
      queue.close();
      assertEquals(0L, queue.getRetainedRequestBytes());
      assertEquals(0L, queue.getRetainedTabletBytes());
    }
    config.setSubscriptionConsensusBatchMaxWalEntries(originalWalEntries);
    config.setSubscriptionConsensusBatchMaxTabletCount(originalTablets);
    config.setSubscriptionConsensusBatchMaxDelayInMs(originalDelay);
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
  }

  @Test
  public void testRecoveredUnknownWriterLogsActualBlockingBarrier() throws Exception {
    final Logger logger = (Logger) LoggerFactory.getLogger(ConsensusPrefetchingQueue.class);
    final Level originalLevel = logger.getLevel();
    final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    logger.setLevel(Level.WARN);
    appender.start();
    logger.addAppender(appender);
    try {
      final File directory = writeWal(8);
      final File data =
          new File(
              directory, WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
      try (FileChannel channel = FileChannel.open(data.toPath(), StandardOpenOption.WRITE)) {
        channel.truncate(channel.size() - 1);
      }
      final WALMetaData recovered;
      try (FileChannel channel = FileChannel.open(data.toPath(), StandardOpenOption.READ)) {
        recovered = WALMetaData.readFromWALFile(data, channel);
      }
      assertTrue(new WALRepairWriter(data).repair(recovered));
      newQueue(walNode(directory, 8), 1024 * 1024);
      queue.setActiveWriterNodeIds(Set.of(7));
      for (int round = 0; round < 5; round++) {
        queue.drivePrefetchOnce();
      }
      assertNull(queue.poll("consumer"));
      final int readsBefore = walReads.get();
      final long bytesBefore = queue.getRetainedTabletBytes();
      final Field iteratorField =
          ConsensusPrefetchingQueue.class.getDeclaredField("subscriptionWALIterator");
      iteratorField.setAccessible(true);
      final ProgressWALIterator iterator = (ProgressWALIterator) iteratorField.get(queue);
      assertNotNull(iterator);
      final String scanStateBefore = iterator.getDiagnosticState();
      final Method report =
          ConsensusPrefetchingQueue.class.getDeclaredMethod(
              "reportReplayStallIfNecessary", long.class);
      report.setAccessible(true);
      final long nowNanos = System.nanoTime();
      report.invoke(queue, nowNanos);
      report.invoke(queue, nowNanos + TimeUnit.SECONDS.toNanos(60));
      final ILoggingEvent warning =
          appender.list.stream()
              .filter(
                  event ->
                      event
                          .getMessage()
                          .equals(
                              DataNodePipeMessages
                                  .LOG_SUBSCRIPTION_REPLAY_STALLED_FOR_ARG_MS_ARG_DIAGNOSTICS_ARG_INSPECT_PREFETCH_WORKER_STACKS_AND_WAL_I_O_CB012F98))
              .findFirst()
              .get();
      final String diagnostic = warning.getFormattedMessage();
      assertTrue(
          diagnostic, diagnostic.contains("lastBlockedFrontier=WriterFrontier{writerNodeId=7"));
      assertTrue(diagnostic, diagnostic.contains("orderingKey=(" + Long.MIN_VALUE + ","));
      assertTrue(diagnostic, diagnostic.contains("barrier=true"));
      // The replicated entry also loses its progress and sorts before the local defaults.
      assertTrue(diagnostic, diagnostic.contains("bufferedHead=(0,-1,-1)"));
      assertTrue(diagnostic, diagnostic.contains("bufferedSearchIndex=-1"));
      assertTrue(diagnostic, diagnostic.contains("bufferedEntryCount=9"));
      assertTrue(diagnostic, diagnostic.contains("admissionBlockReason=WRITER_BACKLOG"));
      assertTrue(diagnostic, diagnostic.contains("retentionContainsUnsupportedProgress=true"));
      assertTrue(diagnostic, diagnostic.contains(directory.toString()));
      // Taking the diagnostic must not consume WAL requests, change ordering, or retain Tablets.
      assertEquals(readsBefore, walReads.get());
      assertEquals(scanStateBefore, iterator.getDiagnosticState());
      assertEquals(bytesBefore, queue.getRetainedTabletBytes());
      assertNull(queue.poll("consumer"));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(originalLevel);
      appender.stop();
    }
  }

  @Test
  public void testBlockedWriterBarrierRetriesLaterAndResumesAfterIdleBarrier() throws Exception {
    newQueue(mock(ConsensusReqReader.class), 1024 * 1024);
    tracker.recordAppliedProgress(2000, 7, 1);
    tracker.observePendingWriterSafeTimeBarrier(1000, 8, 0);
    assertTrue(pending().offer(request(1, 7, 2000)));
    for (int round = 0; round < 100; round++) {
      final PrefetchRoundResult result = queue.drivePrefetchOnce();
      assertEquals(PrefetchRoundResult.Type.RESCHEDULE_LATER, result.getType());
      assertTrue(result.getDelayMs() > 0);
    }
    assertEquals(0, conversions.get());
    assertEquals(0L, queue.getRetainedTabletBytes());
    tracker.observePendingWriterSafeTimeBarrier(3000, 8, 0);
    queue.drivePrefetchOnce();
    assertEvent(7, 2000);
  }

  @Test
  public void testAppliedBarrierCannotHideAnUnreadWriter() throws Exception {
    newQueue(mock(ConsensusReqReader.class), 1024 * 1024);
    installSafeFrontiers(1);
    assertTrue(pending().offer(request(1, 7, 2000)));
    assertEquals(PrefetchRoundResult.Type.RESCHEDULE_LATER, queue.drivePrefetchOnce().getType());
    assertEquals(0, conversions.get());
    assertNull(queue.poll("consumer"));
    assertTrue(pending().offer(request(-1, 8, 1000)));
    queue.drivePrefetchOnce();
    assertEvent(8, 1000);
    queue.drivePrefetchOnce();
    assertEvent(7, 2000);
  }

  @Test
  public void testDefaultPendingBatchBoundaryKeepsWriterOrdering() throws Exception {
    config.setSubscriptionConsensusBatchMaxDelayInMs(50);
    newQueue(mock(ConsensusReqReader.class), 16 * 1024 * 1024);
    installSafeFrontiers(1024);
    for (int index = 1; index <= 1024; index++) {
      assertTrue(pending().offer(request(index, 7, 2000)));
    }
    assertTrue(pending().offer(request(-1, 8, 1000)));
    queue.drivePrefetchOnce();
    assertEvent(8, 1000);
    consumeRemainingWriterSeven(1024);
  }

  @Test
  public void testPendingOrderingFitsOneTabletMemoryBudget() throws Exception {
    newQueue(mock(ConsensusReqReader.class), oneTabletBudget());
    installSafeFrontiers(1024);
    for (int index = 1; index <= 1024; index++) {
      assertTrue(pending().offer(request(index, 7, 2000)));
    }
    assertTrue(pending().offer(request(-1, 8, 1000)));
    queue.drivePrefetchOnce();
    assertEvent(8, 1000);
    consumeRemainingWriterSeven(1024);
  }

  @Test
  public void testWalOrderingReadsOlderWriterBeyondRoundBudgetWithoutTabletBacklog()
      throws Exception {
    final File directory = writeWal(1024);
    final WALNode walNode = walNode(directory, 1024);
    config.setSubscriptionConsensusBatchMaxWalEntries(32);
    newQueue(walNode, oneTabletBudget());
    installSafeFrontiers(1024);
    queue.drivePrefetchOnce();
    assertEquals(0, conversions.get());
    assertEquals(0L, queue.getRetainedTabletBytes());
    SubscriptionEvent first = null;
    for (int round = 0; round < 500 && first == null; round++) {
      final int readsBefore = walReads.get();
      queue.drivePrefetchOnce();
      assertTrue(walReads.get() - readsBefore <= 32);
      first = queue.poll("consumer");
    }
    assertNotNull(first);
    assertEquals(8, first.getCommitContext().getWriterId().getNodeId());
    assertEquals(1000L, first.getCommitContext().getWriterProgress().getPhysicalTime());
    assertTrue(queue.ack("consumer", first.getCommitContext()));
    consumeRemainingWriterSeven(1024);
  }

  @Test
  public void testOrderedReplayResumesAfterSharedWalScanBudgetYields() throws Exception {
    newQueue(walNode(writeWal(1), 1), 1024 * 1024);
    installSafeFrontiers(1);
    final Method driveOrderedReplay =
        ConsensusPrefetchingQueue.class.getDeclaredMethod(
            "driveOrderedWriterReplay",
            long.class,
            int.class,
            int.class,
            long.class,
            int.class,
            ProgressWALIterator.ScanBudget.class);
    driveOrderedReplay.setAccessible(true);
    final ProgressWALIterator.ScanBudget scanBudget =
        new ProgressWALIterator.ScanBudget(1, Long.MAX_VALUE);
    final PrefetchRoundResult result =
        (PrefetchRoundResult)
            driveOrderedReplay.invoke(queue, 0L, 1024, 512, 16 * 1024 * 1024L, 0, scanBudget);

    assertTrue(scanBudget.hasYielded());
    assertEquals(PrefetchRoundResult.Type.RESCHEDULE_NOW, result.getType());
    // A single physical entry remains buffered until the next writer request establishes its end.
    assertEquals(0, walReads.get());
    assertEquals(0, conversions.get());
    assertEquals(0L, queue.getRetainedTabletBytes());
    assertNull(queue.poll("consumer"));

    for (int round = 0; round < 20 && queue.getPrefetchedEventCount() == 0; round++) {
      queue.drivePrefetchOnce();
    }
    assertEvent(8, 1000);
    consumeRemainingWriterSeven(1);
    assertEquals(2, conversions.get());
  }

  @Test
  public void testDeactivationReleasesRawWriterHeads() throws Exception {
    newQueue(walNode(writeWal(1), 1), oneTabletBudget());
    tracker.recordAppliedProgress(2000, 7, 1);
    tracker.observePendingWriterSafeTimeBarrier(500, 8, 0);
    queue.drivePrefetchOnce();
    assertTrue(queue.getRetainedRequestBytes() > 0);
    assertEquals(1, conversions.get());
    queue.setActive(false);
    assertEquals(0L, queue.getRetainedRequestBytes());
    assertEquals(0L, queue.getRetainedTabletBytes());
  }

  @Test
  public void testAppliedWriterWaitsForWalVisibilityAndResumesOnRefresh() throws Exception {
    final File directory = temporaryFolder.newFolder("delayed-wal");
    final WALNode walNode = walNode(directory, 1);
    newQueue(walNode, oneTabletBudget());
    installSafeFrontiers(1);
    assertTrue(pending().offer(request(1, 7, 2000)));
    for (int round = 0; round < 10; round++) {
      assertEquals(PrefetchRoundResult.Type.RESCHEDULE_LATER, queue.drivePrefetchOnce().getType());
    }
    assertEquals(0, conversions.get());
    assertEquals(0L, queue.getRetainedTabletBytes());
    final File data =
        new File(directory, WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
    try (WALWriter writer = new WALWriter(data, WALFileVersion.V3)) {
      writeEntry(writer, 1, 7, 2000, 1);
      writeEntry(writer, -1, 8, 1000, 1);
    }
    try (WALWriter ignored =
        new WALWriter(
            new File(
                directory, WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX)),
            WALFileVersion.V3)) {
      // The independent readers must refresh after previously exhausting an empty directory.
    }
    queue.drivePrefetchOnce();
    assertEvent(8, 1000);
    queue.drivePrefetchOnce();
    assertEvent(7, 2000);
    assertNull(queue.poll("consumer"));
  }

  @Test
  public void testWalAndPendingCopiesAreMaterializedOnce() throws Exception {
    newQueue(walNode(writeWal(2), 2), 1024 * 1024);
    installSafeFrontiers(2);
    assertTrue(pending().offer(request(1, 7, 2000)));
    assertTrue(pending().offer(request(2, 7, 2000)));
    assertTrue(pending().offer(request(-1, 8, 1000)));
    queue.drivePrefetchOnce();
    assertEvent(8, 1000);
    consumeRemainingWriterSeven(2);
    for (int round = 0; round < 10; round++) {
      queue.drivePrefetchOnce();
    }
    assertEquals(3, conversions.get());
    assertEquals(0, queue.getPendingEventCount());
    assertEquals(0, queue.getPrefetchedEventCount());
  }

  @Test
  public void testOrderedReadersReportBrokenWalFileOnlyOnce() throws Exception {
    final File directory = temporaryFolder.newFolder("broken-wal");
    final File broken =
        new File(directory, WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
    Files.write(broken.toPath(), new byte[128]);
    try (WALWriter writer =
        new WALWriter(
            new File(
                directory, WALFileUtils.getLogFileName(1, 1, WALFileStatus.CONTAINS_SEARCH_INDEX)),
            WALFileVersion.V3)) {
      writeEntry(writer, 2, 7, 2000, 2);
      writeEntry(writer, -1, 8, 1000, 1);
    }
    try (WALWriter ignored =
        new WALWriter(
            new File(
                directory, WALFileUtils.getLogFileName(2, 2, WALFileStatus.CONTAINS_SEARCH_INDEX)),
            WALFileVersion.V3)) {
      // Keep both the unreadable prefix and the intact data file historical.
    }
    final WALNode walNode = walNode(directory, 2);
    when(walNode.getCurrentWALFileVersion()).thenReturn(2L);
    newQueue(walNode, 1024 * 1024);
    installSafeFrontiers(2);
    // Retention initialization may recover an unreadable footer. Corrupt the sealed prefix after
    // initialization to exercise the subscription readers' error reporting, rather than recovery.
    Files.write(broken.toPath(), new byte[128]);
    queue.drivePrefetchOnce();
    final SubscriptionEvent error = queue.poll("consumer");
    assertNotNull(error);
    assertEquals(
        SubscriptionPollResponseType.ERROR.getType(), error.getCurrentResponse().getResponseType());
    assertEquals(1L, queue.getUnreadableWalFileCount());
    queue.drivePrefetchOnce();
    assertEvent(8, 1000);
    queue.drivePrefetchOnce();
    assertEvent(7, 2000);
    queue.drivePrefetchOnce();
    assertEquals(1L, queue.getUnreadableWalFileCount());
    assertEquals(1L, queue.getWalGapSkippedEntries());
    assertNull(queue.poll("consumer"));
  }

  private void consumeRemainingWriterSeven(final int expectedCount) throws Exception {
    int tablets = 0;
    long lastSeq = 0;
    for (int round = 0; round < expectedCount * 4 + 100 && tablets < expectedCount; round++) {
      final int readsBefore = walReads.get();
      queue.drivePrefetchOnce();
      assertTrue(
          walReads.get() - readsBefore <= config.getSubscriptionConsensusBatchMaxWalEntries());
      SubscriptionEvent event;
      while ((event = queue.poll("consumer")) != null) {
        assertEquals(7, event.getCommitContext().getWriterId().getNodeId());
        assertEquals(2000L, event.getCommitContext().getWriterProgress().getPhysicalTime());
        assertTrue(event.getCommitContext().getWriterProgress().getLocalSeq() > lastSeq);
        lastSeq = event.getCommitContext().getWriterProgress().getLocalSeq();
        tablets += ((TabletsPayload) event.getCurrentResponse().getPayload()).getTablets().size();
        assertTrue(queue.ack("consumer", event.getCommitContext()));
      }
      assertTrue(queue.getRetainedTabletBytes() <= queue.getSubscriptionMemoryLimitInBytes());
      if (tablets < expectedCount && config.getSubscriptionConsensusBatchMaxDelayInMs() > 0) {
        Thread.sleep(60);
      }
    }
    assertEquals(expectedCount, tablets);
    assertEquals(expectedCount, lastSeq);
    assertEquals(0L, queue.getRetainedTabletBytes());
  }

  private void assertEvent(final int writerNodeId, final long physicalTime) {
    final SubscriptionEvent event = queue.poll("consumer");
    assertNotNull(queue.coreReportMessage().toString(), event);
    assertEquals(writerNodeId, event.getCommitContext().getWriterId().getNodeId());
    assertEquals(physicalTime, event.getCommitContext().getWriterProgress().getPhysicalTime());
    assertTrue(queue.ack("consumer", event.getCommitContext()));
  }

  private void installSafeFrontiers(final long writerSevenSeq) {
    tracker.recordAppliedProgress(2000, 7, writerSevenSeq);
    tracker.recordAppliedProgress(1000, 8, 1);
    tracker.observePendingWriterSafeTimeBarrier(3000, 7, writerSevenSeq);
    tracker.observePendingWriterSafeTimeBarrier(3000, 8, 1);
  }

  private void newQueue(final ConsensusReqReader reader, final long memoryBytes) {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getConsensusReqReader()).thenReturn(reader);
    when(server.getWriterSafeFrontierTracker()).thenReturn(tracker);
    final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
    when(converter.getDatabaseName()).thenReturn("db");
    when(converter.convert(any()))
        .thenAnswer(
            ignored -> {
              conversions.incrementAndGet();
              return Collections.singletonList(tablet());
            });
    queue =
        new ConsensusPrefetchingQueue(
            "consumerGroup",
            "topic",
            TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE,
            new DataRegionId(1),
            server,
            new SubscriptionWalRetentionPolicy(
                "topic",
                SubscriptionWalRetentionPolicy.UNBOUNDED,
                SubscriptionWalRetentionPolicy.UNBOUNDED),
            converter,
            new ConsensusSubscriptionCommitManager(
                (group, topic, id) ->
                    ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent()),
            new RegionProgress(Collections.emptyMap()),
            1,
            1,
            true) {
          @Override
          protected ProgressWALIterator createSubscriptionWALIterator(final long startSearchIndex) {
            final ProgressWALIterator iterator =
                super.createSubscriptionWALIterator(startSearchIndex);
            if (iterator == null) {
              return null;
            }
            final ProgressWALIterator tracked = spy(iterator);
            doAnswer(
                    invocation -> {
                      walReads.incrementAndGet();
                      return invocation.callRealMethod();
                    })
                .when(tracked)
                .next();
            return tracked;
          }
        };
    queue.setSubscriptionMemoryManager(new SubscriptionMemoryManager(memoryBytes));
    queue.setActiveWriterNodeIds(Set.of(7, 8));
    assertNull(queue.poll("consumer"));
  }

  private static long oneTabletBudget() {
    final long bytes = PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet());
    return bytes + bytes / 2;
  }

  private static Tablet tablet() {
    final Tablet tablet =
        new Tablet("root.test.device", List.of(new MeasurementSchema("s", TSDataType.INT64)), 1);
    tablet.addTimestamp(0, 1);
    tablet.addValue("s", 0, 1L);
    tablet.setRowSize(1);
    return tablet;
  }

  private static IndexedConsensusRequest request(
      final long searchIndex, final int writerNodeId, final long physicalTime) {
    return new IndexedConsensusRequest(
            searchIndex,
            writerNodeId == 8 ? 1 : searchIndex,
            Collections.singletonList(StatementTestUtils.genInsertRowNode(1)))
        .setNodeId(writerNodeId)
        .setPhysicalTime(physicalTime);
  }

  @SuppressWarnings("unchecked")
  private BlockingQueue<IndexedConsensusRequest> pending() throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }

  private static WALNode walNode(final File directory, final long currentIndex) {
    final WALNode walNode = mock(WALNode.class);
    when(walNode.getLogDirectory()).thenReturn(directory);
    when(walNode.getCurrentSearchIndex()).thenReturn(currentIndex);
    when(walNode.getCurrentWALFileVersion()).thenReturn(1L);
    when(walNode.getWALMetaDataSnapshot(1L)).thenReturn(new WALMetaData());
    return walNode;
  }

  private File writeWal(final int localCount) throws Exception {
    final File directory = temporaryFolder.newFolder("wal");
    final File data =
        new File(directory, WALFileUtils.getLogFileName(0, 0, WALFileStatus.CONTAINS_SEARCH_INDEX));
    try (WALWriter writer = new WALWriter(data, WALFileVersion.V3)) {
      for (int index = 1; index <= localCount; index++) {
        writeEntry(writer, index, 7, 2000, index);
      }
      writeEntry(writer, -1, 8, 1000, 1);
    }
    try (WALWriter ignored =
        new WALWriter(
            new File(
                directory,
                WALFileUtils.getLogFileName(1, localCount, WALFileStatus.CONTAINS_SEARCH_INDEX)),
            WALFileVersion.V3)) {
      // Seal the data file while keeping the live successor empty.
    }
    return directory;
  }

  private static void writeEntry(
      final WALWriter writer,
      final long searchIndex,
      final int writerNodeId,
      final long physicalTime,
      final long localSeq)
      throws Exception {
    final RelationalInsertRowNode node = StatementTestUtils.genInsertRowNode(1);
    node.setSearchIndex(searchIndex);
    node.setLastFragment(true);
    final WALInfoEntry entry = new WALInfoEntry(1, node);
    final ByteBuffer buffer = ByteBuffer.allocate(entry.serializedSize());
    entry.serialize(new WALByteBufferForTest(buffer));
    final WALMetaData metadata = new WALMetaData();
    metadata.add(entry.serializedSize(), searchIndex, 1, physicalTime, writerNodeId, localSeq);
    writer.write(buffer, metadata);
  }
}
