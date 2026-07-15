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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

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
      final IndexedConsensusRequest emptyRequest =
          new IndexedConsensusRequest(
                  2L, Collections.singletonList(StatementTestUtils.genInsertRowNode(2)))
              .setPhysicalTime(1001L)
              .setNodeId(7);
      reader.currentSearchIndex = 2L;

      assertNull(queue.poll("consumer"));
      pendingEntries(queue).offer(dataRequest);
      pendingEntries(queue).offer(emptyRequest);
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

  @SuppressWarnings("unchecked")
  private static BlockingQueue<IndexedConsensusRequest> pendingEntries(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
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
