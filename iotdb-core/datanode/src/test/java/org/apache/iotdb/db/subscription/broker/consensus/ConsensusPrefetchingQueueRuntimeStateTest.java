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
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPrefetchingQueueRuntimeStateTest {

  private final int previousDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  @Test
  public void testFollowerQueueRemainsDormantWhenWriterSetIncludesLocalNode() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(2);

    final ConsensusPrefetchingQueue queue = createQueue(false);
    try {
      queue.applyRuntimeState(
          new ConsensusRegionRuntimeState(1L, 1, false, new LinkedHashSet<>(Arrays.asList(2, 1))));

      assertFalse(queue.isActive());
      assertNull(queue.poll("consumer", (RegionProgress) null));
    } finally {
      queue.close();
    }
  }

  @Test
  public void testFormerLeaderIsDeactivatedAfterLeaderTransfer() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);

    final ConsensusPrefetchingQueue queue = createQueue(true);
    try {
      queue.applyRuntimeState(
          new ConsensusRegionRuntimeState(2L, 2, false, new LinkedHashSet<>(Arrays.asList(2, 1))));

      assertFalse(queue.isActive());
      assertNull(queue.poll("consumer", (RegionProgress) null));
    } finally {
      queue.close();
    }
  }

  @Test
  public void testResolveCommittedRegionProgressForInitUsesLatestCommitState() {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final RegionProgress latestCommittedRegionProgress =
        new RegionProgress(
            Collections.singletonMap(
                new WriterId("DataRegion[11]", 2, 5L), new WriterProgress(10L, 3L)));
    when(commitManager.getCommittedRegionProgress(
            anyString(), anyString(), any(DataRegionId.class)))
        .thenReturn(latestCommittedRegionProgress);

    final TestConsensusPrefetchingQueue queue =
        createTestQueue(mock(ConsensusReqReader.class), commitManager, null);
    try {
      assertSame(
          latestCommittedRegionProgress, queue.resolveCommittedRegionProgressForInitForTest());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testResolveCommittedRegionProgressForInitFallsBackToConstructorSnapshot() {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    when(commitManager.getCommittedRegionProgress(
            anyString(), anyString(), any(DataRegionId.class)))
        .thenReturn(new RegionProgress(Collections.emptyMap()));
    final RegionProgress fallbackCommittedRegionProgress =
        new RegionProgress(
            Collections.singletonMap(
                new WriterId("DataRegion[11]", 1, 1L), new WriterProgress(20L, 7L)));

    final TestConsensusPrefetchingQueue queue =
        createTestQueue(
            mock(ConsensusReqReader.class), commitManager, fallbackCommittedRegionProgress);
    try {
      assertSame(
          fallbackCommittedRegionProgress, queue.resolveCommittedRegionProgressForInitForTest());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testInitPrefetchResolvesReplayStartFromCommittedRegionProgress() throws Exception {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final RegionProgress committedRegionProgress =
        new RegionProgress(
            Collections.singletonMap(
                new WriterId("DataRegion[11]", 2, 5L), new WriterProgress(10L, 3L)));
    when(commitManager.getCommittedRegionProgress(
            anyString(), anyString(), any(DataRegionId.class)))
        .thenReturn(committedRegionProgress);

    final WALNode walNode = mock(WALNode.class);
    when(walNode.getLogDirectory()).thenReturn(new File("."));
    final TestConsensusPrefetchingQueue queue = createTestQueue(walNode, commitManager, null);
    queue.setLocateDecision(
        ConsensusPrefetchingQueue.ReplayLocateDecision.found(
            37L, committedRegionProgress, "test locate"));
    try {
      queue.initPrefetchForTest(null);

      assertEquals(37L, queue.getCurrentReadSearchIndex());
      assertSame(committedRegionProgress, queue.getLastLocatedRegionProgress());
      assertTrue(queue.wasLastSeekAfter());
      assertEquals(
          committedRegionProgress.getWriterPositions(), queue.getRecoveryProgressForTest());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testInitPrefetchUsesConsumerHintWhenAheadOfCommittedProgress() throws Exception {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final WriterId writerId = new WriterId("DataRegion[11]", 2, 5L);
    final RegionProgress committedRegionProgress =
        new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(10L, 3L)));
    final RegionProgress consumerHint =
        new RegionProgress(Collections.singletonMap(writerId, new WriterProgress(10L, 4L)));
    when(commitManager.getCommittedRegionProgress(
            anyString(), anyString(), any(DataRegionId.class)))
        .thenReturn(committedRegionProgress);

    final WALNode walNode = mock(WALNode.class);
    when(walNode.getLogDirectory()).thenReturn(new File("."));
    final TestConsensusPrefetchingQueue queue = createTestQueue(walNode, commitManager, null);
    queue.setLocateDecision(
        ConsensusPrefetchingQueue.ReplayLocateDecision.found(55L, consumerHint, "test locate"));
    try {
      queue.initPrefetchForTest(consumerHint);

      assertEquals(55L, queue.getCurrentReadSearchIndex());
      assertEquals(
          consumerHint.getWriterPositions(),
          queue.getLastLocatedRegionProgress().getWriterPositions());
      assertTrue(queue.wasLastSeekAfter());
      assertEquals(consumerHint.getWriterPositions(), queue.getRecoveryProgressForTest());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testInitPrefetchMergesCommittedProgressWithPartialConsumerHint() throws Exception {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final WriterId writerA = new WriterId("DataRegion[11]", 2, 5L);
    final WriterId writerB = new WriterId("DataRegion[11]", 3, 6L);
    final java.util.LinkedHashMap<WriterId, WriterProgress> committedWriterProgress =
        new java.util.LinkedHashMap<>();
    committedWriterProgress.put(writerA, new WriterProgress(10L, 100L));
    committedWriterProgress.put(writerB, new WriterProgress(20L, 100L));
    final RegionProgress committedRegionProgress = new RegionProgress(committedWriterProgress);
    final RegionProgress consumerHint =
        new RegionProgress(Collections.singletonMap(writerA, new WriterProgress(10L, 101L)));
    when(commitManager.getCommittedRegionProgress(
            anyString(), anyString(), any(DataRegionId.class)))
        .thenReturn(committedRegionProgress);

    final WALNode walNode = mock(WALNode.class);
    when(walNode.getLogDirectory()).thenReturn(new File("."));
    final TestConsensusPrefetchingQueue queue = createTestQueue(walNode, commitManager, null);
    final Map<WriterId, WriterProgress> expectedRecoveryProgress = new LinkedHashMap<>();
    expectedRecoveryProgress.put(writerA, new WriterProgress(10L, 101L));
    expectedRecoveryProgress.put(writerB, new WriterProgress(20L, 100L));
    queue.setLocateDecision(
        ConsensusPrefetchingQueue.ReplayLocateDecision.found(
            88L, new RegionProgress(expectedRecoveryProgress), "test locate"));
    try {
      queue.initPrefetchForTest(consumerHint);

      assertEquals(88L, queue.getCurrentReadSearchIndex());
      final Map<WriterId, WriterProgress> recoveryProgress = queue.getRecoveryProgressForTest();
      assertEquals(2, recoveryProgress.size());
      assertEquals(new WriterProgress(10L, 101L), recoveryProgress.get(writerA));
      assertEquals(new WriterProgress(20L, 100L), recoveryProgress.get(writerB));
      assertEquals(recoveryProgress, queue.getLastLocatedRegionProgress().getWriterPositions());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testInitPrefetchThrowsWhenNonEmptyProgressCannotBeLocated() throws Exception {
    final ConsensusSubscriptionCommitManager commitManager =
        mock(ConsensusSubscriptionCommitManager.class);
    final RegionProgress committedRegionProgress =
        new RegionProgress(
            Collections.singletonMap(
                new WriterId("DataRegion[11]", 2, 5L), new WriterProgress(10L, 3L)));
    when(commitManager.getCommittedRegionProgress(
            anyString(), anyString(), any(DataRegionId.class)))
        .thenReturn(committedRegionProgress);

    final WALNode walNode = mock(WALNode.class);
    when(walNode.getLogDirectory()).thenReturn(new File("."));
    final TestConsensusPrefetchingQueue queue = createTestQueue(walNode, commitManager, null);
    queue.setLocateDecision(
        ConsensusPrefetchingQueue.ReplayLocateDecision.locateMiss(
            committedRegionProgress, "test locate miss"));
    try {
      try {
        queue.initPrefetchForTest(null);
        fail("expected initPrefetch to reject non-empty progress locate miss");
      } catch (final InvocationTargetException e) {
        assertTrue(e.getCause() instanceof IllegalStateException);
      }
    } finally {
      queue.close();
    }
  }

  @Test
  public void testScanReplayStartTreatsMissingWriterAsUncovered() throws Exception {
    final TestConsensusPrefetchingQueue queue =
        createTestQueue(
            mock(ConsensusReqReader.class), mock(ConsensusSubscriptionCommitManager.class), null);
    try {
      final WriterId writerA = new WriterId("DataRegion[11]", 2, 5L);
      final WriterId writerB = new WriterId("DataRegion[11]", 3, 6L);
      final RegionProgress recoveryProgress =
          new RegionProgress(Collections.singletonMap(writerA, new WriterProgress(10L, 3L)));

      final List<IndexedConsensusRequest> requests = new ArrayList<>();
      requests.add(newIndexedConsensusRequest(30L, 10L, 2, 5L, 3L));
      requests.add(newIndexedConsensusRequest(31L, 11L, 3, 6L, 1L));

      final ConsensusPrefetchingQueue.ReplayLocateDecision decision =
          queue.scanReplayStartForRequestsForTest(requests, recoveryProgress, true);

      assertEquals(ConsensusPrefetchingQueue.ReplayLocateStatus.FOUND, decision.getStatus());
      assertEquals(31L, decision.getStartSearchIndex());
      assertEquals(
          recoveryProgress.getWriterPositions(),
          decision.getRecoveryRegionProgress().getWriterPositions());
      assertTrue(decision.getRecoveryRegionProgress().getWriterPositions().containsKey(writerA));
      assertFalse(decision.getRecoveryRegionProgress().getWriterPositions().containsKey(writerB));
    } finally {
      queue.close();
    }
  }

  @Test
  public void testScanReplayStartReturnsLocateMissForBlockingNonReplayableUncoveredRequest()
      throws Exception {
    final TestConsensusPrefetchingQueue queue =
        createTestQueue(
            mock(ConsensusReqReader.class), mock(ConsensusSubscriptionCommitManager.class), null);
    try {
      final WriterId writerA = new WriterId("DataRegion[11]", 2, 5L);
      final RegionProgress recoveryProgress =
          new RegionProgress(Collections.singletonMap(writerA, new WriterProgress(10L, 3L)));

      final List<IndexedConsensusRequest> requests = new ArrayList<>();
      requests.add(newIndexedConsensusRequest(-1L, 11L, 3, 6L, 1L));
      requests.add(newIndexedConsensusRequest(40L, 12L, 4, 7L, 1L));

      final ConsensusPrefetchingQueue.ReplayLocateDecision decision =
          queue.scanReplayStartForRequestsForTest(requests, recoveryProgress, true);

      assertEquals(ConsensusPrefetchingQueue.ReplayLocateStatus.LOCATE_MISS, decision.getStatus());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testScanReplayStartForSeekToDecrementsExactVisibleWriterFrontiers() throws Exception {
    final TestConsensusPrefetchingQueue queue =
        createTestQueue(
            mock(ConsensusReqReader.class), mock(ConsensusSubscriptionCommitManager.class), null);
    try {
      final WriterId writerA = new WriterId("DataRegion[11]", 2, 5L);
      final WriterId writerB = new WriterId("DataRegion[11]", 3, 6L);
      final Map<WriterId, WriterProgress> writerProgress = new LinkedHashMap<>();
      writerProgress.put(writerA, new WriterProgress(10L, 3L));
      writerProgress.put(writerB, new WriterProgress(20L, 8L));
      final RegionProgress recoveryProgress = new RegionProgress(writerProgress);

      final List<IndexedConsensusRequest> requests = new ArrayList<>();
      requests.add(newIndexedConsensusRequest(30L, 10L, 2, 5L, 3L));
      requests.add(newIndexedConsensusRequest(31L, 20L, 3, 6L, 8L));

      final ConsensusPrefetchingQueue.ReplayLocateDecision decision =
          queue.scanReplayStartForRequestsForTest(requests, recoveryProgress, false);

      assertEquals(ConsensusPrefetchingQueue.ReplayLocateStatus.FOUND, decision.getStatus());
      assertEquals(30L, decision.getStartSearchIndex());
      assertEquals(
          new WriterProgress(10L, 2L),
          decision.getRecoveryRegionProgress().getWriterPositions().get(writerA));
      assertEquals(
          new WriterProgress(20L, 7L),
          decision.getRecoveryRegionProgress().getWriterPositions().get(writerB));
    } finally {
      queue.close();
    }
  }

  @Test
  public void
      testPerWriterRealtimeFrontierDoesNotInjectSyntheticBarrierForMissingPreferredWriterLane()
          throws Exception {
    final TestConsensusPrefetchingQueue queue =
        createTestQueue(
            mock(ConsensusReqReader.class), mock(ConsensusSubscriptionCommitManager.class), null);
    try {
      queue.setOrderMode(TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
      queue.setPreferredWriterNodeId(1);
      queue.setActiveWriterNodeIds(new LinkedHashSet<>(Arrays.asList(1, 3)));

      addRealtimeEntry(queue, 3, 1L, 100L, 1L, 10L);

      final Object frontier = buildRealtimeLaneFrontiers(queue).peek();
      assertFalse(isBarrier(frontier));
      assertEquals(3, getFrontierWriterNodeId(frontier));
    } finally {
      queue.close();
    }
  }

  @Test
  public void
      testMultiWriterRealtimeFrontierStillInjectsSyntheticBarrierForMissingPreferredWriterLane()
          throws Exception {
    final TestConsensusPrefetchingQueue queue =
        createTestQueue(
            mock(ConsensusReqReader.class), mock(ConsensusSubscriptionCommitManager.class), null);
    try {
      queue.setOrderMode(TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE);
      queue.setPreferredWriterNodeId(1);
      queue.setActiveWriterNodeIds(new LinkedHashSet<>(Arrays.asList(1, 3)));

      addRealtimeEntry(queue, 3, 1L, 100L, 1L, 10L);

      final Object frontier = buildRealtimeLaneFrontiers(queue).peek();
      assertTrue(isBarrier(frontier));
      assertEquals(1, getFrontierWriterNodeId(frontier));
    } finally {
      queue.close();
    }
  }

  private static ConsensusPrefetchingQueue createQueue(final boolean initialActive) {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    final ConsensusReqReader reqReader = mock(ConsensusReqReader.class);
    final WriterSafeFrontierTracker writerSafeFrontierTracker =
        mock(WriterSafeFrontierTracker.class);
    when(server.getConsensusReqReader()).thenReturn(reqReader);
    when(server.getWriterSafeFrontierTracker()).thenReturn(writerSafeFrontierTracker);
    when(writerSafeFrontierTracker.snapshotEffectiveSafePts()).thenReturn(Collections.emptyMap());
    when(reqReader.getCurrentSearchIndex()).thenReturn(0L);

    return new ConsensusPrefetchingQueue(
        "cg",
        "topic",
        TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE,
        new DataRegionId(11),
        server,
        mock(ConsensusLogToTabletConverter.class),
        mock(ConsensusSubscriptionCommitManager.class),
        null,
        1L,
        0L,
        initialActive);
  }

  private static TestConsensusPrefetchingQueue createTestQueue(
      final ConsensusReqReader reqReader,
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress) {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    final WriterSafeFrontierTracker writerSafeFrontierTracker =
        mock(WriterSafeFrontierTracker.class);
    when(server.getConsensusReqReader()).thenReturn(reqReader);
    when(server.getWriterSafeFrontierTracker()).thenReturn(writerSafeFrontierTracker);
    when(writerSafeFrontierTracker.snapshotEffectiveSafePts()).thenReturn(Collections.emptyMap());
    when(reqReader.getCurrentSearchIndex()).thenReturn(0L);

    return new TestConsensusPrefetchingQueue(
        server,
        reqReader,
        mock(ConsensusLogToTabletConverter.class),
        commitManager,
        fallbackCommittedRegionProgress);
  }

  @SuppressWarnings("unchecked")
  private static void addRealtimeEntry(
      final ConsensusPrefetchingQueue queue,
      final int writerNodeId,
      final long writerEpoch,
      final long physicalTime,
      final long localSeq,
      final long searchIndex)
      throws Exception {
    final Object laneId = newWriterLaneId(writerNodeId, writerEpoch);
    final Object preparedEntry =
        newPreparedEntry(searchIndex, physicalTime, writerNodeId, writerEpoch, localSeq);

    final Field realtimeEntriesByLaneField =
        ConsensusPrefetchingQueue.class.getDeclaredField("realtimeEntriesByLane");
    realtimeEntriesByLaneField.setAccessible(true);
    final Map<Object, NavigableMap<Long, Object>> realtimeEntriesByLane =
        (Map<Object, NavigableMap<Long, Object>>) realtimeEntriesByLaneField.get(queue);

    final NavigableMap<Long, Object> laneEntries = new TreeMap<>();
    laneEntries.put(localSeq, preparedEntry);
    realtimeEntriesByLane.put(laneId, laneEntries);
  }

  private static Object newWriterLaneId(final int writerNodeId, final long writerEpoch)
      throws Exception {
    final Class<?> writerLaneIdClass =
        Class.forName(
            "org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue$WriterLaneId");
    final Constructor<?> constructor =
        writerLaneIdClass.getDeclaredConstructor(int.class, long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(writerNodeId, writerEpoch);
  }

  private static Object newPreparedEntry(
      final long searchIndex,
      final long physicalTime,
      final int writerNodeId,
      final long writerEpoch,
      final long localSeq)
      throws Exception {
    final Class<?> preparedEntryClass =
        Class.forName(
            "org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue$PreparedEntry");
    final Constructor<?> constructor =
        preparedEntryClass.getDeclaredConstructor(
            java.util.List.class, long.class, long.class, int.class, long.class, long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(
        Collections.emptyList(), searchIndex, physicalTime, writerNodeId, writerEpoch, localSeq);
  }

  @SuppressWarnings("unchecked")
  private static PriorityQueue<Object> buildRealtimeLaneFrontiers(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Method method =
        ConsensusPrefetchingQueue.class.getDeclaredMethod("buildRealtimeLaneFrontiers");
    method.setAccessible(true);
    return (PriorityQueue<Object>) method.invoke(queue);
  }

  private static boolean isBarrier(final Object frontier) throws Exception {
    final Field field = frontier.getClass().getDeclaredField("isBarrier");
    field.setAccessible(true);
    return field.getBoolean(frontier);
  }

  private static int getFrontierWriterNodeId(final Object frontier) throws Exception {
    final Field laneIdField = frontier.getClass().getDeclaredField("laneId");
    laneIdField.setAccessible(true);
    final Object laneId = laneIdField.get(frontier);
    final Field writerNodeIdField = laneId.getClass().getDeclaredField("writerNodeId");
    writerNodeIdField.setAccessible(true);
    return writerNodeIdField.getInt(laneId);
  }

  private static IndexedConsensusRequest newIndexedConsensusRequest(
      final long searchIndex,
      final long physicalTime,
      final int nodeId,
      final long writerEpoch,
      final long localSeq) {
    return new IndexedConsensusRequest(searchIndex, localSeq, Collections.emptyList())
        .setPhysicalTime(physicalTime)
        .setNodeId(nodeId)
        .setWriterEpoch(writerEpoch);
  }

  private static final class TestConsensusPrefetchingQueue extends ConsensusPrefetchingQueue {

    private ReplayLocateDecision locateDecision =
        ReplayLocateDecision.atEnd(
            0L, new RegionProgress(Collections.emptyMap()), "default test locate");
    private RegionProgress lastLocatedRegionProgress;
    private boolean lastSeekAfter;

    private TestConsensusPrefetchingQueue(
        final IoTConsensusServerImpl server,
        final ConsensusReqReader reqReader,
        final ConsensusLogToTabletConverter converter,
        final ConsensusSubscriptionCommitManager commitManager,
        final RegionProgress fallbackCommittedRegionProgress) {
      super(
          "cg",
          "topic",
          TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE,
          new DataRegionId(11),
          server,
          converter,
          commitManager,
          fallbackCommittedRegionProgress,
          1L,
          0L,
          true);
      if (reqReader instanceof WALNode) {
        when(((WALNode) reqReader).getLogDirectory()).thenReturn(new File("."));
      }
    }

    @Override
    protected ReplayLocateDecision locateReplayStartForRegionProgress(
        final RegionProgress regionProgress, final boolean seekAfter) {
      this.lastLocatedRegionProgress = regionProgress;
      this.lastSeekAfter = seekAfter;
      return locateDecision;
    }

    private void setLocateDecision(final ReplayLocateDecision locateDecision) {
      this.locateDecision = locateDecision;
    }

    private RegionProgress getLastLocatedRegionProgress() {
      return lastLocatedRegionProgress;
    }

    private boolean wasLastSeekAfter() {
      return lastSeekAfter;
    }

    private ReplayLocateDecision scanReplayStartForRequestsForTest(
        final Iterable<IndexedConsensusRequest> requests,
        final RegionProgress regionProgress,
        final boolean seekAfter) {
      return scanReplayStartForRequests(requests, regionProgress, seekAfter);
    }

    private void initPrefetchForTest(final RegionProgress regionProgress) throws Exception {
      final Method method =
          ConsensusPrefetchingQueue.class.getDeclaredMethod("initPrefetch", RegionProgress.class);
      method.setAccessible(true);
      method.invoke(this, regionProgress);
    }

    @SuppressWarnings("unchecked")
    private Map<WriterId, WriterProgress> getRecoveryProgressForTest() throws Exception {
      final Field field =
          ConsensusPrefetchingQueue.class.getDeclaredField("recoveryWriterProgressByWriter");
      field.setAccessible(true);
      return (Map<WriterId, WriterProgress>) field.get(this);
    }

    private RegionProgress resolveCommittedRegionProgressForInitForTest() {
      return resolveCommittedRegionProgressForInit();
    }
  }
}
