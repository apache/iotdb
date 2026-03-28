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
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
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
  public void testInitPrefetchRollsWalOnceBeforeRetryingLookup() {
    final TestConsensusPrefetchingQueue queue = createTestQueue();
    final RegionProgress regionProgress =
        new RegionProgress(
            Collections.singletonMap(
                new WriterId("DataRegion[11]", 2, 5L), new WriterProgress(10L, 3L)));
    final AtomicBoolean walRolledDuringInit = new AtomicBoolean(false);
    queue.setLocateResults(-1L, 42L);

    try {
      final long searchIndex =
          queue.findEarliestSearchIndexAfterRegionProgressForInit(
              new File("."), regionProgress, walRolledDuringInit);

      assertEquals(42L, searchIndex);
      assertEquals(1, queue.getWalRollCount());
      assertTrue(walRolledDuringInit.get());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testInitPrefetchDoesNotRollWalTwice() {
    final TestConsensusPrefetchingQueue queue = createTestQueue();
    final RegionProgress regionProgress =
        new RegionProgress(
            Collections.singletonMap(
                new WriterId("DataRegion[11]", 2, 5L), new WriterProgress(10L, 3L)));
    final AtomicBoolean walRolledDuringInit = new AtomicBoolean(true);
    queue.setLocateResults(-1L);

    try {
      final long searchIndex =
          queue.findEarliestSearchIndexAfterRegionProgressForInit(
              new File("."), regionProgress, walRolledDuringInit);

      assertEquals(-1L, searchIndex);
      assertEquals(0, queue.getWalRollCount());
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

    final TestConsensusPrefetchingQueue queue = createTestQueue(commitManager, null);
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
        createTestQueue(commitManager, fallbackCommittedRegionProgress);
    try {
      assertSame(
          fallbackCommittedRegionProgress, queue.resolveCommittedRegionProgressForInitForTest());
    } finally {
      queue.close();
    }
  }

  @Test
  public void testPerWriterFrontierDoesNotInjectSyntheticBarrierForMissingPreferredWriterLane()
      throws Exception {
    final TestConsensusPrefetchingQueue queue = createTestQueue();
    try {
      queue.setOrderMode(TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
      queue.setPreferredWriterNodeId(1);
      queue.setActiveWriterNodeIds(new LinkedHashSet<>(Arrays.asList(1, 3)));

      addHistoricalEntry(queue, 3, 1L, 100L, 1L, 10L);

      final Object frontier = buildHistoricalLaneFrontiers(queue).peek();
      assertFalse(isBarrier(frontier));
      assertEquals(3, getFrontierWriterNodeId(frontier));
    } finally {
      queue.close();
    }
  }

  @Test
  public void testMultiWriterFrontierStillInjectsSyntheticBarrierForMissingPreferredWriterLane()
      throws Exception {
    final TestConsensusPrefetchingQueue queue = createTestQueue();
    try {
      queue.setOrderMode(TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE);
      queue.setPreferredWriterNodeId(1);
      queue.setActiveWriterNodeIds(new LinkedHashSet<>(Arrays.asList(1, 3)));

      addHistoricalEntry(queue, 3, 1L, 100L, 1L, 10L);

      final Object frontier = buildHistoricalLaneFrontiers(queue).peek();
      assertTrue(isBarrier(frontier));
      assertEquals(1, getFrontierWriterNodeId(frontier));
    } finally {
      queue.close();
    }
  }

  @Test
  public void testPerWriterHistoricalCatchUpDoesNotWaitForGlobalLaterTimestamp() throws Exception {
    final TestConsensusPrefetchingQueue queue = createTestQueue();
    try {
      queue.setOrderMode(TopicConstant.ORDER_MODE_PER_WRITER_VALUE);
      final Object entry = addHistoricalEntry(queue, 3, 1L, 100L, 1L, 10L);
      setHistoricalWalIterator(
          queue,
          new ProgressWALIterator(new File(".")) {
            @Override
            public boolean hasNext() {
              return true;
            }
          });

      assertTrue(canReleaseHistoricalEntry(queue, entry));
    } finally {
      queue.close();
    }
  }

  @Test
  public void testMultiWriterHistoricalCatchUpStillWaitsForGlobalLaterTimestamp() throws Exception {
    final TestConsensusPrefetchingQueue queue = createTestQueue();
    try {
      queue.setOrderMode(TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE);
      final Object entry = addHistoricalEntry(queue, 3, 1L, 100L, 1L, 10L);
      setHistoricalWalIterator(
          queue,
          new ProgressWALIterator(new File(".")) {
            @Override
            public boolean hasNext() {
              return true;
            }
          });

      assertFalse(canReleaseHistoricalEntry(queue, entry));
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

  private static TestConsensusPrefetchingQueue createTestQueue() {
    return createTestQueue(mock(ConsensusSubscriptionCommitManager.class), null);
  }

  private static TestConsensusPrefetchingQueue createTestQueue(
      final ConsensusSubscriptionCommitManager commitManager,
      final RegionProgress fallbackCommittedRegionProgress) {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    final ConsensusReqReader reqReader = mock(ConsensusReqReader.class);
    final WriterSafeFrontierTracker writerSafeFrontierTracker =
        mock(WriterSafeFrontierTracker.class);
    when(server.getConsensusReqReader()).thenReturn(reqReader);
    when(server.getWriterSafeFrontierTracker()).thenReturn(writerSafeFrontierTracker);
    when(writerSafeFrontierTracker.snapshotEffectiveSafePts()).thenReturn(Collections.emptyMap());

    return new TestConsensusPrefetchingQueue(
        server,
        mock(ConsensusLogToTabletConverter.class),
        commitManager,
        fallbackCommittedRegionProgress);
  }

  @SuppressWarnings("unchecked")
  private static Object addHistoricalEntry(
      final ConsensusPrefetchingQueue queue,
      final int writerNodeId,
      final long writerEpoch,
      final long physicalTime,
      final long localSeq,
      final long searchIndex)
      throws Exception {
    final Object laneId = newWriterLaneId(writerNodeId, writerEpoch);
    final ConsensusPrefetchingQueue.OrderingKey orderingKey =
        new ConsensusPrefetchingQueue.OrderingKey(
            physicalTime, writerNodeId, writerEpoch, localSeq);
    final Object sortableEntry =
        newSortableEntry(orderingKey, searchIndex, physicalTime, writerNodeId, writerEpoch);

    final Field historicalEntriesByLaneField =
        ConsensusPrefetchingQueue.class.getDeclaredField("historicalEntriesByLane");
    historicalEntriesByLaneField.setAccessible(true);
    final Map<Object, NavigableMap<ConsensusPrefetchingQueue.OrderingKey, Object>>
        historicalEntriesByLane =
            (Map<Object, NavigableMap<ConsensusPrefetchingQueue.OrderingKey, Object>>)
                historicalEntriesByLaneField.get(queue);

    final NavigableMap<ConsensusPrefetchingQueue.OrderingKey, Object> laneEntries = new TreeMap<>();
    laneEntries.put(orderingKey, sortableEntry);
    historicalEntriesByLane.put(laneId, laneEntries);
    return sortableEntry;
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

  private static Object newSortableEntry(
      final ConsensusPrefetchingQueue.OrderingKey orderingKey,
      final long searchIndex,
      final long physicalTime,
      final int writerNodeId,
      final long writerEpoch)
      throws Exception {
    final Class<?> sortableEntryClass =
        Class.forName(
            "org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue$SortableEntry");
    final Constructor<?> constructor =
        sortableEntryClass.getDeclaredConstructor(
            ConsensusPrefetchingQueue.OrderingKey.class,
            java.util.List.class,
            long.class,
            long.class,
            int.class,
            long.class);
    constructor.setAccessible(true);
    return constructor.newInstance(
        orderingKey, Collections.emptyList(), searchIndex, physicalTime, writerNodeId, writerEpoch);
  }

  @SuppressWarnings("unchecked")
  private static PriorityQueue<Object> buildHistoricalLaneFrontiers(
      final ConsensusPrefetchingQueue queue) throws Exception {
    final Method method =
        ConsensusPrefetchingQueue.class.getDeclaredMethod("buildHistoricalLaneFrontiers");
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

  private static void setHistoricalWalIterator(
      final ConsensusPrefetchingQueue queue, final ProgressWALIterator historicalWalIterator)
      throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("historicalWALIterator");
    field.setAccessible(true);
    field.set(queue, historicalWalIterator);
  }

  private static boolean canReleaseHistoricalEntry(
      final ConsensusPrefetchingQueue queue, final Object sortableEntry) throws Exception {
    final Class<?> sortableEntryClass =
        Class.forName(
            "org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue$SortableEntry");
    final Method method =
        ConsensusPrefetchingQueue.class.getDeclaredMethod(
            "canReleaseHistoricalEntry", sortableEntryClass);
    method.setAccessible(true);
    return (boolean) method.invoke(queue, sortableEntry);
  }

  private static final class TestConsensusPrefetchingQueue extends ConsensusPrefetchingQueue {

    private long[] locateResults = new long[0];
    private int locateIndex = 0;
    private int walRollCount = 0;

    private TestConsensusPrefetchingQueue(
        final IoTConsensusServerImpl server,
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
    }

    private void setLocateResults(final long... locateResults) {
      this.locateResults = locateResults;
      this.locateIndex = 0;
      this.walRollCount = 0;
    }

    private int getWalRollCount() {
      return walRollCount;
    }

    private RegionProgress resolveCommittedRegionProgressForInitForTest() {
      return resolveCommittedRegionProgressForInit();
    }

    @Override
    protected long findEarliestSearchIndexAfterRegionProgress(
        final File logDir, final RegionProgress regionProgress) {
      final long result =
          locateIndex < locateResults.length
              ? locateResults[locateIndex]
              : locateResults[locateResults.length - 1];
      locateIndex++;
      return result;
    }

    @Override
    protected boolean canRollCurrentWalFileForPrefetchInit() {
      return true;
    }

    @Override
    protected void rollCurrentWalFileForPrefetchInit() {
      walRollCount++;
    }
  }
}
