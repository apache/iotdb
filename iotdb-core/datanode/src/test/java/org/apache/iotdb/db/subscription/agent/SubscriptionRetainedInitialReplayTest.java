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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskManager;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.PipeDataNodeTaskAgent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.subscription.broker.ISubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionBlockingPendingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueueStates;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEvents;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;
import org.apache.iotdb.session.subscription.util.PollTimer;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SubscriptionRetainedInitialReplayTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDownloadedTsFileReplaysFromBeginningWithoutPoisoning() throws Exception {
    for (final boolean tableModel : new boolean[] {false, true}) {
      for (final boolean closeConsumer : new boolean[] {false, true}) {
        try (final RetainedSubscription subscription = new RetainedSubscription(tableModel)) {
          final SubscriptionEvent event =
              new SubscriptionEvent(
                  mock(SubscriptionPipeEvents.class),
                  folder.newFile(),
                  null,
                  subscription.queue.generateSubscriptionCommitContext());
          subscription.queue.prefetchEvent(event);
          // More than ten detach/rejoin cycles must not trigger poison-message auto-commit.
          for (int i = 0; i < 12; i++) {
            assertSame(event, subscription.poll("first"));
            assertEquals(
                SubscriptionPollResponseType.FILE_INIT.getType(),
                event.getCurrentResponse().getResponseType());
            event.fetchNextResponse(0L);
            assertEquals(
                SubscriptionPollResponseType.FILE_SEAL.getType(),
                event.getCurrentResponse().getResponseType());
            subscription.detach(closeConsumer);
            subscription.rejoin();
            assertFalse(event.isCommitted());
            assertEquals(0L, event.getNackCount());
          }
          assertSame(event, subscription.poll("first"));
          assertEquals(
              SubscriptionPollResponseType.FILE_INIT.getType(),
              event.getCurrentResponse().getResponseType());
        }
      }
    }
  }

  @Test
  public void testReleasedTsFileReplaysFromBeginningAfterLastUnsubscribe() throws Exception {
    final byte[] fileBytes = {1, 2, 3, 4};
    for (final boolean tableModel : new boolean[] {false, true}) {
      for (final boolean closeConsumer : new boolean[] {false, true}) {
        try (final RetainedSubscription subscription = new RetainedSubscription(tableModel)) {
          final SubscriptionEvent event =
              new SubscriptionEvent(
                  mock(SubscriptionPipeEvents.class),
                  Files.write(folder.newFile().toPath(), fileBytes).toFile(),
                  null,
                  subscription.queue.generateSubscriptionCommitContext());
          subscription.queue.prefetchEvent(event);
          assertSame(event, subscription.poll("first"));
          event.fetchNextResponse(0L);
          event.fetchNextResponse(fileBytes.length);
          assertEquals(
              SubscriptionPollResponseType.FILE_SEAL.getType(),
              event.getCurrentResponse().getResponseType());

          // A disconnected receiver releases its deliveries before the group unsubscribes.
          assertEquals(1, subscription.broker.requeueInFlightEvents("first"));
          assertEquals(0L, subscription.queue.getSubscriptionUncommittedEventCount());
          subscription.detach(closeConsumer);
          subscription.rejoin();

          assertSame(event, subscription.poll("first"));
          assertEquals(
              SubscriptionPollResponseType.FILE_INIT.getType(),
              event.getCurrentResponse().getResponseType());
          assertEquals(0L, event.getNackCount());
          assertFalse(event.isCommitted());
          event.fetchNextResponse(0L);
          assertArrayEquals(
              fileBytes,
              ((FilePiecePayload) event.getCurrentResponse().getPayload()).getFilePiece());
          event.fetchNextResponse(fileBytes.length);
          assertTrue(subscription.queue.ack("first", event.getCommitContext()));
          assertNull(subscription.poll("first"));
        }
      }
    }
  }

  @Test
  public void testDownloadedTabletPagesReplayTheirRows() throws Exception {
    assertDownloadedTabletPagesReplayTheirRows(false);
  }

  @Test
  public void testReleasedTabletPagesReplayTheirRows() throws Exception {
    assertDownloadedTabletPagesReplayTheirRows(true);
  }

  private void assertDownloadedTabletPagesReplayTheirRows(final boolean releaseBeforeDetach)
      throws Exception {
    try (final RetainedSubscription subscription = new RetainedSubscription(false)) {
      final Tablet tablet =
          new Tablet(
              "root.retained.d",
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64)),
              1);
      tablet.addTimestamp(0, 42L);
      tablet.addValue("s1", 0, 7L);
      tablet.setRowSize(1);
      final AtomicBoolean iterated = new AtomicBoolean();
      final SubscriptionPipeTabletEventBatch batch = mock(SubscriptionPipeTabletEventBatch.class);
      when(batch.getColumnFilterMatcher()).thenReturn(ColumnFilterMatcher.matchAll());
      when(batch.hasNext()).thenAnswer(invocation -> !iterated.get());
      when(batch.next())
          .thenAnswer(
              invocation -> {
                iterated.set(true);
                return new Pair<>("", Collections.singletonList(tablet));
              });
      doAnswer(
              invocation -> {
                iterated.set(false);
                return null;
              })
          .when(batch)
          .resetForIteration();
      final SubscriptionEvent event = new SubscriptionEvent(batch, subscription.queue);
      subscription.queue.prefetchEvent(event);
      assertSame(event, subscription.poll("first"));
      event.fetchNextResponse(1L);
      assertSame(
          tablet, ((TabletsPayload) event.getCurrentResponse().getPayload()).getTablets().get(0));
      event.fetchNextResponse(2L);
      assertEquals(-1, ((TabletsPayload) event.getCurrentResponse().getPayload()).getNextOffset());
      if (releaseBeforeDetach) {
        assertEquals(1, subscription.broker.requeueInFlightEvents("first"));
      }
      subscription.detach(false);
      subscription.rejoin();
      assertSame(event, subscription.poll("first"));
      assertEquals(1, ((TabletsPayload) event.getCurrentResponse().getPayload()).getNextOffset());
      event.fetchNextResponse(1L);
      assertSame(
          tablet, ((TabletsPayload) event.getCurrentResponse().getPayload()).getTablets().get(0));
    }
  }

  @Test
  public void testLastUnsubscribeReplaysEarlierConsumersUncommittedEvents() throws Exception {
    try (final RetainedSubscription subscription = new RetainedSubscription(false)) {
      subscription.meta.addConsumer(subscription.consumer("second"));
      subscription.meta.addSubscription("second", Collections.singleton(subscription.topic));
      assertNull(
          SubscriptionAgent.consumer()
              .handleSingleConsumerGroupMetaChanges(subscription.meta.deepCopy()));
      final SubscriptionEvent committed = subscription.newEvent();
      subscription.queue.prefetchEvent(committed);
      assertSame(committed, subscription.poll("first"));
      assertTrue(subscription.queue.ack("first", committed.getCommitContext()));
      final SubscriptionEvent uncommitted = subscription.newEvent();
      subscription.queue.prefetchEvent(uncommitted);
      assertSame(uncommitted, subscription.poll("first"));
      subscription.meta.removeSubscription("first", Collections.singleton(subscription.topic));
      assertNull(
          SubscriptionAgent.consumer()
              .handleSingleConsumerGroupMetaChanges(subscription.meta.deepCopy()));
      subscription.meta.removeSubscription("second", Collections.singleton(subscription.topic));
      subscription.meta.retainTopic(subscription.topic);
      assertNull(
          SubscriptionAgent.consumer()
              .handleSingleConsumerGroupMetaChanges(subscription.meta.deepCopy()));
      subscription.rejoin();
      assertEquals(0L, subscription.queue.getSubscriptionUncommittedEventCount());
      assertSame(uncommitted, subscription.poll("first"));
      assertFalse(uncommitted.isCommitted());
      assertEquals(0, subscription.queue.getPrefetchedEventCount());
    }
  }

  @Test
  public void testRetainFalseStillDiscardsQueueOnConsumerClose() throws Exception {
    try (final RetainedSubscription subscription = new RetainedSubscription(false, false)) {
      final SubscriptionEvent event = subscription.newEvent();
      subscription.queue.prefetchEvent(event);
      assertSame(event, subscription.poll("first"));
      subscription.detach(true);
      assertTrue(subscription.queue.isClosed());
      assertEquals(0L, subscription.queue.getSubscriptionUncommittedEventCount());
      assertEquals(0, subscription.queue.getPrefetchedEventCount());
      assertFalse(SubscriptionAgent.consumer().containsConsumerGroup(subscription.group));
      assertFalse(SubscriptionAgent.broker().isBrokerExist(subscription.group));
    }
  }

  @Test
  public void testPollWithStaleTopicSetCannotStrandRetainedEvents() throws Exception {
    for (final String mode :
        new String[] {TopicConstant.MODE_INITIAL_VALUE, TopicConstant.MODE_SNAPSHOT_VALUE}) {
      for (final boolean closeConsumer : new boolean[] {false, true}) {
        try (final RetainedSubscription subscription =
            new RetainedSubscription(false, true, mode)) {
          final SubscriptionEvent event = subscription.newEvent();
          subscription.queue.prefetchEvent(event);
          assertSame(event, subscription.poll("first"));
          doAnswer(
                  invocation -> {
                    invocation.callRealMethod();
                    // Model a request admitted before unsubscribe that reaches the queue as soon as
                    // the uncommitted event is released, before the meta-change handler returns.
                    assertTrue(
                        subscription
                            .broker
                            .poll(
                                "first", Collections.singleton(subscription.topic), Long.MAX_VALUE)
                            .isEmpty());
                    return null;
                  })
              .when(subscription.queue)
              .resetAndRequeueInFlightEvents();
          subscription.detach(closeConsumer);
          assertEquals(0L, subscription.queue.getSubscriptionUncommittedEventCount());
          assertEquals(1, subscription.queue.getPrefetchedEventCount());
          subscription.rejoin();
          assertSame(event, subscription.poll("first"));
        }
      }
    }
  }

  @Test
  public void testCompletedRetainedSnapshotReportsTerminationAfterRejoin() throws Exception {
    for (final boolean tableModel : new boolean[] {false, true}) {
      for (final boolean closeConsumer : new boolean[] {false, true}) {
        try (final RetainedSubscription subscription =
            new RetainedSubscription(tableModel, true, TopicConstant.MODE_SNAPSHOT_VALUE)) {
          final Set<String> topicNames = Collections.singleton(subscription.topic);
          assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topicNames).isEmpty());
          subscription.queue.markCompleted();
          assertEquals(
              Collections.singletonList(subscription.topic),
              subscription.broker.fetchTopicNamesToUnsubscribe(topicNames));
          assertEquals(
              SubscriptionPollResponseType.TERMINATION.getType(),
              subscription
                  .broker
                  .poll("first", topicNames, Long.MAX_VALUE)
                  .get(0)
                  .getCurrentResponse()
                  .getResponseType());

          subscription.detach(closeConsumer);
          assertFalse(subscription.queue.isClosed());
          assertTrue(subscription.broker.poll("first", topicNames, Long.MAX_VALUE).isEmpty());
          subscription.rejoin();
          assertTrue(subscription.broker.hasQueue(subscription.topic));
          final List<SubscriptionEvent> events =
              subscription.broker.poll("first", topicNames, Long.MAX_VALUE);
          assertEquals(1, events.size());
          assertEquals(
              SubscriptionPollResponseType.TERMINATION.getType(),
              events.get(0).getCurrentResponse().getResponseType());
          assertEquals(0L, subscription.queue.getSubscriptionUncommittedEventCount());
          assertEquals(0, subscription.queue.getPrefetchedEventCount());
        }
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEmptyRetainedSnapshotCompletionSurvivesRejoinAndBrokerRecovery()
      throws Exception {
    final CommonConfig originalConfig =
        Whitebox.getInternalState(SubscriptionConfig.class, "COMMON_CONFIG");
    final CommonConfig enabledConfig = spy(originalConfig);
    when(enabledConfig.getSubscriptionEnabled()).thenReturn(true);
    try {
      Whitebox.setInternalState(SubscriptionConfig.class, "COMMON_CONFIG", enabledConfig);
      for (final boolean tableModel : new boolean[] {false, true}) {
        for (final boolean closeConsumer : new boolean[] {false, true}) {
          try (final RetainedSubscription subscription =
              new RetainedSubscription(tableModel, true, TopicConstant.MODE_SNAPSHOT_VALUE)) {
            subscription.broker.removePrefetchingQueue(subscription.topic);
            final Set<String> topics = Collections.singleton(subscription.topic);
            final Map<String, String> source =
                new HashMap<>(
                    Map.of(
                        SystemConstant.SQL_DIALECT_KEY,
                        tableModel ? "table" : "tree",
                        "source.mode",
                        "snapshot"));
            final PipeRuntimeMeta runtime = new PipeRuntimeMeta();
            runtime.getStatus().set(PipeStatus.RUNNING);
            final PipeMeta pipe =
                new PipeMeta(
                    new PipeStaticMeta(
                        PipeStaticMeta.generateSubscriptionPipeName(
                            subscription.topic, subscription.group),
                        1L,
                        source,
                        Collections.emptyMap(),
                        Map.of(
                            PipeSinkConstant.SINK_TOPIC_KEY, subscription.topic,
                            PipeSinkConstant.SINK_CONSUMER_GROUP_KEY, subscription.group)),
                    runtime);
            final PipeDataNodeTaskAgent taskAgent =
                new PipeDataNodeTaskAgent() {
                  @Override
                  protected boolean isShutdown() {
                    return false;
                  }

                  @Override
                  protected void createPipeTask(
                      final int regionId,
                      final PipeStaticMeta staticMeta,
                      final PipeTaskMeta taskMeta) {
                    throw new AssertionError(
                        "A completed empty snapshot must not accept new tasks");
                  }
                };
            try {
              assertNull(
                  taskAgent.handleSinglePipeMetaChanges(
                      PipeMeta.deserialize4TaskAgent(pipe.serialize())));
              assertTrue(subscription.broker.poll("first", topics, Long.MAX_VALUE).isEmpty());

              source.put(SystemConstant.SUBSCRIPTION_EMPTY_SNAPSHOT_COMPLETED_KEY, "true");
              final PipeMeta completed = PipeMeta.deserialize4TaskAgent(pipe.serialize());
              assertNull(taskAgent.handleSinglePipeMetaChanges(completed));
              // Later DataRegions must not change the original snapshot's empty result.
              runtime
                  .getConsensusGroupId2TaskMetaMap()
                  .put(7, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0));
              assertNull(
                  taskAgent.handleSinglePipeMetaChanges(
                      PipeMeta.deserialize4TaskAgent(pipe.serialize())));
              assertEquals(
                  Collections.singletonList(subscription.topic),
                  subscription.broker.fetchTopicNamesToUnsubscribe(topics));
              subscription.detach(closeConsumer);
              assertTrue(subscription.broker.poll("first", topics, Long.MAX_VALUE).isEmpty());
              subscription.rejoin();
              assertEquals(
                  SubscriptionPollResponseType.TERMINATION.getType(),
                  subscription
                      .broker
                      .poll("first", topics, Long.MAX_VALUE)
                      .get(0)
                      .getCurrentResponse()
                      .getResponseType());

              // Restore an unsubscribed group and its persisted Pipe after losing all local
              // brokers.
              subscription.detach(closeConsumer);
              assertNull(SubscriptionAgent.consumer().handleDropConsumerGroup(subscription.group));
              assertNull(
                  SubscriptionAgent.consumer()
                      .handleSingleConsumerGroupMetaChanges(subscription.meta.deepCopy()));
              final PipeDataNodeTaskAgent recoveredTaskAgent =
                  new PipeDataNodeTaskAgent() {
                    @Override
                    protected boolean isShutdown() {
                      return false;
                    }

                    @Override
                    protected void createPipeTask(
                        final int regionId,
                        final PipeStaticMeta staticMeta,
                        final PipeTaskMeta taskMeta) {
                      throw new AssertionError(
                          "A completed empty snapshot must not accept new tasks");
                    }
                  };
              assertTrue(
                  recoveredTaskAgent
                      .handlePipeMetaChangesInternal(
                          Collections.singletonList(
                              PipeMeta.deserialize4TaskAgent(pipe.serialize())))
                      .isEmpty());
              final Field brokersField =
                  SubscriptionBrokerAgent.class.getDeclaredField("consumerGroupIdToBrokers");
              brokersField.setAccessible(true);
              final Map<String, List<ISubscriptionBroker>> brokers =
                  (Map<String, List<ISubscriptionBroker>>)
                      brokersField.get(SubscriptionAgent.broker());
              final SubscriptionBroker recovered =
                  (SubscriptionBroker) brokers.get(subscription.group).get(0);
              assertFalse(recovered.hasQueue(subscription.topic));
              assertTrue(recovered.poll("first", topics, Long.MAX_VALUE).isEmpty());
              subscription.rejoin();
              assertEquals(
                  SubscriptionPollResponseType.TERMINATION.getType(),
                  recovered
                      .poll("first", topics, Long.MAX_VALUE)
                      .get(0)
                      .getCurrentResponse()
                      .getResponseType());
              assertTrue(
                  recoveredTaskAgent
                      .handlePipeMetaChangesInternal(Collections.emptyList())
                      .isEmpty());
            } finally {
              taskAgent.handlePipeMetaChangesInternal(Collections.emptyList());
            }
          }
        }
      }
    } finally {
      Whitebox.setInternalState(SubscriptionConfig.class, "COMMON_CONFIG", originalConfig);
    }
  }

  @Test
  public void testMultiRegionRetainedSnapshotWaitsForAllExpectedRegionsBeforeTermination()
      throws Exception {
    for (final boolean tableModel : new boolean[] {false, true}) {
      // V1 (tablet queue): Region 1 terminating early must not trigger TERMINATION before Region 2
      // events and terminate event arrive and are acked.
      final ArrayDequeBlockingPendingQueue v1Pending = new ArrayDequeBlockingPendingQueue();
      try (final RetainedSubscription subscription =
          new RetainedSubscription(
              tableModel, true, TopicConstant.MODE_SNAPSHOT_VALUE, false, v1Pending)) {
        final SubscriptionPrefetchingQueueStates spiedStates =
            spy(
                (SubscriptionPrefetchingQueueStates)
                    Whitebox.getInternalState(subscription.queue, "states"));
        doReturn(true).when(spiedStates).shouldPrefetch();
        Whitebox.setInternalState(subscription.queue, "states", spiedStates);
        doReturn(Optional.of(ImmutableSet.of(1, 2)))
            .when(subscription.queue)
            .getExpectedDataRegionIds("pipe", 1L);

        final PipeTerminateEvent termR1 = new PipeTerminateEvent("pipe", 1L, null, 1, true);
        assertTrue(termR1.increaseReferenceCount("test"));
        v1Pending.directOffer(termR1);

        subscription.queue.executePrefetch();
        final Set<String> topics = Collections.singleton(subscription.topic);
        assertFalse(subscription.queue.isCompleted());
        assertFalse(termR1.isReleased());
        assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topics).isEmpty());
        assertTrue(subscription.broker.poll("first", topics, Long.MAX_VALUE).isEmpty());

        final PipeRawTabletInsertionEvent tabletR2 = createTabletEvent(tableModel, 2, 1L);
        assertTrue(tabletR2.increaseReferenceCount("test"));
        final PipeTerminateEvent termR2 = new PipeTerminateEvent("pipe", 1L, null, 2, true);
        assertTrue(termR2.increaseReferenceCount("test"));
        v1Pending.directOffer(tabletR2);
        v1Pending.directOffer(termR2);

        subscription.queue.executePrefetch();
        assertFalse(subscription.queue.isCompleted());
        assertFalse(termR1.isReleased());
        assertFalse(termR2.isReleased());
        assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topics).isEmpty());
        assertEquals(1, subscription.queue.getPrefetchedEventCount());

        final List<SubscriptionEvent> delivered =
            subscription.broker.poll("first", topics, Long.MAX_VALUE);
        assertEquals(1, delivered.size());
        assertEquals(
            SubscriptionPollResponseType.TABLETS.getType(),
            delivered.get(0).getCurrentResponse().getResponseType());
        assertFalse(subscription.queue.isCompleted());
        assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topics).isEmpty());

        final SubscriptionEvent tabletDelivered = delivered.get(0);
        int nextOffset =
            ((TabletsPayload) tabletDelivered.getCurrentResponse().getPayload()).getNextOffset();
        while (nextOffset != -1) {
          tabletDelivered.fetchNextResponse(nextOffset);
          nextOffset =
              ((TabletsPayload) tabletDelivered.getCurrentResponse().getPayload()).getNextOffset();
        }
        assertTrue(subscription.queue.ack("first", tabletDelivered.getCommitContext()));
        assertTrue(subscription.queue.isCompleted());
        assertTrue(termR1.isReleased());
        assertTrue(termR2.isReleased());
        assertEquals(
            Collections.singletonList(subscription.topic),
            subscription.broker.fetchTopicNamesToUnsubscribe(topics));
        final List<SubscriptionEvent> termEvents =
            subscription.broker.poll("first", topics, Long.MAX_VALUE);
        assertEquals(1, termEvents.size());
        assertEquals(
            SubscriptionPollResponseType.TERMINATION.getType(),
            termEvents.get(0).getCurrentResponse().getResponseType());
      }

      // V2 (TsFile queue): early terminate event in queue must not block draining later regions,
      // and Optional.empty() expected-region lookup (lock contention) must defer completion.
      final ArrayDequeBlockingPendingQueue v2Pending = new ArrayDequeBlockingPendingQueue();
      try (final RetainedSubscription subscription =
          new RetainedSubscription(
              tableModel, true, TopicConstant.MODE_SNAPSHOT_VALUE, true, v2Pending)) {
        doReturn(Optional.empty()).when(subscription.queue).getExpectedDataRegionIds("pipe", 2L);

        final byte[] tsFileBytes = {1, 2, 3, 4};
        final PipeTerminateEvent termR1 = new PipeTerminateEvent("pipe", 2L, null, 1, true);
        assertTrue(termR1.increaseReferenceCount("test"));
        final PipeTsFileInsertionEvent tsFileR2 = mock(PipeTsFileInsertionEvent.class);
        when(tsFileR2.shouldParseTimeOrPattern()).thenReturn(false);
        when(tsFileR2.getTsFile())
            .thenReturn(Files.write(folder.newFile().toPath(), tsFileBytes).toFile());
        when(tsFileR2.isTableModelEvent()).thenReturn(tableModel);
        when(tsFileR2.getTableModelDatabaseName()).thenReturn("db");
        final PipeTerminateEvent termR2 = new PipeTerminateEvent("pipe", 2L, null, 2, true);
        assertTrue(termR2.increaseReferenceCount("test"));
        v2Pending.directOffer(termR1);
        v2Pending.directOffer(tsFileR2);
        v2Pending.directOffer(termR2);

        subscription.queue.executePrefetchV2();
        final Set<String> topics = Collections.singleton(subscription.topic);
        assertFalse(subscription.queue.isCompleted());
        assertEquals(1, subscription.queue.getPrefetchedEventCount());

        final List<SubscriptionEvent> delivered =
            subscription.broker.poll("first", topics, Long.MAX_VALUE);
        assertEquals(1, delivered.size());
        final SubscriptionEvent tsFileDelivered = delivered.get(0);
        assertEquals(
            SubscriptionPollResponseType.FILE_INIT.getType(),
            tsFileDelivered.getCurrentResponse().getResponseType());
        tsFileDelivered.fetchNextResponse(0L);
        tsFileDelivered.fetchNextResponse(tsFileBytes.length);
        assertTrue(subscription.queue.ack("first", tsFileDelivered.getCommitContext()));
        // Still unproven because expected region set lookup returned Optional.empty().
        assertFalse(subscription.queue.isCompleted());

        doReturn(Optional.of(ImmutableSet.of(1, 2)))
            .when(subscription.queue)
            .getExpectedDataRegionIds("pipe", 2L);
        subscription.queue.executePrefetchV2();
        assertTrue(subscription.queue.isCompleted());
        assertTrue(termR1.isReleased());
        assertTrue(termR2.isReleased());
      }

      // shouldMark=false (mode=initial): terminate event releases without marking completed.
      final ArrayDequeBlockingPendingQueue initialPending = new ArrayDequeBlockingPendingQueue();
      try (final RetainedSubscription subscription =
          new RetainedSubscription(
              tableModel, true, TopicConstant.MODE_INITIAL_VALUE, false, initialPending)) {
        final SubscriptionPrefetchingQueueStates spiedStates =
            spy(
                (SubscriptionPrefetchingQueueStates)
                    Whitebox.getInternalState(subscription.queue, "states"));
        doReturn(true).when(spiedStates).shouldPrefetch();
        Whitebox.setInternalState(subscription.queue, "states", spiedStates);

        final PipeTerminateEvent initTerm = new PipeTerminateEvent("init_pipe", 1L, null, 1, false);
        assertTrue(initTerm.increaseReferenceCount("test"));
        initialPending.directOffer(initTerm);
        subscription.queue.executePrefetch();
        assertTrue(initTerm.isReleased());
        assertFalse(subscription.queue.isCompleted());
        assertTrue(
            subscription
                .broker
                .fetchTopicNamesToUnsubscribe(Collections.singleton(subscription.topic))
                .isEmpty());
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRetainedSnapshotEmitsTerminationWhenRunningWithoutLocalTasks() throws Exception {
    for (final boolean tableModel : new boolean[] {false, true}) {
      try (final RetainedSubscription subscription =
          new RetainedSubscription(
              tableModel,
              true,
              TopicConstant.MODE_SNAPSHOT_VALUE,
              false,
              RetainedSubscription.mockEmptyQueue())) {
        final Field queuesField =
            SubscriptionBroker.class.getDeclaredField("topicNameToPrefetchingQueue");
        queuesField.setAccessible(true);
        ((Map<String, SubscriptionPrefetchingQueue>) queuesField.get(subscription.broker))
            .remove(subscription.topic);

        final Set<String> topics = Collections.singleton(subscription.topic);
        assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topics).isEmpty());
        assertTrue(subscription.broker.poll("first", topics, Long.MAX_VALUE).isEmpty());

        final String pipeName =
            PipeStaticMeta.generateSubscriptionPipeName(subscription.topic, subscription.group);
        final Map<String, String> sourceAttrs = new HashMap<>();
        sourceAttrs.put(SystemConstant.SQL_DIALECT_KEY, tableModel ? "table" : "tree");
        sourceAttrs.put("source.mode", TopicConstant.MODE_SNAPSHOT_VALUE);
        sourceAttrs.put("source.realtime.enable", Boolean.FALSE.toString());
        final PipeStaticMeta staticMeta =
            new PipeStaticMeta(
                pipeName,
                1L,
                sourceAttrs,
                Collections.emptyMap(),
                Collections.singletonMap(PipeSinkConstant.SINK_TOPIC_KEY, subscription.topic));
        final PipeRuntimeMeta runtimeMeta = new PipeRuntimeMeta();
        runtimeMeta.getStatus().set(PipeStatus.STOPPED);
        final PipeMeta pipeMeta = new PipeMeta(staticMeta, runtimeMeta);

        final PipeDataNodeTaskAgent taskAgent = PipeDataNodeAgent.task();
        final PipeMetaKeeper keeper =
            (PipeMetaKeeper) Whitebox.getInternalState(taskAgent, "pipeMetaKeeper");
        final PipeTaskManager taskManager =
            (PipeTaskManager) Whitebox.getInternalState(taskAgent, "pipeTaskManager");
        keeper.addPipeMeta(pipeMeta);
        try {
          // STOPPED subscription pipe with zero local tasks must not emit TERMINATION.
          assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topics).isEmpty());
          assertTrue(subscription.broker.poll("first", topics, Long.MAX_VALUE).isEmpty());

          // RUNNING subscription pipe with zero local tasks must emit TERMINATION.
          runtimeMeta.getStatus().set(PipeStatus.RUNNING);
          assertEquals(
              Collections.singletonList(subscription.topic),
              subscription.broker.fetchTopicNamesToUnsubscribe(topics));
          final List<SubscriptionEvent> termEvents =
              subscription.broker.poll("first", topics, Long.MAX_VALUE);
          assertEquals(1, termEvents.size());
          assertEquals(
              SubscriptionPollResponseType.TERMINATION.getType(),
              termEvents.get(0).getCurrentResponse().getResponseType());

          // Once a local PipeTask exists on this DataNode, zero-local-task fallback is inactive.
          taskManager.addPipeTask(staticMeta, 1, mock(PipeTask.class));
          assertTrue(subscription.broker.fetchTopicNamesToUnsubscribe(topics).isEmpty());
          assertTrue(subscription.broker.poll("first", topics, Long.MAX_VALUE).isEmpty());
        } finally {
          taskManager.removePipeTasks(staticMeta);
          keeper.removePipeMeta(staticMeta);
        }
      }
    }
  }

  private static PipeRawTabletInsertionEvent createTabletEvent(
      final boolean tableModel, final int regionId, final long commitId) {
    final Tablet tablet =
        tableModel
            ? new Tablet(
                "src",
                Arrays.asList("dev", "s1"),
                Arrays.asList(TSDataType.STRING, TSDataType.INT64),
                Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
                1)
            : new Tablet(
                "root.retained.d",
                Collections.singletonList(new MeasurementSchema("s1", TSDataType.INT64)),
                1);
    tablet.addTimestamp(0, 1000L);
    if (tableModel) {
      tablet.addValue(0, 0, "d1");
      tablet.addValue(0, 1, 7L);
    } else {
      tablet.addValue("s1", 0, 7L);
    }
    tablet.setRowSize(1);
    final PipeRawTabletInsertionEvent event =
        new PipeRawTabletInsertionEvent(
            tableModel,
            "root.retained",
            "db",
            "root.retained",
            tablet,
            false,
            "pipe",
            1L,
            null,
            null,
            false);
    event.setCommitterKeyAndCommitId(new CommitterKey("pipe", 1L, regionId, 0), commitId);
    return event;
  }

  private static final class ArrayDequeBlockingPendingQueue
      extends SubscriptionBlockingPendingQueue {
    private final Queue<Event> queue = new ArrayDeque<>();

    private ArrayDequeBlockingPendingQueue() {
      super(null);
    }

    @Override
    public synchronized Event waitedPoll() {
      return queue.poll();
    }

    @Override
    public synchronized Event peek() {
      return queue.peek();
    }

    @Override
    public synchronized void directOffer(final Event event) {
      queue.offer(event);
    }

    @Override
    public synchronized int size() {
      return queue.size();
    }

    @Override
    public synchronized boolean isEmpty() {
      return queue.isEmpty();
    }
  }

  private static final class RetainedSubscription implements AutoCloseable {
    private final String topic = "retained_replay";
    private final String group = "retained_replay_group";
    private final boolean tableModel;
    private final boolean retain;
    private final SubscriptionPrefetchingQueue queue;
    private final SubscriptionBroker broker;
    private ConsumerGroupMeta meta;

    private RetainedSubscription(final boolean tableModel) throws Exception {
      this(tableModel, true, TopicConstant.MODE_INITIAL_VALUE);
    }

    private RetainedSubscription(final boolean tableModel, final boolean retain) throws Exception {
      this(tableModel, retain, TopicConstant.MODE_INITIAL_VALUE);
    }

    private RetainedSubscription(final boolean tableModel, final boolean retain, final String mode)
        throws Exception {
      this(tableModel, retain, mode, false, mockEmptyQueue());
    }

    @SuppressWarnings("unchecked")
    private RetainedSubscription(
        final boolean tableModel,
        final boolean retain,
        final String mode,
        final boolean tsFileQueue,
        final SubscriptionBlockingPendingQueue input)
        throws Exception {
      this.tableModel = tableModel;
      this.retain = retain;
      final Map<String, String> topicAttributes = new HashMap<>();
      topicAttributes.put(SystemConstant.SQL_DIALECT_KEY, tableModel ? "table" : "tree");
      topicAttributes.put(TopicConstant.MODE_KEY, mode);
      topicAttributes.put(
          TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY, String.valueOf(retain));
      topicAttributes.put(TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY, "0");
      if (tsFileQueue) {
        topicAttributes.put(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      }
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(new TopicMeta(topic, 1L, topicAttributes)));
      meta = new ConsumerGroupMeta(group, 1L, consumer("first"));
      meta.addSubscription("first", Collections.singleton(topic));
      assertNull(
          SubscriptionAgent.consumer().handleSingleConsumerGroupMetaChanges(meta.deepCopy()));
      queue =
          spy(
              tsFileQueue
                  ? new SubscriptionPrefetchingTsFileQueue(group, topic, input, new AtomicLong())
                  : new SubscriptionPrefetchingTabletQueue(group, topic, input, new AtomicLong()));
      final Field brokersField =
          SubscriptionBrokerAgent.class.getDeclaredField("consumerGroupIdToBrokers");
      brokersField.setAccessible(true);
      final Map<String, List<ISubscriptionBroker>> brokers =
          (Map<String, List<ISubscriptionBroker>>) brokersField.get(SubscriptionAgent.broker());
      broker = (SubscriptionBroker) brokers.get(group).get(0);
      final Field queuesField =
          SubscriptionBroker.class.getDeclaredField("topicNameToPrefetchingQueue");
      queuesField.setAccessible(true);
      ((Map<String, SubscriptionPrefetchingQueue>) queuesField.get(broker)).put(topic, queue);
    }

    private static SubscriptionBlockingPendingQueue mockEmptyQueue() {
      final SubscriptionBlockingPendingQueue input = mock(SubscriptionBlockingPendingQueue.class);
      when(input.isEmpty()).thenReturn(true);
      return input;
    }

    private ConsumerMeta consumer(final String id) {
      return new ConsumerMeta(
          id,
          1L,
          Collections.singletonMap(
              ConsumerConstant.SQL_DIALECT_KEY, tableModel ? "table" : "tree"));
    }

    private SubscriptionEvent newEvent() {
      return new SubscriptionEvent(
          SubscriptionPollResponseType.TERMINATION.getType(),
          new TerminationPayload(),
          queue.generateSubscriptionCommitContext());
    }

    private SubscriptionEvent poll(final String consumerId) {
      return queue.pollV2(consumerId, new PollTimer(System.currentTimeMillis(), 0L));
    }

    private void detach(final boolean closeConsumer) {
      meta.removeSubscription("first", Collections.singleton(topic));
      if (retain) {
        meta.retainTopic(topic);
      }
      if (closeConsumer) {
        meta.removeConsumer("first");
      }
      assertNull(
          SubscriptionAgent.consumer().handleSingleConsumerGroupMetaChanges(meta.deepCopy()));
    }

    private void rejoin() {
      if (!meta.containsConsumer("first")) {
        meta.addConsumer(consumer("first"));
      }
      meta.addSubscription("first", Collections.singleton(topic));
      meta.removeRetainedTopic(topic);
      assertNull(
          SubscriptionAgent.consumer().handleSingleConsumerGroupMetaChanges(meta.deepCopy()));
    }

    @Override
    public void close() {
      assertNull(SubscriptionAgent.consumer().handleDropConsumerGroup(group));
      assertNull(SubscriptionAgent.topic().handleDropTopic(topic, tableModel));
    }
  }
}
