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
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.pipe.agent.task.PipeDataNodeTaskAgent;
import org.apache.iotdb.db.subscription.broker.ISubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionBlockingPendingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionBroker;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.columnfilter.ColumnFilterMatcher;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.batch.SubscriptionPipeTabletEventBatch;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeEvents;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.FilePiecePayload;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;
import org.apache.iotdb.rpc.subscription.payload.poll.TerminationPayload;
import org.apache.iotdb.session.subscription.util.PollTimer;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @SuppressWarnings("unchecked")
    private RetainedSubscription(final boolean tableModel, final boolean retain, final String mode)
        throws Exception {
      this.tableModel = tableModel;
      this.retain = retain;
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      topic,
                      1L,
                      Map.of(
                          SystemConstant.SQL_DIALECT_KEY,
                          tableModel ? "table" : "tree",
                          TopicConstant.MODE_KEY,
                          mode,
                          TopicConstant.RETAIN_PROGRESS_AFTER_UNSUBSCRIBE_KEY,
                          String.valueOf(retain),
                          TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY,
                          "0"))));
      meta = new ConsumerGroupMeta(group, 1L, consumer("first"));
      meta.addSubscription("first", Collections.singleton(topic));
      assertNull(
          SubscriptionAgent.consumer().handleSingleConsumerGroupMetaChanges(meta.deepCopy()));
      final SubscriptionBlockingPendingQueue input = mock(SubscriptionBlockingPendingQueue.class);
      when(input.isEmpty()).thenReturn(true);
      queue = spy(new SubscriptionPrefetchingTabletQueue(group, topic, input, new AtomicLong()));
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
