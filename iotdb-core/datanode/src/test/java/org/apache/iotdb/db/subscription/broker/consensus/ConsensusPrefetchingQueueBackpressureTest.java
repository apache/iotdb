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
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.SubscriptionWalRetentionPolicy;
import org.apache.iotdb.consensus.iot.WriterSafeFrontierTracker;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.StatementTestUtils;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ConsensusPrefetchingQueueBackpressureTest {

  @Parameterized.Parameters(name = "{0}, WAL={1}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(
        new Object[] {TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE, false},
        new Object[] {TopicConstant.ORDER_MODE_LEADER_ONLY_VALUE, true},
        new Object[] {TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE, false});
  }

  @Parameterized.Parameter public String orderMode;

  @Parameterized.Parameter(1)
  public boolean fromWal;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final AtomicInteger conversions = new AtomicInteger();
  private final AtomicInteger walPosition = new AtomicInteger();
  private final List<IndexedConsensusRequest> requests = new ArrayList<>();
  private final List<SubscriptionEvent> polledEvents = new ArrayList<>();
  private String originalSystemDir;
  private int originalTablets;
  private int originalWalEntries;
  private int originalDelay;
  private long originalWatermarkInterval;
  private int capacity;
  private ConsensusPrefetchingQueue queue;

  @Before
  public void setUp() throws Exception {
    originalSystemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    originalTablets = config.getSubscriptionConsensusBatchMaxTabletCount();
    originalWalEntries = config.getSubscriptionConsensusBatchMaxWalEntries();
    originalDelay = config.getSubscriptionConsensusBatchMaxDelayInMs();
    originalWatermarkInterval = config.getSubscriptionConsensusWatermarkIntervalMs();
    capacity = config.getSubscriptionConsensusPrefetchingQueueCapacity();
    config.setSubscriptionConsensusBatchMaxTabletCount(1);
    config.setSubscriptionConsensusBatchMaxWalEntries(capacity * 2);
    config.setSubscriptionConsensusBatchMaxDelayInMs(0);
    config.setSubscriptionConsensusWatermarkIntervalMs(0L);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setSystemDir(temporaryFolder.newFolder("system").getAbsolutePath());

    for (int index = 1; index <= capacity * 2; index++) {
      requests.add(
          new IndexedConsensusRequest(
                  index, Collections.singletonList(StatementTestUtils.genInsertRowNode(index)))
              .setNodeId(7)
              .setPhysicalTime(1000L + index));
    }
    final ConsensusReqReader reader = mock(ConsensusReqReader.class);
    when(reader.getCurrentSearchIndex()).thenReturn((long) requests.size());
    final WriterSafeFrontierTracker tracker = new WriterSafeFrontierTracker();
    tracker.recordAppliedProgress(1000L + requests.size(), 7, requests.size());
    tracker.observePendingWriterSafeTimeBarrier(Long.MAX_VALUE, 7, requests.size());
    tracker.observePendingWriterSafeTimeBarrier(Long.MAX_VALUE, 8, 0);
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(server.getConsensusReqReader()).thenReturn(reader);
    when(server.getWriterSafeFrontierTracker()).thenReturn(tracker);
    final ConsensusLogToTabletConverter converter = mock(ConsensusLogToTabletConverter.class);
    when(converter.getDatabaseName()).thenReturn("db");
    when(converter.convert(any()))
        .thenAnswer(
            ignored -> {
              conversions.incrementAndGet();
              final Tablet tablet =
                  new Tablet(
                      "root.test.device",
                      Collections.singletonList(new MeasurementSchema("s", TSDataType.INT64)),
                      1);
              tablet.addTimestamp(0, 1L);
              tablet.addValue("s", 0, 1L);
              tablet.setRowSize(1);
              return Collections.singletonList(tablet);
            });
    final ProgressWALIterator walIterator = mock(ProgressWALIterator.class);
    when(walIterator.hasNext(any(ProgressWALIterator.ScanBudget.class)))
        .thenAnswer(ignored -> walPosition.get() < requests.size());
    when(walIterator.next()).thenAnswer(ignored -> requests.get(walPosition.getAndIncrement()));
    queue =
        new ConsensusPrefetchingQueue(
            "backpressure_group",
            "backpressure_topic",
            orderMode,
            new DataRegionId(1),
            server,
            new SubscriptionWalRetentionPolicy(
                "backpressure_topic",
                SubscriptionWalRetentionPolicy.UNBOUNDED,
                SubscriptionWalRetentionPolicy.UNBOUNDED),
            converter,
            new ConsensusSubscriptionCommitManager(
                (group, topic, id) ->
                    ConsensusSubscriptionCommitManager.ConfigNodeProgressQueryResult.absent()),
            new RegionProgress(Collections.emptyMap()),
            1L,
            1L,
            true) {
          @Override
          protected ProgressWALIterator createSubscriptionWALIterator(final long startSearchIndex) {
            return fromWal ? walIterator : null;
          }
        };
    queue.setSubscriptionMemoryManager(new SubscriptionMemoryManager(64L * 1024 * 1024));
    if (TopicConstant.ORDER_MODE_MULTI_WRITER_VALUE.equals(orderMode)) {
      queue.setActiveWriterNodeIds(Set.of(7, 8));
    }
    assertNull(queue.poll("consumer"));
    if (!fromWal) {
      for (final IndexedConsensusRequest request : requests) {
        assertTrue(pending().offer(request));
      }
    }
  }

  @After
  public void tearDown() {
    if (queue != null) {
      queue.close();
      assertEquals(0L, queue.getRetainedRequestBytes());
      assertEquals(0L, queue.getRetainedTabletBytes());
    }
    SubscriptionAgent.topic().handleDropTopic("backpressure_topic", false);
    config.setSubscriptionConsensusBatchMaxTabletCount(originalTablets);
    config.setSubscriptionConsensusBatchMaxWalEntries(originalWalEntries);
    config.setSubscriptionConsensusBatchMaxDelayInMs(originalDelay);
    config.setSubscriptionConsensusWatermarkIntervalMs(originalWatermarkInterval);
    IoTDBDescriptor.getInstance().getConfig().setSystemDir(originalSystemDir);
  }

  @Test
  public void testAckResumesWithinRetainedEventCapacity() {
    verifyAckResumesWithinRetainedEventCapacity(false);
  }

  @Test
  public void testAckSilentResumesWithinRetainedEventCapacity() {
    verifyAckResumesWithinRetainedEventCapacity(true);
  }

  private void verifyAckResumesWithinRetainedEventCapacity(final boolean silent) {
    fillRetainedCapacity();
    assertTrue(
        silent
            ? queue.ackSilent("consumer", polledEvents.get(0).getCommitContext())
            : queue.ack("consumer", polledEvents.get(0).getCommitContext()));
    queue.drivePrefetchOnce();
    assertEquals(capacity, queue.getSubscriptionRetainedEventCount());
    assertEquals(2, queue.getPrefetchedEventCount());
    assertEquals(capacity - 2L, queue.getSubscriptionUncommittedEventCount());
    assertEquals(capacity + 1, conversions.get());
    queue.drivePrefetchOnce();
    assertEquals(capacity + 1, conversions.get());

    for (int index = 1; index < polledEvents.size(); index++) {
      assertTrue(queue.ack("consumer", polledEvents.get(index).getCommitContext()));
    }
    int delivered = polledEvents.size();
    for (int round = 0; round < requests.size() && delivered < requests.size(); round++) {
      queue.drivePrefetchOnce();
      assertTrue(queue.getSubscriptionRetainedEventCount() <= capacity);
      SubscriptionEvent event;
      while ((event = queue.poll("consumer")) != null) {
        delivered++;
        assertEquals(delivered, event.getCommitContext().getWriterProgress().getLocalSeq());
        assertTrue(queue.ack("consumer", event.getCommitContext()));
      }
    }
    assertEquals(requests.size(), delivered);
    assertEquals(requests.size(), conversions.get());
    assertEquals(0L, queue.getSubscriptionRetainedEventCount());
  }

  @Test
  public void testFullQueueUsesTopicRecycleInterval() {
    fillRetainedCapacity();
    for (final long intervalMs : new long[] {1200000L, 0L, -1L, Long.MIN_VALUE, 300000L}) {
      assertNull(
          SubscriptionAgent.topic()
              .handleSingleTopicMetaChanges(
                  new TopicMeta(
                      "backpressure_topic",
                      1L,
                      Collections.singletonMap(
                          TopicConstant.RECYCLE_UNCOMMITTED_EVENT_INTERVAL_MS_KEY,
                          Long.toString(intervalMs)))));
      final PrefetchRoundResult result = queue.drivePrefetchOnce();
      assertEquals(
          intervalMs > 0L
              ? PrefetchRoundResult.Type.RESCHEDULE_LATER
              : PrefetchRoundResult.Type.DORMANT,
          result.getType());
      if (intervalMs > 0L) {
        assertEquals(intervalMs, result.getDelayMs());
      }
      assertEquals(capacity, queue.getSubscriptionRetainedEventCount());
      assertEquals(capacity, conversions.get());
    }
  }

  private void fillRetainedCapacity() {
    queue.drivePrefetchOnce();
    assertEquals(capacity, queue.getPrefetchedEventCount());
    for (int index = 1; index < capacity; index++) {
      final SubscriptionEvent event = queue.poll("consumer");
      assertNotNull(event);
      polledEvents.add(event);
    }
    queue.drivePrefetchOnce();
    assertEquals(capacity, queue.getSubscriptionRetainedEventCount());
    assertEquals(capacity, conversions.get());
    assertEquals(1, queue.getPrefetchedEventCount());
  }

  @SuppressWarnings("unchecked")
  private BlockingQueue<IndexedConsensusRequest> pending() throws Exception {
    final Field field = ConsensusPrefetchingQueue.class.getDeclaredField("pendingEntries");
    field.setAccessible(true);
    return (BlockingQueue<IndexedConsensusRequest>) field.get(queue);
  }
}
