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

package org.apache.iotdb.db.subscription.metric;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.db.subscription.resource.SubscriptionDataNodeResourceManager;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/** Subscription metrics must survive a metric service restart, which drops all metrics. */
public class SubscriptionMetricsRestartTest {

  private static final String QUEUE_ID = "consumer_group_topic";

  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final List<IMetricSet> boundMetricSets = new ArrayList<>();
  private MetricLevel originalLevel;
  private String originalReporters;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service.startService();
  }

  @After
  public void tearDown() {
    boundMetricSets.forEach(service::removeMetricSet);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testPrefetchingQueueMetrics() {
    final SubscriptionPrefetchingQueueMetrics metrics =
        SubscriptionPrefetchingQueueMetrics.getInstance();
    bind(metrics);
    final SubscriptionPrefetchingQueue queue = Mockito.mock(SubscriptionPrefetchingQueue.class);
    when(queue.getPrefetchingQueueId()).thenReturn(QUEUE_ID);
    when(queue.getSubscriptionUncommittedEventCount()).thenReturn(3L);
    metrics.register(queue);
    try {
      service.restartService();
      assertEquals(3, ((AutoGauge) get(Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT)).getValue(), 0);
      metrics.mark(QUEUE_ID, 5);
      assertEquals(5, ((Rate) get(Metric.SUBSCRIPTION_EVENT_TRANSFER)).getCount());
    } finally {
      metrics.deregister(QUEUE_ID);
    }
    assertEquals(0, count(Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT));
    assertEquals(0, count(Metric.SUBSCRIPTION_EVENT_TRANSFER));
  }

  @Test
  public void testSubscriptionMemoryMetrics() throws Exception {
    final SubscriptionMetrics metrics = SubscriptionMetrics.getInstance();
    // Subscription can be disabled in the default test configuration. Give the shared allocator
    // an explicit test budget and restore its original block after removing the metrics.
    final long budget = 10L;
    final SubscriptionMemoryManager memoryManager = SubscriptionDataNodeResourceManager.memory();
    final long originalOversizedEntryCount = memoryManager.getOversizedEntryCount();
    final Field blockField = SubscriptionMemoryManager.class.getDeclaredField("memoryBlock");
    blockField.setAccessible(true);
    final Object originalBlock = blockField.get(memoryManager);
    try {
      blockField.set(memoryManager, blockField.get(new SubscriptionMemoryManager(budget)));
      bind(metrics);
      assertTrue(memoryManager.tryAllocate(budget + 1L));
      try {
        service.restartService();
        assertEquals(
            budget, ((AutoGauge) get(Metric.SUBSCRIPTION_MEMORY_LIMIT_BYTES)).getValue(), 0);
        assertEquals(
            budget + 1L, ((AutoGauge) get(Metric.SUBSCRIPTION_MEMORY_USED_BYTES)).getValue(), 0);
        assertEquals(
            1, ((AutoGauge) get(Metric.SUBSCRIPTION_MEMORY_OVERCOMMIT_BYTES)).getValue(), 0);
        assertEquals(
            originalOversizedEntryCount + 1L,
            ((AutoGauge) get(Metric.SUBSCRIPTION_MEMORY_OVERSIZED_ENTRY_COUNT)).getValue(),
            0);
        assertFalse(memoryManager.tryAllocate(1L));
      } finally {
        memoryManager.release(budget + 1L);
      }
      assertEquals(0, ((AutoGauge) get(Metric.SUBSCRIPTION_MEMORY_USED_BYTES)).getValue(), 0);
      assertEquals(0, ((AutoGauge) get(Metric.SUBSCRIPTION_MEMORY_OVERCOMMIT_BYTES)).getValue(), 0);
      service.removeMetricSet(metrics);
      boundMetricSets.remove(metrics);
      assertEquals(0, count(Metric.SUBSCRIPTION_MEMORY_USED_BYTES));
      assertEquals(0, count(Metric.SUBSCRIPTION_MEMORY_LIMIT_BYTES));
      assertEquals(0, count(Metric.SUBSCRIPTION_MEMORY_OVERCOMMIT_BYTES));
      assertEquals(0, count(Metric.SUBSCRIPTION_MEMORY_OVERSIZED_ENTRY_COUNT));
    } finally {
      service.removeMetricSet(metrics);
      boundMetricSets.remove(metrics);
      blockField.set(memoryManager, originalBlock);
    }
  }

  @Test
  public void testConsensusPrefetchingQueueMetrics() {
    final ConsensusSubscriptionPrefetchingQueueMetrics metrics =
        ConsensusSubscriptionPrefetchingQueueMetrics.getInstance();
    bind(metrics);
    final ConsensusPrefetchingQueue queue = Mockito.mock(ConsensusPrefetchingQueue.class);
    when(queue.getPrefetchingQueueId()).thenReturn(QUEUE_ID);
    when(queue.getConsensusGroupId()).thenReturn(new DataRegionId(1));
    when(queue.getLag()).thenReturn(7L);
    when(queue.getLastDeliveryIntervalMs()).thenReturn(60_001L);
    when(queue.getMaxDeliveryIntervalMs()).thenReturn(65_000L);
    when(queue.getDeliveryIdleTimeMs()).thenReturn(60_000L);
    when(queue.getPrefetchDurationMs()).thenReturn(65_000L);
    when(queue.getMaxPrefetchDurationMs()).thenReturn(69_000L);
    when(queue.getPrefetchIdleTimeMs()).thenReturn(67_000L);
    metrics.register(queue);
    try {
      service.restartService();
      assertEquals(7, ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_LAG)).getValue(), 0);
      assertEquals(
          60_001,
          ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_LAST_DELIVERY_INTERVAL_MS)).getValue(),
          0);
      assertEquals(
          65_000,
          ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_MAX_DELIVERY_INTERVAL_MS)).getValue(),
          0);
      assertEquals(
          60_000,
          ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_DELIVERY_IDLE_TIME_MS)).getValue(),
          0);
      assertEquals(
          65_000,
          ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_PREFETCH_DURATION_MS)).getValue(),
          0);
      assertEquals(
          69_000,
          ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_MAX_PREFETCH_DURATION_MS)).getValue(),
          0);
      assertEquals(
          67_000,
          ((AutoGauge) get(Metric.SUBSCRIPTION_CONSENSUS_PREFETCH_IDLE_TIME_MS)).getValue(),
          0);
      metrics.mark(QUEUE_ID, new DataRegionId(1).toString(), 5);
      assertEquals(5, ((Rate) get(Metric.SUBSCRIPTION_EVENT_TRANSFER)).getCount());
    } finally {
      metrics.deregister(queue);
    }
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_LAG));
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_LAST_DELIVERY_INTERVAL_MS));
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_MAX_DELIVERY_INTERVAL_MS));
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_DELIVERY_IDLE_TIME_MS));
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_PREFETCH_DURATION_MS));
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_MAX_PREFETCH_DURATION_MS));
    assertEquals(0, count(Metric.SUBSCRIPTION_CONSENSUS_PREFETCH_IDLE_TIME_MS));
    assertEquals(0, count(Metric.SUBSCRIPTION_EVENT_TRANSFER));
  }

  @Test
  public void testPipeAndConsensusQueuesCoexist() {
    final SubscriptionPrefetchingQueueMetrics pipeMetrics =
        SubscriptionPrefetchingQueueMetrics.getInstance();
    final ConsensusSubscriptionPrefetchingQueueMetrics consensusMetrics =
        ConsensusSubscriptionPrefetchingQueueMetrics.getInstance();
    bind(pipeMetrics);
    bind(consensusMetrics);
    // Whichever kind of queue registers first must not stop the other kind from exporting metrics
    final ConsensusPrefetchingQueue firstConsensusQueue = mockConsensusQueue("consensus_topic", 1);
    final SubscriptionPrefetchingQueue pipeQueue = Mockito.mock(SubscriptionPrefetchingQueue.class);
    when(pipeQueue.getPrefetchingQueueId()).thenReturn(QUEUE_ID);
    when(pipeQueue.getSubscriptionUncommittedEventCount()).thenReturn(3L);
    final ConsensusPrefetchingQueue secondConsensusQueue = mockConsensusQueue("consensus_topic", 2);
    consensusMetrics.register(firstConsensusQueue);
    pipeMetrics.register(pipeQueue);
    consensusMetrics.register(secondConsensusQueue);
    try {
      assertEquals(3, count(Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT));
      pipeMetrics.mark(QUEUE_ID, 5);
      consensusMetrics.mark("consensus_topic", new DataRegionId(1).toString(), 7);
      consensusMetrics.mark("consensus_topic", new DataRegionId(2).toString(), 11);
      assertEquals(
          23,
          service.getAllMetrics().entrySet().stream()
              .filter(
                  entry ->
                      Metric.SUBSCRIPTION_EVENT_TRANSFER
                          .toString()
                          .equals(entry.getKey().getName()))
              .mapToLong(entry -> ((Rate) entry.getValue()).getCount())
              .sum());
    } finally {
      pipeMetrics.deregister(QUEUE_ID);
      consensusMetrics.deregister(firstConsensusQueue);
      consensusMetrics.deregister(secondConsensusQueue);
    }
    assertEquals(0, count(Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT));
  }

  private static ConsensusPrefetchingQueue mockConsensusQueue(
      final String queueId, final int regionId) {
    final ConsensusPrefetchingQueue queue = Mockito.mock(ConsensusPrefetchingQueue.class);
    when(queue.getPrefetchingQueueId()).thenReturn(queueId);
    when(queue.getConsensusGroupId()).thenReturn(new DataRegionId(regionId));
    return queue;
  }

  private void bind(final IMetricSet metricSet) {
    service.addMetricSet(metricSet);
    boundMetricSets.add(metricSet);
  }

  private long count(final Metric metric) {
    return service.getAllMetrics().keySet().stream()
        .filter(info -> metric.toString().equals(info.getName()))
        .count();
  }

  private IMetric get(final Metric metric) {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (metric.toString().equals(entry.getKey().getName())) {
        return entry.getValue();
      }
    }
    throw new AssertionError(metric + " is not registered");
  }
}
