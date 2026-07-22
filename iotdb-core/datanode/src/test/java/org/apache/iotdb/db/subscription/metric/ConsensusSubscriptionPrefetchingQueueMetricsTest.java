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
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusSubscriptionPrefetchingQueueMetricsTest {

  @Test
  public void testMetricsAreIsolatedByRegion() throws Exception {
    final String queueId = "consumer_group_topic";
    final DataRegionId firstRegionId = new DataRegionId(1);
    final DataRegionId secondRegionId = new DataRegionId(2);
    final ConsensusPrefetchingQueue firstQueue = mock(ConsensusPrefetchingQueue.class);
    final ConsensusPrefetchingQueue secondQueue = mock(ConsensusPrefetchingQueue.class);
    final ConsensusPrefetchingQueue staleFirstQueue = mock(ConsensusPrefetchingQueue.class);
    final AbstractMetricService metricService = mock(AbstractMetricService.class);
    final Rate firstRate = mock(Rate.class);
    final Rate secondRate = mock(Rate.class);

    when(firstQueue.getPrefetchingQueueId()).thenReturn(queueId);
    when(firstQueue.getConsensusGroupId()).thenReturn(firstRegionId);
    when(secondQueue.getPrefetchingQueueId()).thenReturn(queueId);
    when(secondQueue.getConsensusGroupId()).thenReturn(secondRegionId);
    when(staleFirstQueue.getPrefetchingQueueId()).thenReturn(queueId);
    when(staleFirstQueue.getConsensusGroupId()).thenReturn(firstRegionId);
    when(metricService.getOrCreateRate(
            Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            queueId,
            Tag.REGION.toString(),
            firstRegionId.toString()))
        .thenReturn(firstRate);
    when(metricService.getOrCreateRate(
            Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            queueId,
            Tag.REGION.toString(),
            secondRegionId.toString()))
        .thenReturn(secondRate);

    final ConsensusSubscriptionPrefetchingQueueMetrics metrics =
        ConsensusSubscriptionPrefetchingQueueMetrics.getInstance();
    final Field metricServiceField =
        ConsensusSubscriptionPrefetchingQueueMetrics.class.getDeclaredField("metricService");
    final Field queueMapField =
        ConsensusSubscriptionPrefetchingQueueMetrics.class.getDeclaredField("queueMap");
    final Field rateMapField =
        ConsensusSubscriptionPrefetchingQueueMetrics.class.getDeclaredField("rateMap");
    metricServiceField.setAccessible(true);
    queueMapField.setAccessible(true);
    rateMapField.setAccessible(true);
    final Map<?, ?> queueMap = (Map<?, ?>) queueMapField.get(metrics);
    final Map<?, ?> rateMap = (Map<?, ?>) rateMapField.get(metrics);
    queueMap.clear();
    rateMap.clear();
    metricServiceField.set(metrics, null);

    try {
      metrics.bindTo(metricService);
      metrics.register(firstQueue);
      metrics.register(secondQueue);

      assertEquals(2, queueMap.size());
      assertEquals(2, rateMap.size());
      verify(metricService)
          .createAutoGauge(
              eq(Metric.SUBSCRIPTION_CONSENSUS_LAG.toString()),
              eq(MetricLevel.IMPORTANT),
              eq(firstQueue),
              any(),
              eq(Tag.NAME.toString()),
              eq(queueId),
              eq(Tag.REGION.toString()),
              eq(firstRegionId.toString()));
      verify(metricService)
          .createAutoGauge(
              eq(Metric.SUBSCRIPTION_CONSENSUS_LAG.toString()),
              eq(MetricLevel.IMPORTANT),
              eq(secondQueue),
              any(),
              eq(Tag.NAME.toString()),
              eq(queueId),
              eq(Tag.REGION.toString()),
              eq(secondRegionId.toString()));

      metrics.mark(queueId, firstRegionId.toString(), 11L);
      metrics.mark(queueId, secondRegionId.toString(), 22L);
      verify(firstRate).mark(11L);
      verify(secondRate).mark(22L);

      metrics.register(staleFirstQueue);
      metrics.deregister(staleFirstQueue);
      assertEquals(2, queueMap.size());
      assertEquals(2, rateMap.size());

      metrics.deregister(firstQueue);
      assertEquals(1, queueMap.size());
      assertEquals(1, rateMap.size());
      verify(metricService)
          .remove(
              MetricType.AUTO_GAUGE,
              Metric.SUBSCRIPTION_CONSENSUS_LAG.toString(),
              Tag.NAME.toString(),
              queueId,
              Tag.REGION.toString(),
              firstRegionId.toString());
      verify(metricService, never())
          .remove(
              MetricType.AUTO_GAUGE,
              Metric.SUBSCRIPTION_CONSENSUS_LAG.toString(),
              Tag.NAME.toString(),
              queueId,
              Tag.REGION.toString(),
              secondRegionId.toString());

      metrics.mark(queueId, secondRegionId.toString(), 33L);
      verify(secondRate).mark(33L);
    } finally {
      queueMap.clear();
      rateMap.clear();
      metricServiceField.set(metrics, null);
    }
  }
}
