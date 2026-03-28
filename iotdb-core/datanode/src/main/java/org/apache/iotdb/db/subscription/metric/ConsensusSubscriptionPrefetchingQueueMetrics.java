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

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.subscription.broker.consensus.ConsensusPrefetchingQueue;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class ConsensusSubscriptionPrefetchingQueueMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionPrefetchingQueueMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, ConsensusPrefetchingQueue> queueMap = new ConcurrentHashMap<>();

  private final Map<String, Rate> rateMap = new ConcurrentHashMap<>();

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> ids = ImmutableSet.copyOf(queueMap.keySet());
    for (final String id : ids) {
      createMetrics(id);
    }
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> ids = ImmutableSet.copyOf(queueMap.keySet());
    for (final String id : ids) {
      deregister(id);
    }
    if (!queueMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from consensus subscription prefetching queue metrics, queue map not empty");
    }
  }

  //////////////////////////// register & deregister ////////////////////////////

  public void register(final ConsensusPrefetchingQueue queue) {
    final String id = queue.getPrefetchingQueueId();
    queueMap.putIfAbsent(id, queue);
    if (Objects.nonNull(metricService)) {
      createMetrics(id);
    }
  }

  private void createMetrics(final String id) {
    createAutoGauge(id);
    createRate(id);
  }

  private void createAutoGauge(final String id) {
    final ConsensusPrefetchingQueue queue = queueMap.get(id);
    if (Objects.isNull(queue)) {
      return;
    }
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getSubscriptionUncommittedEventCount,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CURRENT_COMMIT_ID.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getCurrentCommitId,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_LAG.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getLag,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_WAL_GAP.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getWalGapSkippedEntries,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_ROUTING_EPOCH_CHANGE.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getEpochChangeCount,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_WATERMARK.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getMaxObservedTimestamp,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
  }

  private void createRate(final String id) {
    final ConsensusPrefetchingQueue queue = queueMap.get(id);
    if (Objects.isNull(queue)) {
      return;
    }
    rateMap.put(
        id,
        metricService.getOrCreateRate(
            Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            queue.getPrefetchingQueueId()));
  }

  public void deregister(final String id) {
    if (!queueMap.containsKey(id)) {
      LOGGER.warn(
          "Failed to deregister consensus subscription prefetching queue metrics, "
              + "ConsensusPrefetchingQueue({}) does not exist",
          id);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(id);
    }
    queueMap.remove(id);
  }

  private void removeMetrics(final String id) {
    removeAutoGauge(id);
    removeRate(id);
  }

  private void removeAutoGauge(final String id) {
    final ConsensusPrefetchingQueue queue = queueMap.get(id);
    if (Objects.isNull(queue)) {
      return;
    }
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CURRENT_COMMIT_ID.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CONSENSUS_LAG.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CONSENSUS_WAL_GAP.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CONSENSUS_ROUTING_EPOCH_CHANGE.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CONSENSUS_WATERMARK.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
  }

  private void removeRate(final String id) {
    final ConsensusPrefetchingQueue queue = queueMap.get(id);
    if (Objects.isNull(queue)) {
      return;
    }
    metricService.remove(
        MetricType.RATE,
        Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
  }

  public void mark(final String id, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final Rate rate = rateMap.get(id);
    if (rate == null) {
      LOGGER.warn(
          "Failed to mark transfer event rate, ConsensusPrefetchingQueue({}) does not exist", id);
      return;
    }
    rate.mark(size);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class Holder {

    private static final ConsensusSubscriptionPrefetchingQueueMetrics INSTANCE =
        new ConsensusSubscriptionPrefetchingQueueMetrics();

    private Holder() {}
  }

  public static ConsensusSubscriptionPrefetchingQueueMetrics getInstance() {
    return Holder.INSTANCE;
  }

  private ConsensusSubscriptionPrefetchingQueueMetrics() {}
}
