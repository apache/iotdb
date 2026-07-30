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
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
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

  private final Map<QueueMetricsKey, ConsensusPrefetchingQueue> queueMap =
      new ConcurrentHashMap<>();

  private final Map<QueueMetricsKey, Rate> rateMap = new ConcurrentHashMap<>();

  @Override
  public synchronized void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<QueueMetricsKey> keys = ImmutableSet.copyOf(queueMap.keySet());
    for (final QueueMetricsKey key : keys) {
      createMetrics(key);
    }
  }

  @Override
  public synchronized void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<QueueMetricsKey> keys = ImmutableSet.copyOf(queueMap.keySet());
    for (final QueueMetricsKey key : keys) {
      deregister(key);
    }
    if (!queueMap.isEmpty()) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_UNBIND_FROM_CONSENSUS_SUBSCRIPTION_PREFETCHING_A8F920D9);
    }
  }

  //////////////////////////// register & deregister ////////////////////////////

  public synchronized void register(final ConsensusPrefetchingQueue queue) {
    final QueueMetricsKey key = QueueMetricsKey.from(queue);
    if (Objects.isNull(queueMap.putIfAbsent(key, queue)) && Objects.nonNull(metricService)) {
      createMetrics(key);
    }
  }

  private void createMetrics(final QueueMetricsKey key) {
    createAutoGauge(key);
    createRate(key);
  }

  private void createAutoGauge(final QueueMetricsKey key) {
    final ConsensusPrefetchingQueue queue = queueMap.get(key);
    if (Objects.isNull(queue)) {
      return;
    }
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getSubscriptionUncommittedEventCount,
        key.getTags());
    // Keep the legacy metric name for dashboard compatibility, but expose seek generation here.
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CURRENT_COMMIT_ID.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getCurrentSeekGeneration,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_SEEK_GENERATION.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getCurrentSeekGeneration,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_LAG.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getLag,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_WAL_GAP.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getWalGapSkippedEntries,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_ROUTING_EPOCH_CHANGE.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getEpochChangeCount,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_WATERMARK.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getMaxObservedTimestamp,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_ACTIVE.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getActiveStatus,
        key.getTags());
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CONSENSUS_INITIALIZED.toString(),
        MetricLevel.IMPORTANT,
        queue,
        ConsensusPrefetchingQueue::getInitializedStatus,
        key.getTags());
  }

  private void createRate(final QueueMetricsKey key) {
    final ConsensusPrefetchingQueue queue = queueMap.get(key);
    if (Objects.isNull(queue)) {
      return;
    }
    rateMap.put(
        key,
        metricService.getOrCreateRate(
            Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(), MetricLevel.IMPORTANT, key.getTags()));
  }

  public synchronized void deregister(final ConsensusPrefetchingQueue queue) {
    final QueueMetricsKey key = QueueMetricsKey.from(queue);
    if (queueMap.get(key) != queue) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_DEREGISTER_CONSENSUS_SUBSCRIPTION_PREFETCHING_8B180091,
          key);
      return;
    }
    deregister(key);
  }

  private void deregister(final QueueMetricsKey key) {
    if (!queueMap.containsKey(key)) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_DEREGISTER_CONSENSUS_SUBSCRIPTION_PREFETCHING_8B180091,
          key);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(key);
    }
    queueMap.remove(key);
  }

  private void removeMetrics(final QueueMetricsKey key) {
    removeAutoGauge(key);
    removeRate(key);
  }

  private void removeAutoGauge(final QueueMetricsKey key) {
    if (!queueMap.containsKey(key)) {
      return;
    }
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT.toString(),
        key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_CURRENT_COMMIT_ID.toString(), key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CONSENSUS_SEEK_GENERATION.toString(),
        key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_CONSENSUS_LAG.toString(), key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_CONSENSUS_WAL_GAP.toString(), key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CONSENSUS_ROUTING_EPOCH_CHANGE.toString(),
        key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_CONSENSUS_WATERMARK.toString(), key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_CONSENSUS_ACTIVE.toString(), key.getTags());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_CONSENSUS_INITIALIZED.toString(), key.getTags());
  }

  private void removeRate(final QueueMetricsKey key) {
    if (!queueMap.containsKey(key)) {
      return;
    }
    metricService.remove(
        MetricType.RATE, Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(), key.getTags());
    rateMap.remove(key);
  }

  public void mark(final String id, final String regionId, final long size) {
    if (Objects.isNull(metricService)) {
      return;
    }
    final QueueMetricsKey key = new QueueMetricsKey(id, regionId);
    final Rate rate = rateMap.get(key);
    if (rate == null) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_MARK_TRANSFER_EVENT_RATE_CONSENSUSPREFETCHINGQUEUE_FE9B91C3,
          key);
      return;
    }
    rate.mark(size);
  }

  private static final class QueueMetricsKey {

    private final String queueId;
    private final String regionId;

    private QueueMetricsKey(final String queueId, final String regionId) {
      this.queueId = queueId;
      this.regionId = regionId;
    }

    private static QueueMetricsKey from(final ConsensusPrefetchingQueue queue) {
      return new QueueMetricsKey(
          queue.getPrefetchingQueueId(), queue.getConsensusGroupId().toString());
    }

    private String[] getTags() {
      return new String[] {
        Tag.NAME.toString(), queueId, Tag.REGION.toString(), regionId,
      };
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof QueueMetricsKey)) {
        return false;
      }
      final QueueMetricsKey that = (QueueMetricsKey) obj;
      return Objects.equals(queueId, that.queueId) && Objects.equals(regionId, that.regionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(queueId, regionId);
    }

    @Override
    public String toString() {
      return queueId + "/" + regionId;
    }
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
