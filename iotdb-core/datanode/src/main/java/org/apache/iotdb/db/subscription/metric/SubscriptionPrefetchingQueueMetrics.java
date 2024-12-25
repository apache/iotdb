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
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingQueue;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriptionPrefetchingQueueMetrics implements IMetricSet {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPrefetchingQueueMetrics.class);

  private volatile AbstractMetricService metricService;

  private final Map<String, SubscriptionPrefetchingQueue> prefetchingQueueMap =
      new ConcurrentHashMap<>();

  private final Map<String, Rate> rateMap = new ConcurrentHashMap<>();

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> ids = ImmutableSet.copyOf(prefetchingQueueMap.keySet());
    for (final String id : ids) {
      createMetrics(id);
    }
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    final ImmutableSet<String> ids = ImmutableSet.copyOf(prefetchingQueueMap.keySet());
    for (final String id : ids) {
      deregister(id);
    }
    if (!prefetchingQueueMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from subscription prefetching queue metrics, prefetching queue map not empty");
    }
  }

  //////////////////////////// register & deregister ////////////////////////////

  public void register(@NonNull final SubscriptionPrefetchingQueue prefetchingQueue) {
    final String id = prefetchingQueue.getPrefetchingQueueId();
    prefetchingQueueMap.putIfAbsent(id, prefetchingQueue);
    if (Objects.nonNull(metricService)) {
      createMetrics(id);
    }
  }

  private void createMetrics(final String id) {
    createAutoGauge(id);
    createRate(id);
  }

  private void createAutoGauge(final String id) {
    final SubscriptionPrefetchingQueue queue = prefetchingQueueMap.get(id);
    // uncommited event count
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT.toString(),
        MetricLevel.IMPORTANT,
        queue,
        SubscriptionPrefetchingQueue::getSubscriptionUncommittedEventCount,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    // current commit id
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_CURRENT_COMMIT_ID.toString(),
        MetricLevel.IMPORTANT,
        queue,
        SubscriptionPrefetchingQueue::getCurrentCommitId,
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
  }

  private void createRate(final String id) {
    final SubscriptionPrefetchingQueue queue = prefetchingQueueMap.get(id);
    // transfer event rate
    rateMap.put(
        id,
        metricService.getOrCreateRate(
            Metric.SUBSCRIPTION_EVENT_TRANSFER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            queue.getPrefetchingQueueId()));
  }

  public void deregister(final String id) {
    if (!prefetchingQueueMap.containsKey(id)) {
      LOGGER.warn(
          "Failed to deregister subscription prefetching queue metrics, SubscriptionPrefetchingQueue({}) does not exist",
          id);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(id);
    }
    prefetchingQueueMap.remove(id);
  }

  private void removeMetrics(final String id) {
    removeAutoGauge(id);
    removeRate(id);
  }

  private void removeAutoGauge(final String id) {
    final SubscriptionPrefetchingQueue queue = prefetchingQueueMap.get(id);
    // uncommited event count
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_UNCOMMITTED_EVENT_COUNT.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
    // current commit id
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SUBSCRIPTION_CURRENT_COMMIT_ID.toString(),
        Tag.NAME.toString(),
        queue.getPrefetchingQueueId());
  }

  private void removeRate(final String id) {
    final SubscriptionPrefetchingQueue queue = prefetchingQueueMap.get(id);
    // transfer event rate
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
          "Failed to mark transfer event rate, SubscriptionPrefetchingQueue({}) does not exist",
          id);
      return;
    }
    rate.mark(size);
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionPrefetchingQueueMetricsHolder {

    private static final SubscriptionPrefetchingQueueMetrics INSTANCE =
        new SubscriptionPrefetchingQueueMetrics();

    private SubscriptionPrefetchingQueueMetricsHolder() {
      // empty constructor
    }
  }

  public static SubscriptionPrefetchingQueueMetrics getInstance() {
    return SubscriptionPrefetchingQueueMetrics.SubscriptionPrefetchingQueueMetricsHolder.INSTANCE;
  }

  private SubscriptionPrefetchingQueueMetrics() {
    // empty constructor
  }
}
