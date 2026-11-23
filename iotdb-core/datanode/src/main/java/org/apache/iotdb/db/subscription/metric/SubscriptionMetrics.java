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
import org.apache.iotdb.db.subscription.resource.SubscriptionDataNodeResourceManager;
import org.apache.iotdb.db.subscription.resource.SubscriptionMemoryManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

public class SubscriptionMetrics implements IMetricSet {

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(final AbstractMetricService metricService) {
    SubscriptionPrefetchingQueueMetrics.getInstance().bindTo(metricService);
    ConsensusSubscriptionPrefetchingQueueMetrics.getInstance().bindTo(metricService);
    // Read the shared allocator directly; queue snapshots taken at different times cannot be
    // summed to establish whether a DataNode exceeded its subscription memory budget.
    final SubscriptionMemoryManager memoryManager = SubscriptionDataNodeResourceManager.memory();
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_MEMORY_USED_BYTES.toString(),
        MetricLevel.IMPORTANT,
        memoryManager,
        SubscriptionMemoryManager::getUsedMemorySizeInBytes);
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_MEMORY_LIMIT_BYTES.toString(),
        MetricLevel.IMPORTANT,
        memoryManager,
        SubscriptionMemoryManager::getTotalMemorySizeInBytes);
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_MEMORY_OVERCOMMIT_BYTES.toString(),
        MetricLevel.IMPORTANT,
        memoryManager,
        SubscriptionMemoryManager::getOvercommitSizeInBytes);
    metricService.createAutoGauge(
        Metric.SUBSCRIPTION_MEMORY_OVERSIZED_ENTRY_COUNT.toString(),
        MetricLevel.IMPORTANT,
        memoryManager,
        SubscriptionMemoryManager::getOversizedEntryCount);
  }

  @Override
  public void unbindFrom(final AbstractMetricService metricService) {
    SubscriptionPrefetchingQueueMetrics.getInstance().unbindFrom(metricService);
    ConsensusSubscriptionPrefetchingQueueMetrics.getInstance().unbindFrom(metricService);
    metricService.remove(MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_MEMORY_USED_BYTES.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_MEMORY_LIMIT_BYTES.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_MEMORY_OVERCOMMIT_BYTES.toString());
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SUBSCRIPTION_MEMORY_OVERSIZED_ENTRY_COUNT.toString());
  }

  //////////////////////////// singleton ////////////////////////////

  private static class SubscriptionMetricsHolder {

    private static final SubscriptionMetrics INSTANCE = new SubscriptionMetrics();

    private SubscriptionMetricsHolder() {
      // empty constructor
    }
  }

  public static SubscriptionMetrics getInstance() {
    return SubscriptionMetrics.SubscriptionMetricsHolder.INSTANCE;
  }

  private SubscriptionMetrics() {
    // empty constructor
  }
}
