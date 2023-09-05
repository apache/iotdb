/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.concurrent;

import org.apache.iotdb.commons.concurrent.threadpool.IThreadPoolMBean;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("java:S6548")
public class ThreadPoolMetrics implements IMetricSet {
  private AbstractMetricService metricService;
  private Map<String, IThreadPoolMBean> notRegisteredPoolMap = new HashMap<>();
  private Map<String, IThreadPoolMBean> registeredPoolMap = new HashMap<>();

  public static ThreadPoolMetrics getInstance() {
    return ThreadPoolMetricsHolder.INSTANCE;
  }

  private ThreadPoolMetrics() {}

  public void registerThreadPool(IThreadPoolMBean pool, String name) {
    synchronized (this) {
      if (metricService == null) {
        notRegisteredPoolMap.put(name, pool);
      } else {
        registeredPoolMap.put(name, pool);
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_ACTIVE_THREAD_COUNT.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> registeredPoolMap.get(name).getActiveCount(),
            SystemTag.POOL_NAME.toString(),
            name);
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_CORE_SIZE.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> registeredPoolMap.get(name).getCorePoolSize(),
            SystemTag.POOL_NAME.toString(),
            name);
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_WAITING_TASK_COUNT.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> registeredPoolMap.get(name).getQueueLength(),
            SystemTag.POOL_NAME.toString(),
            name);
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_DONE_TASK_COUNT.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> registeredPoolMap.get(name).getCompletedTaskCount(),
            SystemTag.POOL_NAME.toString(),
            name);
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_LARGEST_POOL_SIZE.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> registeredPoolMap.get(name).getLargestPoolSize(),
            SystemTag.POOL_NAME.toString(),
            name);
      }
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    synchronized (this) {
      this.metricService = metricService;
      for (Map.Entry<String, IThreadPoolMBean> entry : notRegisteredPoolMap.entrySet()) {
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_ACTIVE_THREAD_COUNT.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> entry.getValue().getActiveCount(),
            SystemTag.POOL_NAME.toString(),
            entry.getKey());
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_CORE_SIZE.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> entry.getValue().getCorePoolSize(),
            SystemTag.POOL_NAME.toString(),
            entry.getKey());
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_WAITING_TASK_COUNT.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> entry.getValue().getQueue().size(),
            SystemTag.POOL_NAME.toString(),
            entry.getKey());
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_DONE_TASK_COUNT.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> entry.getValue().getCompletedTaskCount(),
            SystemTag.POOL_NAME.toString(),
            entry.getKey());
        metricService.createAutoGauge(
            SystemMetric.THREAD_POOL_LARGEST_POOL_SIZE.toString(),
            MetricLevel.IMPORTANT,
            registeredPoolMap,
            map -> entry.getValue().getLargestPoolSize(),
            SystemTag.POOL_NAME.toString(),
            entry.getKey());
      }
      registeredPoolMap.putAll(notRegisteredPoolMap);
      notRegisteredPoolMap.clear();
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (Map.Entry<String, IThreadPoolMBean> entry : registeredPoolMap.entrySet()) {
      metricService.remove(
          MetricType.GAUGE,
          SystemMetric.THREAD_POOL_ACTIVE_THREAD_COUNT.toString(),
          SystemTag.POOL_NAME.toString(),
          entry.getKey());
      metricService.remove(
          MetricType.GAUGE,
          SystemMetric.THREAD_POOL_CORE_SIZE.toString(),
          SystemTag.POOL_NAME.toString(),
          entry.getKey());
      metricService.remove(
          MetricType.GAUGE,
          SystemMetric.THREAD_POOL_WAITING_TASK_COUNT.toString(),
          SystemTag.POOL_NAME.toString(),
          entry.getKey());
      metricService.remove(
          MetricType.GAUGE,
          SystemMetric.THREAD_POOL_DONE_TASK_COUNT.toString(),
          SystemTag.POOL_NAME.toString(),
          entry.getKey());
      metricService.remove(
          MetricType.GAUGE,
          SystemMetric.THREAD_POOL_LARGEST_POOL_SIZE.toString(),
          SystemTag.POOL_NAME.toString(),
          entry.getKey());
    }
  }

  private static class ThreadPoolMetricsHolder {
    private static final ThreadPoolMetrics INSTANCE = new ThreadPoolMetrics();

    private ThreadPoolMetricsHolder() {}
  }
}
