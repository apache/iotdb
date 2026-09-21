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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ClientManagerMetrics implements IMetricSet {
  private static final String CLIENT_MANAGER_NUM_ACTIVE = "client_manager_num_active";
  private static final String CLIENT_MANAGER_NUM_IDLE = "client_manager_num_idle";
  private static final String CLIENT_MANAGER_BORROWED_COUNT = "client_manager_borrowed_count";
  private static final String CLIENT_MANAGER_CREATED_COUNT = "client_manager_created_count";
  private static final String CLIENT_MANAGER_DESTROYED_COUNT = "client_manager_destroyed_count";
  private static final String MEAN_ACTIVE_TIME_MILLIS = "client_manager_mean_active_time";
  private static final String MEAN_BORROW_WAIT_TIME_MILLIS = "client_manager_mean_borrow_wait_time";
  private static final String MEAN_IDLE_TIME_MILLIS = "client_manager_mean_idle_time";

  private final Map<String, GenericKeyedObjectPool<?, ?>> poolMap = new HashMap<>();
  private AbstractMetricService metricService;

  private static class ClientManagerMetricsHolder {
    private static final ClientManagerMetrics INSTANCE = new ClientManagerMetrics();

    private ClientManagerMetricsHolder() {}
  }

  public static ClientManagerMetrics getInstance() {
    return ClientManagerMetrics.ClientManagerMetricsHolder.INSTANCE;
  }

  private ClientManagerMetrics() {
    // empty constructor
  }

  public synchronized void registerClientManager(
      String poolName, GenericKeyedObjectPool<?, ?> clientPool) {
    GenericKeyedObjectPool<?, ?> existingPool = poolMap.get(poolName);
    if (metricService != null && existingPool != null && !existingPool.isClosed()) {
      return;
    }
    if (metricService != null && existingPool != null) {
      removeMetrics(metricService, poolName);
    }
    poolMap.put(poolName, clientPool);
    if (metricService != null) {
      createMetrics(poolName, clientPool);
    }
  }

  public synchronized void unregisterClientManager(GenericKeyedObjectPool<?, ?> clientPool) {
    Iterator<Map.Entry<String, GenericKeyedObjectPool<?, ?>>> iterator =
        poolMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, GenericKeyedObjectPool<?, ?>> entry = iterator.next();
      // A pool with the same name may have been registered while the old pool was closing.
      if (entry.getValue() == clientPool) {
        iterator.remove();
        if (metricService != null) {
          removeMetrics(metricService, entry.getKey());
        }
      }
    }
  }

  @Override
  public synchronized void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    for (Map.Entry<String, GenericKeyedObjectPool<?, ?>> entry : poolMap.entrySet()) {
      createMetrics(entry.getKey(), entry.getValue());
    }
  }

  private void createMetrics(String poolName, GenericKeyedObjectPool<?, ?> clientPool) {
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getNumActive,
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_ACTIVE,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getNumIdle,
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_IDLE,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getBorrowedCount,
        Tag.NAME.toString(),
        CLIENT_MANAGER_BORROWED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getCreatedCount,
        Tag.NAME.toString(),
        CLIENT_MANAGER_CREATED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getDestroyedCount,
        Tag.NAME.toString(),
        CLIENT_MANAGER_DESTROYED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getMeanActiveTimeMillis,
        Tag.NAME.toString(),
        MEAN_ACTIVE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getMeanBorrowWaitTimeMillis,
        Tag.NAME.toString(),
        MEAN_BORROW_WAIT_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        clientPool,
        GenericKeyedObjectPool::getMeanIdleTimeMillis,
        Tag.NAME.toString(),
        MEAN_IDLE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
  }

  @Override
  public synchronized void unbindFrom(AbstractMetricService metricService) {
    if (this.metricService != metricService) {
      return;
    }
    this.metricService = null;
    // Keep live pools registered so a metric service restart can bind them again.
    for (String poolName : poolMap.keySet()) {
      removeMetrics(metricService, poolName);
    }
  }

  private void removeMetrics(AbstractMetricService metricService, String poolName) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_ACTIVE,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_IDLE,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_BORROWED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_CREATED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_DESTROYED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        MEAN_ACTIVE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        MEAN_BORROW_WAIT_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.CLIENT_MANAGER.toString(),
        Tag.NAME.toString(),
        MEAN_IDLE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
  }
}
