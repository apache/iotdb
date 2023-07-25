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

  public void registerClientManager(String poolName, GenericKeyedObjectPool<?, ?> clientPool) {
    synchronized (this) {
      if (metricService == null) {
        poolMap.put(poolName, clientPool);
      } else {
        if (!poolMap.containsKey(poolName)) {
          poolMap.put(poolName, clientPool);
          createMetrics(poolName);
        }
      }
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String poolName : poolMap.keySet()) {
        createMetrics(poolName);
      }
    }
  }

  private void createMetrics(String poolName) {
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getNumActive(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_ACTIVE,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getNumIdle(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_IDLE,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getBorrowedCount(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_BORROWED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getCreatedCount(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_CREATED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getDestroyedCount(),
        Tag.NAME.toString(),
        CLIENT_MANAGER_DESTROYED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getMeanActiveTimeMillis(),
        Tag.NAME.toString(),
        MEAN_ACTIVE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getMeanBorrowWaitTimeMillis(),
        Tag.NAME.toString(),
        MEAN_BORROW_WAIT_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        poolMap,
        map -> poolMap.get(poolName).getMeanIdleTimeMillis(),
        Tag.NAME.toString(),
        MEAN_IDLE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String poolName : poolMap.keySet()) {
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          CLIENT_MANAGER_NUM_ACTIVE,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          CLIENT_MANAGER_NUM_IDLE,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          CLIENT_MANAGER_BORROWED_COUNT,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          CLIENT_MANAGER_CREATED_COUNT,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          CLIENT_MANAGER_DESTROYED_COUNT,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          MEAN_ACTIVE_TIME_MILLIS,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          MEAN_BORROW_WAIT_TIME_MILLIS,
          Tag.TYPE.toString(),
          poolName);
      metricService.remove(
          MetricType.GAUGE,
          Metric.CLIENT_MANAGER.toString(),
          Tag.NAME.toString(),
          MEAN_IDLE_TIME_MILLIS,
          Tag.TYPE.toString(),
          poolName);
    }
    poolMap.clear();
  }
}
