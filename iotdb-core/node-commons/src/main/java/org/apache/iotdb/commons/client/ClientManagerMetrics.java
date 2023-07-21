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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ClientManagerMetrics implements IMetricSet {
  private static final String CLIENT_MANAGER_NUM_ACTIVE = "client_manager_num_active";
  private static final String CLIENT_MANAGER_NUM_IDLE = "client_manager_num_idle";
  private static final String CLIENT_MANAGER_BORROWED_COUNT = "client_manager_borrowed_count";
  private static final String CLIENT_MANAGER_CREATED_COUNT = "client_manager_created_count";
  private static final String CLIENT_MANAGER_DESTROYED_COUNT = "client_manager_destroyed_count";
  private static final String MEAN_ACTIVE_TIME_MILLIS = "client_manager_mean_active_time";
  private static final String MEAN_BORROW_WAIT_TIME_MILLIS = "client_manager_mean_borrow_wait_time";
  private static final String MEAN_IDLE_TIME_MILLIS = "client_manager_mean_idle_time";

  private final ArrayList<String> registeredClientName = new ArrayList<>();
  private final ArrayList<GenericKeyedObjectPool<?, ?>> registeredClientPool = new ArrayList<>();
  private final Set<String> poolNameSet = new HashSet<>();
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
      registeredClientName.add(poolName);
      registeredClientPool.add(clientPool);
      if (metricService == null) {
        poolNameSet.add(poolName);
      } else {
        if (!poolNameSet.contains(poolName)) {
          createMetrics(poolName);
        }
      }
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (String poolName : poolNameSet) {
        createMetrics(poolName);
      }
    }
  }

  private void createMetrics(String poolName) {
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getNumActive(poolName),
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_ACTIVE,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getNumIdle(poolName),
        Tag.NAME.toString(),
        CLIENT_MANAGER_NUM_IDLE,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getBorrowedCount(poolName),
        Tag.NAME.toString(),
        CLIENT_MANAGER_BORROWED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getCreatedCount(poolName),
        Tag.NAME.toString(),
        CLIENT_MANAGER_CREATED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getDestroyedCount(poolName),
        Tag.NAME.toString(),
        CLIENT_MANAGER_DESTROYED_COUNT,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getMeanActiveTime(poolName),
        Tag.NAME.toString(),
        MEAN_ACTIVE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getMeanBorrowWaitTime(poolName),
        Tag.NAME.toString(),
        MEAN_BORROW_WAIT_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
    metricService.createAutoGauge(
        Metric.CLIENT_MANAGER.toString(),
        MetricLevel.IMPORTANT,
        registeredClientPool,
        map -> getMeanIdleTime(poolName),
        Tag.NAME.toString(),
        MEAN_IDLE_TIME_MILLIS,
        Tag.TYPE.toString(),
        poolName);
  }

  private long getNumActive(String poolName) {
    AtomicLong numActive = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        numActive.addAndGet(registeredClientPool.get(i).getNumActive());
      }
    }
    return numActive.get();
  }

  private long getNumIdle(String poolName) {
    AtomicLong numIdle = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        numIdle.addAndGet(registeredClientPool.get(i).getNumIdle());
      }
    }
    return numIdle.get();
  }

  private long getBorrowedCount(String poolName) {
    AtomicLong borrowedCount = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        borrowedCount.addAndGet(registeredClientPool.get(i).getBorrowedCount());
      }
    }
    return borrowedCount.get();
  }

  private long getCreatedCount(String poolName) {
    AtomicLong createdCount = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        createdCount.addAndGet(registeredClientPool.get(i).getCreatedCount());
      }
    }
    return createdCount.get();
  }

  private long getDestroyedCount(String poolName) {
    AtomicLong destroyedCount = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        destroyedCount.addAndGet(registeredClientPool.get(i).getDestroyedCount());
      }
    }
    return destroyedCount.get();
  }

  private long getMeanActiveTime(String poolName) {
    AtomicLong activeTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        activeTime.addAndGet(registeredClientPool.get(i).getMeanActiveTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? activeTime.get() / count.get() : 0;
  }

  private long getMeanBorrowWaitTime(String poolName) {
    AtomicLong borrowWaitTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        borrowWaitTime.addAndGet(registeredClientPool.get(i).getMeanBorrowWaitTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? borrowWaitTime.get() / count.get() : 0;
  }

  private long getMeanIdleTime(String poolName) {
    AtomicLong idleTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).contains(poolName)) {
        idleTime.addAndGet(registeredClientPool.get(i).getMeanIdleTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? idleTime.get() / count.get() : 0;
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String poolName : poolNameSet) {
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
    registeredClientName.clear();
    registeredClientPool.clear();
    poolNameSet.clear();
  }
}
