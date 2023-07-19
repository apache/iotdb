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

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javafx.util.Pair;

public class ClientManagerMetrics implements IMetricSet {
  private static final String CLIENT_MANAGER_NUM_ACTIVE = "client_manager_num_active";
  private static final String CLIENT_MANAGER_NUM_IDLE = "client_manager_num_idle";
  private static final String CLIENT_MANAGER_BORROWED_COUNT = "client_manager_borrowed_count";
  private static final String CLIENT_MANAGER_CREATED_COUNT = "client_manager_created_count";
  private static final String CLIENT_MANAGER_DESTROYED_COUNT = "client_manager_destroyed_count";
  private static final String MEAN_ACTIVE_TIME_MILLIS = "client_manager_mean_active_time";
  private static final String MEAN_BORROW_WAIT_TIME_MILLIS = "client_manager_mean_borrow_wait_time";
  private static final String MEAN_IDLE_TIME_MILLIS = "client_manager_mean_idle_time";

  private static final String POOL_NAME = "pool_name";
  private final ArrayList<Pair<String, GenericKeyedObjectPool<?, ?>>> registeredClientPool =
      new ArrayList<>();

  private static final List<String> POOL_NAME_LIST = new ArrayList<>();

  static {
    POOL_NAME_LIST.add("AsyncConfigNodeHeartbeatServiceClientPool");
    POOL_NAME_LIST.add("AsyncConfigNodeIServiceClientPool");
    POOL_NAME_LIST.add("AsyncDataNodeHeartbeatServiceClientPool");
    POOL_NAME_LIST.add("AsyncDataNodeInternalServiceClientPool");
    POOL_NAME_LIST.add("AsyncDataNodeMPPDataExchangeServiceClientPool");
    POOL_NAME_LIST.add("AsyncIoTConsensusServiceClientPool");
    POOL_NAME_LIST.add("AsyncPipeDataTransferServiceClientPool");
    POOL_NAME_LIST.add("ClusterDeletionConfigNodeClientPool");
    POOL_NAME_LIST.add("ConfigNodeClientPool");
    POOL_NAME_LIST.add("RatisClientPool");
    POOL_NAME_LIST.add("SyncConfigNodeIServiceClientPool");
    POOL_NAME_LIST.add("SyncDataNodeInternalServiceClientPool");
    POOL_NAME_LIST.add("SyncDataNodeMPPDataExchangeServiceClientPool");
    POOL_NAME_LIST.add("SyncIoTConsensusServiceClientPool");
  }

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

  @Override
  public void bindTo(AbstractMetricService metricService) {
    synchronized (this) {
      for (String poolName : POOL_NAME_LIST) {
        metricService.createAutoGauge(
            CLIENT_MANAGER_NUM_ACTIVE,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getNumActive(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            CLIENT_MANAGER_NUM_IDLE,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getNumIdle(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            CLIENT_MANAGER_BORROWED_COUNT,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getBorrowedCount(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            CLIENT_MANAGER_CREATED_COUNT,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getCreatedCount(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            CLIENT_MANAGER_DESTROYED_COUNT,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getDestroyedCount(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            MEAN_ACTIVE_TIME_MILLIS,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getMeanActiveTime(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            MEAN_BORROW_WAIT_TIME_MILLIS,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getMeanBorrowWaitTime(poolName),
            POOL_NAME,
            poolName);
        metricService.createAutoGauge(
            MEAN_IDLE_TIME_MILLIS,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> getMeanIdleTime(poolName),
            POOL_NAME,
            poolName);
      }
    }
  }

  private long getNumActive(String poolName) {
    AtomicLong numActive = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        numActive.addAndGet(pair.getValue().getNumActive());
      }
    }
    return numActive.get();
  }

  private long getNumIdle(String poolName) {
    AtomicLong numIdle = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        numIdle.addAndGet(pair.getValue().getNumIdle());
      }
    }
    return numIdle.get();
  }

  private long getBorrowedCount(String poolName) {
    AtomicLong borrowedCount = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        borrowedCount.addAndGet(pair.getValue().getBorrowedCount());
      }
    }
    return borrowedCount.get();
  }

  private long getCreatedCount(String poolName) {
    AtomicLong createdCount = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        createdCount.addAndGet(pair.getValue().getCreatedCount());
      }
    }
    return createdCount.get();
  }

  private long getDestroyedCount(String poolName) {
    AtomicLong destroyedCount = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        destroyedCount.addAndGet(pair.getValue().getDestroyedCount());
      }
    }
    return destroyedCount.get();
  }

  private long getMeanActiveTime(String poolName) {
    AtomicLong activeTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        activeTime.addAndGet(pair.getValue().getMeanActiveTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? activeTime.get() / count.get() : 0;
  }

  private long getMeanBorrowWaitTime(String poolName) {
    AtomicLong borrowWaitTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        borrowWaitTime.addAndGet(pair.getValue().getMeanBorrowWaitTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? borrowWaitTime.get() / count.get() : 0;
  }

  private long getMeanIdleTime(String poolName) {
    AtomicLong idleTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (Pair<String, GenericKeyedObjectPool<?, ?>> pair : registeredClientPool) {
      if (pair.getKey().equals(poolName)) {
        idleTime.addAndGet(pair.getValue().getMeanIdleTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? idleTime.get() / count.get() : 0;
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String poolName : POOL_NAME_LIST) {
      metricService.remove(MetricType.GAUGE, CLIENT_MANAGER_NUM_ACTIVE, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, CLIENT_MANAGER_NUM_IDLE, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, CLIENT_MANAGER_BORROWED_COUNT, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, CLIENT_MANAGER_CREATED_COUNT, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, CLIENT_MANAGER_DESTROYED_COUNT, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, MEAN_ACTIVE_TIME_MILLIS, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, MEAN_BORROW_WAIT_TIME_MILLIS, POOL_NAME, poolName);
      metricService.remove(MetricType.GAUGE, MEAN_IDLE_TIME_MILLIS, POOL_NAME, poolName);
    }
  }

  public void registerClientManager(String poolName, GenericKeyedObjectPool<?, ?> clientPool) {
    synchronized (this) {
      registeredClientPool.add(new Pair<>(poolName, clientPool));
    }
  }
}
