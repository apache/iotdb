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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import java.util.ArrayList;
import java.util.List;
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

  private String role;
  private final ArrayList<String> registeredClientName = new ArrayList<>();
  private final ArrayList<GenericKeyedObjectPool<?, ?>> registeredClientPool = new ArrayList<>();
  private static final List<String> POOL_NAME_LIST = new ArrayList<>();

  private static class ClientManagerMetricsHolder {
    private static ClientManagerMetrics INSTANCE;

    private ClientManagerMetricsHolder() {}

    private static ClientManagerMetrics getInstance(String role) {
      synchronized (ClientManagerMetrics.class) {
        if (INSTANCE == null) {
          INSTANCE = new ClientManagerMetrics(role);
        }
        return INSTANCE;
      }
    }

    private static ClientManagerMetrics getInstance() {
      synchronized (ClientManagerMetrics.class) {
        return INSTANCE;
      }
    }
  }

  public static ClientManagerMetrics getInstance(String role) {
    return ClientManagerMetrics.ClientManagerMetricsHolder.getInstance(role);
  }

  public static ClientManagerMetrics getInstance() {
    return ClientManagerMetrics.ClientManagerMetricsHolder.getInstance();
  }

  private ClientManagerMetrics(String role) {
    this.role = role;
    if (role.equals(IoTDBConstant.CN_ROLE)) {
      // ConfigNode list
      POOL_NAME_LIST.add("AsyncConfigNodeHeartbeatServiceClientPool");
      POOL_NAME_LIST.add("AsyncConfigNodeIServiceClientPool");
      POOL_NAME_LIST.add("AsyncDataNodeHeartbeatServiceClientPool");
      POOL_NAME_LIST.add("AsyncDataNodeInternalServiceClientPool");
      POOL_NAME_LIST.add("SyncConfigNodeIServiceClientPool");
      POOL_NAME_LIST.add("SyncDataNodeInternalServiceClientPool");
      POOL_NAME_LIST.add("SyncDataNodeMPPDataExchangeServiceClientPool");
    } else {
      // DataNode list
      POOL_NAME_LIST.add("AsyncDataNodeInternalServiceClientPool");
      POOL_NAME_LIST.add("AsyncDataNodeMPPDataExchangeServiceClientPool");
      POOL_NAME_LIST.add("AsyncIoTConsensusServiceClientPool");
      POOL_NAME_LIST.add("AsyncPipeDataTransferServiceClientPool");
      POOL_NAME_LIST.add("ClusterDeletionConfigNodeClientPool");
      POOL_NAME_LIST.add("ConfigNodeClientPool");
      POOL_NAME_LIST.add("RatisClientPool");
      POOL_NAME_LIST.add("SyncDataNodeInternalServiceClientPool");
      POOL_NAME_LIST.add("SyncDataNodeMPPDataExchangeServiceClientPool");
      POOL_NAME_LIST.add("SyncIoTConsensusServiceClientPool");
    }
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    synchronized (this) {
      for (String poolName : POOL_NAME_LIST) {
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
    }
  }

  private long getNumActive(String poolName) {
    AtomicLong numActive = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).equals(poolName)) {
        numActive.addAndGet(registeredClientPool.get(i).getNumActive());
      }
    }
    return numActive.get();
  }

  private long getNumIdle(String poolName) {
    AtomicLong numIdle = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).equals(poolName)) {
        numIdle.addAndGet(registeredClientPool.get(i).getNumIdle());
      }
    }
    return numIdle.get();
  }

  private long getBorrowedCount(String poolName) {
    AtomicLong borrowedCount = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).equals(poolName)) {
        borrowedCount.addAndGet(registeredClientPool.get(i).getBorrowedCount());
      }
    }
    return borrowedCount.get();
  }

  private long getCreatedCount(String poolName) {
    AtomicLong createdCount = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).equals(poolName)) {
        createdCount.addAndGet(registeredClientPool.get(i).getCreatedCount());
      }
    }
    return createdCount.get();
  }

  private long getDestroyedCount(String poolName) {
    AtomicLong destroyedCount = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).equals(poolName)) {
        destroyedCount.addAndGet(registeredClientPool.get(i).getDestroyedCount());
      }
    }
    return destroyedCount.get();
  }

  private long getMeanActiveTime(String poolName) {
    AtomicLong activeTime = new AtomicLong();
    AtomicLong count = new AtomicLong();
    for (int i = 0; i < registeredClientName.size(); i++) {
      if (registeredClientName.get(i).equals(poolName)) {
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
      if (registeredClientName.get(i).equals(poolName)) {
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
      if (registeredClientName.get(i).equals(poolName)) {
        idleTime.addAndGet(registeredClientPool.get(i).getMeanIdleTimeMillis());
        count.addAndGet(1);
      }
    }
    return count.get() > 0 ? idleTime.get() / count.get() : 0;
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String poolName : POOL_NAME_LIST) {
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
  }

  public void registerClientManager(String poolName, GenericKeyedObjectPool<?, ?> clientPool) {
    synchronized (this) {
      registeredClientName.add(poolName);
      registeredClientPool.add(clientPool);
    }
  }
}
