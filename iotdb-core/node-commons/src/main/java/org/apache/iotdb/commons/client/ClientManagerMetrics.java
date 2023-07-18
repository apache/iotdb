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

import org.apache.commons.pool2.KeyedObjectPool;

import java.util.HashMap;
import java.util.Map;

public class ClientManagerMetrics implements IMetricSet {
  private static final String CLIENT_MANAGER_NUM_ACTIVE = "client_manager_num_active";
  private static final String CLIENT_MANAGER_NUM_IDLE = "client_manager_num_idle";
  private static final String POOL_NAME = "pool_name";
  private static final String CALLER_NAME = "caller_name";
  private AbstractMetricService metricService;
  private Map<String, KeyedObjectPool<?, ?>> registeredClientPool = new HashMap<>();
  private Map<String, KeyedObjectPool<?, ?>> notRegisteredClientPool = new HashMap<>();

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
      this.metricService = metricService;
      for (Map.Entry<String, KeyedObjectPool<?, ?>> entry : notRegisteredClientPool.entrySet()) {
        int index = entry.getKey().indexOf("-");
        metricService.createAutoGauge(
            CLIENT_MANAGER_NUM_ACTIVE,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> entry.getValue().getNumActive(),
            POOL_NAME,
            entry.getKey().substring(0, index),
            CALLER_NAME,
            entry.getKey().substring(index + 1));
      }
      registeredClientPool.putAll(notRegisteredClientPool);
      notRegisteredClientPool.clear();
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {}

  public void registerClientManager(
      String poolName, String callerName, KeyedObjectPool<?, ?> clientPool) {
    synchronized (this) {
      String name = poolName + "-" + callerName;
      if (metricService == null) {
        notRegisteredClientPool.put(name, clientPool);
      } else {
        registeredClientPool.put(name, clientPool);
        metricService.createAutoGauge(
            CLIENT_MANAGER_NUM_ACTIVE,
            MetricLevel.IMPORTANT,
            registeredClientPool,
            map -> registeredClientPool.get(name).getNumActive(),
            POOL_NAME,
            poolName,
            CALLER_NAME,
            callerName);
      }
    }
  }
}
