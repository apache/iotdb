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

package org.apache.iotdb.metrics.metricsets.cpu;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.List;
import java.util.function.UnaryOperator;

public class CpuUsageMetrics implements IMetricSet {

  private static final String CPU_USAGE = "cpu_usage";
  private static final String POOL = "pool";
  private static final String MODULE = "module";
  private static final String USER_TIME_PERCENTAGE = "user_time_percentage";
  private final List<String> modules;
  private final List<String> pools;
  private final CpuUsageManager cpuUsageManager;

  public CpuUsageMetrics(
      List<String> modules,
      List<String> pools,
      UnaryOperator<String> threadNameToModule,
      UnaryOperator<String> threadNameToPool) {
    this.modules = modules;
    this.pools = pools;
    cpuUsageManager = new CpuUsageManager(threadNameToModule, threadNameToPool);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    cpuUsageManager.setMetricService(metricService);
    for (String moduleName : modules) {
      metricService.createAutoGauge(
          CPU_USAGE,
          MetricLevel.IMPORTANT,
          cpuUsageManager,
          x -> x.getModuleCpuUsage().getOrDefault(moduleName, 0.0),
          MODULE,
          moduleName);
    }
    for (String poolName : pools) {
      metricService.createAutoGauge(
          CPU_USAGE,
          MetricLevel.IMPORTANT,
          cpuUsageManager,
          x -> x.getPoolCpuUsage().getOrDefault(poolName, 0.0),
          POOL,
          poolName);
      metricService.createAutoGauge(
          USER_TIME_PERCENTAGE,
          MetricLevel.IMPORTANT,
          cpuUsageManager,
          x -> x.getPoolUserCpuPercentage().getOrDefault(poolName, 0.0),
          POOL,
          poolName);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String moduleName : modules) {
      metricService.remove(MetricType.AUTO_GAUGE, CPU_USAGE, MODULE, moduleName);
    }
    for (String poolName : pools) {
      metricService.remove(MetricType.AUTO_GAUGE, CPU_USAGE, POOL, poolName);
      metricService.remove(MetricType.AUTO_GAUGE, USER_TIME_PERCENTAGE, POOL, poolName);
    }
  }
}
