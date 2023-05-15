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

  private final List<String> modules;
  private final UnaryOperator<String> threadNameToModule;
  private final String cpuUsage = "cpu_usage";
  private final String module = "module";
  private final AbstractCpuUsageMetricsManager cpuUsageMetricsManager;

  public CpuUsageMetrics(List<String> modules, UnaryOperator<String> threadNameToModule) {
    this.modules = modules;
    this.threadNameToModule = threadNameToModule;
    cpuUsageMetricsManager =
        AbstractCpuUsageMetricsManager.getCpuUsageMetricsManager(threadNameToModule);
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (String moduleName : modules) {
      metricService.createAutoGauge(
          cpuUsage,
          MetricLevel.IMPORTANT,
          cpuUsageMetricsManager,
          x -> x.getCpuUsageForPerModule().getOrDefault(moduleName, 0.0),
          module,
          moduleName);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (String moduleName : modules) {
      metricService.remove(MetricType.AUTO_GAUGE, cpuUsage, module, moduleName);
    }
  }
}
