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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

public class AbstractCpuUsageMetricsManager {
  protected AbstractMetricService metricService;
  protected final UnaryOperator<String> threadNameToModule;
  protected final UnaryOperator<String> threadNameToPool;
  protected final Map<Long, String> threadIdToModuleCache = new HashMap<>();
  protected final Map<Long, String> threadIdToPoolCache = new HashMap<>();

  AbstractCpuUsageMetricsManager(
      UnaryOperator<String> threadNameToModule, UnaryOperator<String> threadNameToPool) {
    this.threadNameToModule = threadNameToModule;
    this.threadNameToPool = threadNameToPool;
  }

  public static AbstractCpuUsageMetricsManager getCpuUsageMetricsManager(
      UnaryOperator<String> threadNameToModule, UnaryOperator<String> threadNameToPool) {
    String os = System.getProperty("os.name").toLowerCase();

    if (os.startsWith("windows")) {
      return new WindowsCpuUsageMetricsManager(threadNameToModule, threadNameToPool);
    } else if (os.startsWith("linux")) {
      return new LinuxCpuUsageMetricsManager(threadNameToModule, threadNameToPool);
    } else {
      return new MacCpuUsageMetricsManager(threadNameToModule, threadNameToPool);
    }
  }

  public Map<String, Double> getCpuUsageForPerModule() {
    return Collections.emptyMap();
  }

  public void setMetricService(AbstractMetricService metricService) {
    this.metricService = metricService;
  }
}
