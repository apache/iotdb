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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.utils.MetricLevel;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * @author Erickin
 * @create 2022-03-31-上午 8:55
 */
public class ProcessMetricsMonitor {

  private MetricManager metricManager = MetricsService.getInstance().getMetricManager();
  private static OperatingSystemMXBean sunOsMXBean;

  public void collectProcessCPUInfo() {
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_CPU_LOAD.toString(),
        MetricLevel.IMPORTANT,
        sunOsMXBean,
        a -> (long) (sunOsMXBean.getProcessCpuLoad() * 100),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_CPU_TIME.toString(),
        MetricLevel.IMPORTANT,
        sunOsMXBean,
        com.sun.management.OperatingSystemMXBean::getProcessCpuTime,
        Tag.NAME.toString(),
        "process");
  }

  public void collectProcessMemInfo() {
    Runtime runtime = Runtime.getRuntime();
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_MAX_MEM.toString(),
        MetricLevel.IMPORTANT,
        runtime,
        a -> runtime.maxMemory(),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_TOTAL_MEM.toString(),
        MetricLevel.IMPORTANT,
        runtime,
        a -> runtime.totalMemory(),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_FREE_MEM.toString(),
        MetricLevel.IMPORTANT,
        runtime,
        a -> runtime.freeMemory(),
        Tag.NAME.toString(),
        "process");
  }

  private ProcessMetricsMonitor() {
    sunOsMXBean =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  public static ProcessMetricsMonitor getInstance() {
    return ThreadMetricsMonitorHolder.INSTANCE;
  }

  private static class ThreadMetricsMonitorHolder {
    private static final ProcessMetricsMonitor INSTANCE = new ProcessMetricsMonitor();
  }
}
