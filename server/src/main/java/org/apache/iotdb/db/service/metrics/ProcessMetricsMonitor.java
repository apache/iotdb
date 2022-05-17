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

public class ProcessMetricsMonitor {

  private MetricManager metricManager = MetricsService.getInstance().getMetricManager();
  private OperatingSystemMXBean sunOsMXBean;
  private Runtime runtime;

  public void collectProcessCPUInfo() {
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_CPU_LOAD.toString(),
        MetricLevel.CORE,
        sunOsMXBean,
        a -> (long) (sunOsMXBean.getProcessCpuLoad() * 100),
        Tag.NAME.toString(),
        "process");

    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_CPU_TIME.toString(),
        MetricLevel.CORE,
        sunOsMXBean,
        com.sun.management.OperatingSystemMXBean::getProcessCpuTime,
        Tag.NAME.toString(),
        "process");
  }

  public void collectProcessMemInfo() {
    Runtime runtime = Runtime.getRuntime();
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_MAX_MEM.toString(),
        MetricLevel.CORE,
        runtime,
        a -> runtime.maxMemory(),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_TOTAL_MEM.toString(),
        MetricLevel.CORE,
        runtime,
        a -> runtime.totalMemory(),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_FREE_MEM.toString(),
        MetricLevel.CORE,
        runtime,
        a -> runtime.freeMemory(),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_USED_MEM.toString(),
        MetricLevel.CORE,
        this,
        a -> getProcessUsedMemory(),
        Tag.NAME.toString(),
        "process");
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_MEM_RATIO.toString(),
        MetricLevel.CORE,
        this,
        a -> Math.round(getProcessMemoryRatio()),
        Tag.NAME.toString(),
        "process");
  }

  public void collectThreadInfo() {
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_THREADS_COUNT.toString(),
        MetricLevel.CORE,
        this,
        a -> getThreadsCount(),
        Tag.NAME.toString(),
        "process");
  }

  public void collectProcessStatusInfo() {
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_STATUS.toString(),
        MetricLevel.CORE,
        this,
        a -> (getProcessStatus()),
        Tag.NAME.toString(),
        "process");
  }

  private ProcessMetricsMonitor() {
    sunOsMXBean =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    runtime = Runtime.getRuntime();
  }

  private long getProcessUsedMemory() {
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private long getProcessStatus() {
    return Thread.currentThread().isAlive() ? 1 : 0;
  }

  private int getThreadsCount() {
    ThreadGroup parentThread;
    for (parentThread = Thread.currentThread().getThreadGroup();
        parentThread.getParent() != null;
        parentThread = parentThread.getParent()) {}

    return parentThread.activeCount();
  }

  private double getProcessMemoryRatio() {
    long processUsedMemory = getProcessUsedMemory();
    long totalPhysicalMemorySize = sunOsMXBean.getTotalPhysicalMemorySize();
    return (double) processUsedMemory / (double) totalPhysicalMemorySize * 100;
  }

  public static ProcessMetricsMonitor getInstance() {
    return ThreadMetricsMonitorHolder.INSTANCE;
  }

  private static class ThreadMetricsMonitorHolder {
    private static final ProcessMetricsMonitor INSTANCE = new ProcessMetricsMonitor();
  }
}
