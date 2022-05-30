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

package org.apache.iotdb.db.service.metrics.predefined;

import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.predefined.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.PredefinedMetric;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class ProcessMetrics implements IMetricSet {
  private OperatingSystemMXBean sunOsMXBean;
  private Runtime runtime;

  public ProcessMetrics() {
    sunOsMXBean =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    runtime = Runtime.getRuntime();
  }

  @Override
  public void bindTo(MetricManager metricManager) {
    collectProcessCPUInfo(metricManager);
    collectProcessMemInfo(metricManager);
    collectProcessStatusInfo(metricManager);
    collectThreadInfo(metricManager);
  }

  @Override
  public PredefinedMetric getType() {
    return PredefinedMetric.PROCESS;
  }

  private void collectProcessCPUInfo(MetricManager metricManager) {
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

  private void collectProcessMemInfo(MetricManager metricManager) {
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

  private void collectThreadInfo(MetricManager metricManager) {
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_THREADS_COUNT.toString(),
        MetricLevel.CORE,
        this,
        a -> getThreadsCount(),
        Tag.NAME.toString(),
        "process");
  }

  private void collectProcessStatusInfo(MetricManager metricManager) {
    metricManager.getOrCreateAutoGauge(
        Metric.PROCESS_STATUS.toString(),
        MetricLevel.CORE,
        this,
        a -> (getProcessStatus()),
        Tag.NAME.toString(),
        "process");
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
}
