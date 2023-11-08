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

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class ProcessMetrics implements IMetricSet {
  private final OperatingSystemMXBean sunOsMxBean;
  private final Runtime runtime;
  private static final String PROCESS = "process";
  private long lastUpdateTime = 0L;
  private volatile long processCpuLoad = 0L;
  private volatile long processCpuTime = 0L;

  public ProcessMetrics() {
    sunOsMxBean =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    runtime = Runtime.getRuntime();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    collectProcessCpuInfo(metricService);
    collectProcessMemInfo(metricService);
    collectProcessStatusInfo(metricService);
    collectThreadInfo(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    removeProcessCpuInfo(metricService);
    removeProcessMemInfo(metricService);
    removeProcessStatusInfo(metricService);
    removeThreadInfo(metricService);
  }

  private void collectProcessCpuInfo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        SystemMetric.PROCESS_CPU_LOAD.toString(),
        MetricLevel.CORE,
        sunOsMxBean,
        a -> {
          if (System.currentTimeMillis() - lastUpdateTime > MetricConstant.UPDATE_INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            processCpuLoad = (long) (sunOsMxBean.getProcessCpuLoad() * 100);
            processCpuTime = sunOsMxBean.getProcessCpuTime();
          }
          return processCpuLoad;
        },
        Tag.NAME.toString(),
        PROCESS);

    metricService.createAutoGauge(
        SystemMetric.PROCESS_CPU_TIME.toString(),
        MetricLevel.CORE,
        sunOsMxBean,
        bean -> {
          if (System.currentTimeMillis() - lastUpdateTime > MetricConstant.UPDATE_INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            processCpuLoad = (long) (sunOsMxBean.getProcessCpuLoad() * 100);
            processCpuTime = sunOsMxBean.getProcessCpuTime();
          }
          return processCpuTime;
        },
        Tag.NAME.toString(),
        PROCESS);
  }

  private void removeProcessCpuInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_CPU_LOAD.toString(),
        Tag.NAME.toString(),
        PROCESS);

    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_CPU_TIME.toString(),
        Tag.NAME.toString(),
        PROCESS);
  }

  private void collectProcessMemInfo(AbstractMetricService metricService) {
    Runtime runtime = Runtime.getRuntime();
    metricService.createAutoGauge(
        SystemMetric.PROCESS_MAX_MEM.toString(),
        MetricLevel.CORE,
        runtime,
        a -> runtime.maxMemory(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_TOTAL_MEM.toString(),
        MetricLevel.CORE,
        runtime,
        a -> runtime.totalMemory(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_FREE_MEM.toString(),
        MetricLevel.CORE,
        runtime,
        a -> runtime.freeMemory(),
        Tag.NAME.toString(),
        PROCESS);
    // TODO maybe following metrics can be removed
    metricService.createAutoGauge(
        SystemMetric.PROCESS_USED_MEM.toString(),
        MetricLevel.IMPORTANT,
        this,
        a -> getProcessUsedMemory(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_MEM_RATIO.toString(),
        MetricLevel.IMPORTANT,
        this,
        a -> Math.round(getProcessMemoryRatio()),
        Tag.NAME.toString(),
        PROCESS);
  }

  private void removeProcessMemInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_MAX_MEM.toString(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_TOTAL_MEM.toString(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_FREE_MEM.toString(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_USED_MEM.toString(),
        Tag.NAME.toString(),
        PROCESS);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_MEM_RATIO.toString(),
        Tag.NAME.toString(),
        PROCESS);
  }

  private void collectThreadInfo(AbstractMetricService metricService) {
    // TODO maybe duplicated with thread info in jvm related metrics
    metricService.createAutoGauge(
        SystemMetric.PROCESS_THREADS_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        a -> getThreadsCount(),
        Tag.NAME.toString(),
        PROCESS);
  }

  private void removeThreadInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_THREADS_COUNT.toString(),
        Tag.NAME.toString(),
        PROCESS);
  }

  private void collectProcessStatusInfo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        SystemMetric.PROCESS_STATUS.toString(),
        MetricLevel.IMPORTANT,
        this,
        a -> (getProcessStatus()),
        Tag.NAME.toString(),
        PROCESS);
  }

  private void removeProcessStatusInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_STATUS.toString(),
        Tag.NAME.toString(),
        PROCESS);
  }

  private long getProcessUsedMemory() {
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private long getProcessStatus() {
    return Thread.currentThread().isAlive() ? 1 : 0;
  }

  private int getThreadsCount() {
    ThreadGroup parentThread = Thread.currentThread().getThreadGroup();
    while (parentThread.getParent() != null) {
      parentThread = parentThread.getParent();
    }
    return parentThread.activeCount();
  }

  private double getProcessMemoryRatio() {
    long processUsedMemory = getProcessUsedMemory();
    long totalPhysicalMemorySize = sunOsMxBean.getTotalPhysicalMemorySize();
    return (double) processUsedMemory / (double) totalPhysicalMemorySize * 100;
  }
}
