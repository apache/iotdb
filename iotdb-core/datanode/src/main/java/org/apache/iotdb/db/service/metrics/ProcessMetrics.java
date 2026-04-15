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
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.BaseTSD.SIZE_T;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProcessMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessMetrics.class);
  private static final MetricConfig CONFIG = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final OperatingSystemMXBean sunOsMxBean;
  private final Runtime runtime;
  private static final String PROCESS = "process";
  private static final String LINUX_RSS_PREFIX = "VmRSS:";
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
    long residentMemory = getResidentMemory();
    return residentMemory > 0 ? residentMemory : runtime.totalMemory() - runtime.freeMemory();
  }

  private long getResidentMemory() {
    if (CONFIG.getPid().isEmpty()) {
      return 0L;
    }
    try {
      switch (CONFIG.getSystemType()) {
        case LINUX:
          return getLinuxResidentMemory();
        case MAC:
          return getMacResidentMemory();
        case WINDOWS:
          return getWindowsResidentMemory();
        default:
          return 0L;
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to get process resident memory for pid {}", CONFIG.getPid(), e);
      return 0L;
    }
  }

  private long getLinuxResidentMemory() throws IOException {
    Path statusPath = Paths.get(String.format("/proc/%s/status", CONFIG.getPid()));
    if (!Files.exists(statusPath)) {
      return 0L;
    }
    for (String line : Files.readAllLines(statusPath)) {
      if (line.startsWith(LINUX_RSS_PREFIX)) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length >= 2) {
          return Long.parseLong(parts[1]) * 1024L;
        }
      }
    }
    return 0L;
  }

  private long getMacResidentMemory() throws IOException, InterruptedException {
    Process process = runtime.exec(new String[] {"ps", "-o", "rss=", "-p", CONFIG.getPid()});
    try (BufferedReader input =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line = input.readLine();
      int exitCode = process.waitFor();
      if (exitCode == 0 && line != null && !line.trim().isEmpty()) {
        return Long.parseLong(line.trim()) * 1024L;
      }
    }
    return 0L;
  }

  private long getWindowsResidentMemory() {
    WinNT.HANDLE hProcess = Kernel32.INSTANCE.GetCurrentProcess();
    ProcessMemoryCounters counters = new ProcessMemoryCounters();
    boolean success = PsapiExt.INSTANCE.GetProcessMemoryInfo(hProcess, counters, counters.size());
    if (!success) {
      return 0L;
    }
    return counters.workingSetSize.longValue();
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

  public interface PsapiExt extends Library {
    PsapiExt INSTANCE = Native.load("psapi", PsapiExt.class);

    boolean GetProcessMemoryInfo(WinNT.HANDLE process, ProcessMemoryCounters counters, int size);
  }

  @Structure.FieldOrder({
    "cb",
    "pageFaultCount",
    "peakWorkingSetSize",
    "workingSetSize",
    "quotaPeakPagedPoolUsage",
    "quotaPagedPoolUsage",
    "quotaPeakNonPagedPoolUsage",
    "quotaNonPagedPoolUsage",
    "pagefileUsage",
    "peakPagefileUsage"
  })
  public static class ProcessMemoryCounters extends Structure {
    public int cb = size();
    public int pageFaultCount;
    public SIZE_T peakWorkingSetSize;
    public SIZE_T workingSetSize;
    public SIZE_T quotaPeakPagedPoolUsage;
    public SIZE_T quotaPagedPoolUsage;
    public SIZE_T quotaPeakNonPagedPoolUsage;
    public SIZE_T quotaNonPagedPoolUsage;
    public SIZE_T pagefileUsage;
    public SIZE_T peakPagefileUsage;
  }
}
