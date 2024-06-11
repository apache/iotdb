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

package org.apache.iotdb.metrics.metricsets.system;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.FileStoreUtils;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;
import org.apache.iotdb.metrics.utils.SystemType;

import com.sun.management.OperatingSystemMXBean;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SystemMetrics implements IMetricSet {
  private static final Logger logger = LoggerFactory.getLogger(SystemMetrics.class);
  private static final MetricConfig CONFIG = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final Runtime runtime = Runtime.getRuntime();
  private final String[] getSystemMemoryCommand = new String[] {"/bin/sh", "-c", "free"};
  private final String[] linuxMemoryTitles =
      new String[] {"Total", "Used", "Free", "Shared", "Buff/Cache", "Available"};
  private long lastUpdateTime = 0L;
  private volatile long usedMemory = 0L;
  private volatile long sharedMemory = 0L;
  private volatile long buffCacheMemory = 0L;
  private volatile long availableMemory = 0L;

  static final String SYSTEM = "system";
  private final com.sun.management.OperatingSystemMXBean osMxBean;
  private Set<FileStore> fileStores = new HashSet<>();
  private static final String FAILED_TO_STATISTIC = "Failed to statistic the size of {}, because";

  public SystemMetrics() {
    this.osMxBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  public void setDiskDirs(List<String> diskDirs) {
    if (!MetricConfigDescriptor.getInstance()
        .getMetricConfig()
        .getMetricLevel()
        .equals(MetricLevel.OFF)) {
      this.fileStores = getFileStores(diskDirs);
    }
  }

  public static Set<FileStore> getFileStores(List<String> dirs) {
    Set<FileStore> fileStoreSet = new HashSet<>();
    for (String diskDir : dirs) {
      if (!FSUtils.isLocal(diskDir)) {
        continue;
      }
      FileStore fileStore = FileStoreUtils.getFileStore(diskDir);
      if (fileStore != null) {
        fileStoreSet.add(fileStore);
      }
    }
    return fileStoreSet;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    collectSystemCpuInfo(metricService);
    collectSystemMemInfo(metricService);
    collectSystemDiskInfo(metricService);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    removeSystemCpuInfo(metricService);
    removeSystemDiskInfo(metricService);
    removeSystemMemInfo(metricService);
  }

  private void collectSystemCpuInfo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        SystemMetric.SYS_CPU_LOAD.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getSystemCpuLoad() * 100,
        SystemTag.NAME.toString(),
        SYSTEM);

    metricService
        .getOrCreateGauge(
            SystemMetric.SYS_CPU_CORES.toString(),
            MetricLevel.CORE,
            SystemTag.NAME.toString(),
            SYSTEM)
        .set(osMxBean.getAvailableProcessors());
  }

  private void removeSystemCpuInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_CPU_LOAD.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);

    metricService.remove(
        MetricType.GAUGE, SystemMetric.SYS_CPU_CORES.toString(), SystemTag.NAME.toString(), SYSTEM);
  }

  private void collectSystemMemInfo(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            SystemMetric.SYS_TOTAL_PHYSICAL_MEMORY_SIZE.toString(),
            MetricLevel.CORE,
            SystemTag.NAME.toString(),
            SYSTEM)
        .set(osMxBean.getTotalPhysicalMemorySize());
    metricService.createAutoGauge(
        SystemMetric.SYS_FREE_PHYSICAL_MEMORY_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getFreePhysicalMemorySize(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.createAutoGauge(
        SystemMetric.SYS_TOTAL_SWAP_SPACE_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getTotalSwapSpaceSize(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.createAutoGauge(
        SystemMetric.SYS_FREE_SWAP_SPACE_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getFreeSwapSpaceSize(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.createAutoGauge(
        SystemMetric.SYS_COMMITTED_VM_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getCommittedVirtualMemorySize(),
        SystemTag.NAME.toString(),
        SYSTEM);
    if (CONFIG.getSystemType() == SystemType.LINUX) {
      metricService.createAutoGauge(
          SystemMetric.LINUX_MEMORY_SIZE.toString(),
          MetricLevel.CORE,
          this,
          a -> {
            updateLinuxSystemMemInfo();
            return usedMemory;
          },
          SystemTag.NAME.toString(),
          linuxMemoryTitles[1]);
      metricService.createAutoGauge(
          SystemMetric.LINUX_MEMORY_SIZE.toString(),
          MetricLevel.CORE,
          this,
          a -> {
            updateLinuxSystemMemInfo();
            return sharedMemory;
          },
          SystemTag.NAME.toString(),
          linuxMemoryTitles[3]);
      metricService.createAutoGauge(
          SystemMetric.LINUX_MEMORY_SIZE.toString(),
          MetricLevel.CORE,
          this,
          a -> {
            updateLinuxSystemMemInfo();
            return buffCacheMemory;
          },
          SystemTag.NAME.toString(),
          linuxMemoryTitles[4]);
      metricService.createAutoGauge(
          SystemMetric.LINUX_MEMORY_SIZE.toString(),
          MetricLevel.CORE,
          this,
          a -> {
            updateLinuxSystemMemInfo();
            return availableMemory;
          },
          SystemTag.NAME.toString(),
          linuxMemoryTitles[5]);
    }
  }

  private void updateLinuxSystemMemInfo() {
    long time = System.currentTimeMillis();
    if (time - lastUpdateTime > MetricConstant.UPDATE_INTERVAL) {
      lastUpdateTime = time;
      try {
        Process process = runtime.exec(getSystemMemoryCommand);
        StringBuilder result = new StringBuilder();
        try (BufferedReader input =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = input.readLine()) != null) {
            result.append(line).append("\n");
          }
        }
        String[] lines = result.toString().trim().split("\n");
        // if failed to get result
        if (lines.length >= 2) {
          String[] memParts = lines[1].trim().split("\\s+");
          if (memParts.length >= linuxMemoryTitles.length) {
            usedMemory = Long.parseLong(memParts[2]) * 1024;
            sharedMemory = Long.parseLong(memParts[4]) * 1024;
            buffCacheMemory = Long.parseLong(memParts[5]) * 1024;
            availableMemory = Long.parseLong(memParts[6]) * 1024;
          }
        }
      } catch (IOException e) {
        logger.debug("Failed to get memory, because ", e);
      }
    }
  }

  private void removeSystemMemInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        SystemMetric.SYS_TOTAL_PHYSICAL_MEMORY_SIZE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_FREE_PHYSICAL_MEMORY_SIZE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_TOTAL_SWAP_SPACE_SIZE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_FREE_SWAP_SPACE_SIZE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_COMMITTED_VM_SIZE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    if (CONFIG.getSystemType() == SystemType.LINUX) {
      for (String title : linuxMemoryTitles) {
        metricService.remove(
            MetricType.GAUGE,
            SystemMetric.LINUX_MEMORY_SIZE.toString(),
            SystemTag.NAME.toString(),
            title);
      }
    }
  }

  private void collectSystemDiskInfo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        SystemMetric.SYS_DISK_TOTAL_SPACE.toString(),
        MetricLevel.CORE,
        this,
        SystemMetrics::getSystemDiskTotalSpace,
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.createAutoGauge(
        SystemMetric.SYS_DISK_FREE_SPACE.toString(),
        MetricLevel.CORE,
        this,
        SystemMetrics::getSystemDiskFreeSpace,
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.createAutoGauge(
        SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
        MetricLevel.CORE,
        this,
        SystemMetrics::getSystemDiskAvailableSpace,
        SystemTag.NAME.toString(),
        SYSTEM);
  }

  private void removeSystemDiskInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_DISK_TOTAL_SPACE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_DISK_FREE_SPACE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.SYS_DISK_AVAILABLE_SPACE.toString(),
        SystemTag.NAME.toString(),
        SYSTEM);
    fileStores.clear();
  }

  public long getSystemDiskTotalSpace() {
    long sysTotalSpace = 0L;
    for (FileStore fileStore : fileStores) {
      try {
        sysTotalSpace += fileStore.getTotalSpace();
      } catch (IOException e) {
        logger.error(FAILED_TO_STATISTIC, fileStore, e);
      }
    }
    return sysTotalSpace;
  }

  public long getSystemDiskFreeSpace() {
    long sysFreeSpace = 0L;
    for (FileStore fileStore : fileStores) {
      try {
        sysFreeSpace += fileStore.getUnallocatedSpace();
      } catch (IOException e) {
        logger.error(FAILED_TO_STATISTIC, fileStore, e);
      }
    }
    return sysFreeSpace;
  }

  public long getSystemDiskAvailableSpace() {
    long sysAvailableSpace = 0L;
    for (FileStore fileStore : fileStores) {
      try {
        sysAvailableSpace += fileStore.getUsableSpace();
      } catch (IOException e) {
        logger.error(FAILED_TO_STATISTIC, fileStore, e);
      }
    }
    return sysAvailableSpace;
  }

  public static SystemMetrics getInstance() {
    return SystemMetricsHolder.INSTANCE;
  }

  private static class SystemMetricsHolder {
    private static final SystemMetrics INSTANCE = new SystemMetrics();

    private SystemMetricsHolder() {}
  }
}
