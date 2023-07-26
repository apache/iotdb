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
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;
import org.apache.iotdb.tsfile.utils.FSUtils;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class SystemMetrics implements IMetricSet {
  private static final Logger logger = LoggerFactory.getLogger(SystemMetrics.class);
  private static final String SYSTEM = "system";
  private final com.sun.management.OperatingSystemMXBean osMxBean;
  private final Set<FileStore> fileStores = new HashSet<>();
  private final ArrayList<String> diskDirs;

  public SystemMetrics(ArrayList<String> diskDirs) {
    this.diskDirs = diskDirs;
    this.osMxBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
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
  }

  private void collectSystemDiskInfo(AbstractMetricService metricService) {
    for (String diskDir : diskDirs) {
      if (!FSUtils.isLocal(diskDir)) {
        continue;
      }
      Path path = Paths.get(diskDir);
      FileStore fileStore = null;
      try {
        fileStore = Files.getFileStore(path);
      } catch (IOException e) {
        // check parent if path is not exists
        path = path.getParent();
        try {
          fileStore = Files.getFileStore(path);
        } catch (IOException innerException) {
          logger.error("Failed to get storage path of {}, because", diskDir, innerException);
        }
      }
      if (null != fileStore) {
        fileStores.add(fileStore);
      }
    }

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

    diskDirs.clear();
    fileStores.clear();
  }

  public long getSystemDiskTotalSpace() {
    long sysTotalSpace = 0L;
    for (FileStore fileStore : fileStores) {
      try {
        sysTotalSpace += fileStore.getTotalSpace();
      } catch (IOException e) {
        logger.error("Failed to statistic the size of {}, because", fileStore, e);
      }
    }
    return sysTotalSpace;
  }

  public long getSystemDiskFreeSpace() {
    long sysFreeSpace = 0L;
    for (FileStore fileStore : fileStores) {
      try {
        sysFreeSpace += fileStore.getUsableSpace();
      } catch (IOException e) {
        logger.error("Failed to statistic the size of {}, because", fileStore, e);
      }
    }
    return sysFreeSpace;
  }
}
