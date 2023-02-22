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

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SystemMetrics implements IMetricSet {
  private static final Logger logger = LoggerFactory.getLogger(SystemMetrics.class);
  private final com.sun.management.OperatingSystemMXBean osMxBean;
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private final Set<FileStore> fileStores = new HashSet<>();
  private final boolean isDataNode;
  private long systemDiskTotalSpace = 0L;
  private long systemDiskFreeSpace = 0L;

  public SystemMetrics(boolean isDataNode) {
    this.isDataNode = isDataNode;
    this.osMxBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    collectSystemCpuInfo(metricService);
    collectSystemMemInfo(metricService);

    // register disk related metrics and start to collect the value of metrics in async way
    if (null == currentServiceFuture && isDataNode) {
      collectSystemDiskInfo(metricService);
      currentServiceFuture =
          ScheduledExecutorUtil.safelyScheduleAtFixedRate(
              service,
              this::collectDiskMetrics,
              1,
              MetricConfigDescriptor.getInstance()
                  .getMetricConfig()
                  .getAsyncCollectPeriodInSecond(),
              TimeUnit.SECONDS);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // first stop to update the value of some metrics in async way
    if (currentServiceFuture != null && isDataNode) {
      currentServiceFuture.cancel(true);
      currentServiceFuture = null;
    }

    removeSystemCpuInfo(metricService);
    if (isDataNode) {
      removeSystemDiskInfo(metricService);
    }
    removeSystemMemInfo(metricService);
  }

  private void collectSystemCpuInfo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.SYS_CPU_LOAD.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getSystemCpuLoad() * 100,
        Tag.NAME.toString(),
        "system");

    metricService
        .getOrCreateGauge(
            Metric.SYS_CPU_CORES.toString(), MetricLevel.CORE, Tag.NAME.toString(), "system")
        .set(osMxBean.getAvailableProcessors());
  }

  private void removeSystemCpuInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.SYS_CPU_LOAD.toString(), Tag.NAME.toString(), "system");

    metricService.remove(
        MetricType.GAUGE, Metric.SYS_CPU_CORES.toString(), Tag.NAME.toString(), "system");
  }

  private void collectSystemMemInfo(AbstractMetricService metricService) {
    metricService
        .getOrCreateGauge(
            Metric.SYS_TOTAL_PHYSICAL_MEMORY_SIZE.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "system")
        .set(osMxBean.getTotalPhysicalMemorySize());
    metricService.createAutoGauge(
        Metric.SYS_FREE_PHYSICAL_MEMORY_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getFreePhysicalMemorySize(),
        Tag.NAME.toString(),
        "system");
    metricService.createAutoGauge(
        Metric.SYS_TOTAL_SWAP_SPACE_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getTotalSwapSpaceSize(),
        Tag.NAME.toString(),
        "system");
    metricService.createAutoGauge(
        Metric.SYS_FREE_SWAP_SPACE_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getFreeSwapSpaceSize(),
        Tag.NAME.toString(),
        "system");
    metricService.createAutoGauge(
        Metric.SYS_COMMITTED_VM_SIZE.toString(),
        MetricLevel.CORE,
        osMxBean,
        a -> osMxBean.getCommittedVirtualMemorySize(),
        Tag.NAME.toString(),
        "system");
  }

  private void removeSystemMemInfo(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.GAUGE,
        Metric.SYS_TOTAL_PHYSICAL_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "system");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SYS_FREE_PHYSICAL_MEMORY_SIZE.toString(),
        Tag.NAME.toString(),
        "system");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SYS_TOTAL_SWAP_SPACE_SIZE.toString(),
        Tag.NAME.toString(),
        "system");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SYS_FREE_SWAP_SPACE_SIZE.toString(),
        Tag.NAME.toString(),
        "system");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SYS_COMMITTED_VM_SIZE.toString(),
        Tag.NAME.toString(),
        "system");
  }

  private void collectSystemDiskInfo(AbstractMetricService metricService) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    for (String dataDir : dataDirs) {
      Path path = Paths.get(dataDir);
      FileStore fileStore = null;
      try {
        fileStore = Files.getFileStore(path);
      } catch (IOException e) {
        // check parent if path is not exists
        path = path.getParent();
        try {
          fileStore = Files.getFileStore(path);
        } catch (IOException innerException) {
          logger.error("Failed to get storage path of {}, because", dataDir, innerException);
        }
      }
      if (null != fileStore) {
        fileStores.add(fileStore);
      }
    }

    metricService.createAutoGauge(
        Metric.SYS_DISK_TOTAL_SPACE.toString(),
        MetricLevel.CORE,
        this,
        SystemMetrics::getSystemDiskTotalSpace,
        Tag.NAME.toString(),
        "system");
    metricService.createAutoGauge(
        Metric.SYS_DISK_FREE_SPACE.toString(),
        MetricLevel.CORE,
        this,
        SystemMetrics::getSystemDiskFreeSpace,
        Tag.NAME.toString(),
        "system");
  }

  private void removeSystemDiskInfo(AbstractMetricService metricService) {
    fileStores.clear();
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SYS_DISK_TOTAL_SPACE.toString(),
        Tag.NAME.toString(),
        "system");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.SYS_DISK_FREE_SPACE.toString(),
        Tag.NAME.toString(),
        "system");
  }

  private void collectDiskMetrics() {
    long sysTotalSpace = 0L;
    long sysFreeSpace = 0L;

    for (FileStore fileStore : fileStores) {
      try {
        sysTotalSpace += fileStore.getTotalSpace();
        sysFreeSpace += fileStore.getUsableSpace();
      } catch (IOException e) {
        logger.error("Failed to statistic the size of {}, because", fileStore, e);
      }
    }
    systemDiskTotalSpace = sysTotalSpace;
    systemDiskFreeSpace = sysFreeSpace;
  }

  public long getSystemDiskTotalSpace() {
    return systemDiskTotalSpace;
  }

  public long getSystemDiskFreeSpace() {
    return systemDiskFreeSpace;
  }
}
