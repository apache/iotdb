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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.predefined.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.PredefinedMetric;

import com.sun.management.OperatingSystemMXBean;

import java.io.File;
import java.lang.management.ManagementFactory;

public class SystemMetrics implements IMetricSet {
  private com.sun.management.OperatingSystemMXBean osMXBean;

  public SystemMetrics() {
    osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  @Override
  public void bindTo(MetricManager metricManager) {
    collectSystemCpuInfo(metricManager);
    collectSystemDiskInfo(metricManager);
    collectSystemMEMInfo(metricManager);
  }

  @Override
  public PredefinedMetric getType() {
    return PredefinedMetric.SYSTEM;
  }

  private void collectSystemCpuInfo(MetricManager metricManager) {
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_CPU_LOAD.toString(),
        MetricLevel.CORE,
        osMXBean,
        a -> (long) (osMXBean.getSystemCpuLoad() * 100),
        Tag.NAME.toString(),
        "system");

    metricManager
        .getOrCreateGauge(
            Metric.SYS_CPU_CORES.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), "system")
        .set(osMXBean.getAvailableProcessors());
  }

  private void collectSystemMEMInfo(MetricManager metricManager) {
    metricManager
        .getOrCreateGauge(
            Metric.SYS_TOTAL_PHYSICAL_MEMORY_SIZE.toString(),
            MetricLevel.CORE,
            Tag.NAME.toString(),
            "system")
        .set(osMXBean.getTotalPhysicalMemorySize());
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_FREE_PHYSICAL_MEMORY_SIZE.toString(),
        MetricLevel.CORE,
        osMXBean,
        a -> osMXBean.getFreePhysicalMemorySize(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_TOTAL_SWAP_SPACE_SIZE.toString(),
        MetricLevel.CORE,
        osMXBean,
        a -> osMXBean.getTotalSwapSpaceSize(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_FREE_SWAP_SPACE_SIZE.toString(),
        MetricLevel.CORE,
        osMXBean,
        a -> osMXBean.getFreeSwapSpaceSize(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_COMMITTED_VM_SIZE.toString(),
        MetricLevel.CORE,
        osMXBean,
        a -> osMXBean.getCommittedVirtualMemorySize(),
        Tag.NAME.toString(),
        "system");
  }

  private void collectSystemDiskInfo(MetricManager metricManager) {
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_DISK_TOTAL_SPACE.toString(),
        MetricLevel.CORE,
        this,
        a -> getSysDiskTotalSpace(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_DISK_FREE_SPACE.toString(),
        MetricLevel.CORE,
        this,
        a -> getSysDickFreeSpace(),
        Tag.NAME.toString(),
        "system");
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
  }

  private long getSysDiskTotalSpace() {
    File[] files = File.listRoots();
    long sysTotalSpace = 0L;
    for (File file : files) {
      sysTotalSpace += file.getTotalSpace();
    }
    return sysTotalSpace;
  }

  private long getSysDickFreeSpace() {
    File[] files = File.listRoots();
    long sysFreeSpace = 0L;
    for (File file : files) {
      sysFreeSpace += file.getFreeSpace();
    }
    return sysFreeSpace;
  }
}
