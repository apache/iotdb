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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.utils.MetricLevel;

import com.sun.management.OperatingSystemMXBean;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.stream.Stream;

/**
 * @author Erickin
 * @create 2022-03-31-下午 3:09
 */
public class SysRunMetricsMonitor {
  private MetricManager metricManager = MetricsService.getInstance().getMetricManager();
  private com.sun.management.OperatingSystemMXBean osMXBean;

  private SysRunMetricsMonitor() {
    osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  }

  public void collectSystemCpuInfo() {
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_CPU_LOAD.toString(),
        MetricLevel.IMPORTANT,
        osMXBean,
        a -> (long) (osMXBean.getSystemCpuLoad() * 100),
        Tag.NAME.toString(),
        "system");

    metricManager
        .getOrCreateGauge(
            Metric.SYS_CPU_CORES.toString(), MetricLevel.IMPORTANT, Tag.NAME.toString(), "system")
        .set(osMXBean.getAvailableProcessors());
  }

  public void collectSystemMEMInfo() {
    metricManager
        .getOrCreateGauge(
            Metric.SYS_TOTAL_PHYSICAL_MEMORY_SIZE.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "system")
        .set(osMXBean.getTotalPhysicalMemorySize());
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_FREE_PHYSICAL_MEMORY_SIZE.toString(),
        MetricLevel.IMPORTANT,
        osMXBean,
        a -> osMXBean.getFreePhysicalMemorySize(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_TOTAL_SWAP_SPACE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        osMXBean,
        a -> osMXBean.getTotalSwapSpaceSize(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_FREE_SWAP_SPACE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        osMXBean,
        a -> osMXBean.getFreeSwapSpaceSize(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_COMMITTED_VM_SIZE.toString(),
        MetricLevel.IMPORTANT,
        osMXBean,
        a -> osMXBean.getCommittedVirtualMemorySize(),
        Tag.NAME.toString(),
        "system");
  }

  public void collectSystemDiskInfo() {
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_DICK_TOTAL_SPACE.toString(),
        MetricLevel.IMPORTANT,
        this,
        a -> getSysDiskTotalSpace(),
        Tag.NAME.toString(),
        "system");
    metricManager.getOrCreateAutoGauge(
        Metric.SYS_DICK_FREE_SPACE.toString(),
        MetricLevel.IMPORTANT,
        this,
        a -> getSysDickFreeSpace(),
        Tag.NAME.toString(),
        "system");
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    metricManager.getOrCreateAutoGauge(
        Metric.TABLE_SPACE.toString(),
        MetricLevel.IMPORTANT,
        dataDirs,
        value ->
            Stream.of(value)
                .mapToLong(
                    dir -> {
                      dir += File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME;
                      dir += File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME;
                      return FileUtils.getDirSize(dir);
                    })
                .sum(),
        Tag.NAME.toString(),
        "tablespace");
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

  public static SysRunMetricsMonitor getInstance() {
    return SysRunMetricsMonitor.SysRunMetricsMonitorHolder.INSTANCE;
  }

  private static class SysRunMetricsMonitorHolder {
    private static final SysRunMetricsMonitor INSTANCE = new SysRunMetricsMonitor();
  }
}
