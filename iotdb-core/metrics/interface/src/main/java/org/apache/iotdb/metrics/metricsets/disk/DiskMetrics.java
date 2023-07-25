/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.disk;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;

import java.util.Set;

public class DiskMetrics implements IMetricSet {
  private final IDiskMetricsManager diskMetricsManager =
      IDiskMetricsManager.getDiskMetricsManager();

  private final String processName;
  private static final String WRITE = "write";
  private static final String READ = "read";
  private static final String AVG_READ = "avg_read";
  private static final String AVG_WRITE = "avg_write";
  private static final String MERGED_READ = "merged_read";
  private static final String MERGED_WRITE = "merged_write";
  private static final String ATTEMPT_READ = "attempt_read";
  private static final String ATTEMPT_WRITE = "attempt_write";
  private static final String ACTUAL_WRITE = "actual_write";
  private static final String ACTUAL_READ = "actual_read";

  public DiskMetrics(String processName) {
    this.processName = processName;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // metrics for disks
    Set<String> diskIDs = diskMetricsManager.getDiskIds();
    for (String diskID : diskIDs) {
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadDataSizeForDisk().getOrDefault(diskID, 0.0),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteDataSizeForDisk().getOrDefault(diskID, 0.0),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadOperationCountForDisk().getOrDefault(diskID, 0L),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteOperationCountForDisk().getOrDefault(diskID, 0L),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getMergedReadOperationForDisk().getOrDefault(diskID, 0L),
          SystemTag.TYPE.toString(),
          MERGED_READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getMergedWriteOperationForDisk().getOrDefault(diskID, 0L),
          SystemTag.TYPE.toString(),
          MERGED_WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadCostTimeForDisk().getOrDefault(diskID, 0L),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteCostTimeForDisk().getOrDefault(diskID, 0L),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_AVG_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgReadCostTimeOfEachOpsForDisk().getOrDefault(diskID, 0.0),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_AVG_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgWriteCostTimeOfEachOpsForDisk().getOrDefault(diskID, 0.0),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_AVG_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgSizeOfEachReadForDisk().getOrDefault(diskID, 0.0),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_AVG_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgSizeOfEachWriteForDisk().getOrDefault(diskID, 0.0),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_BUSY_PERCENTAGE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getIoUtilsPercentage().getOrDefault(diskID, 0.0),
          SystemTag.DISK.toString(),
          diskID);
      metricService.createAutoGauge(
          SystemMetric.DISK_IO_QUEUE_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getQueueSizeForDisk().getOrDefault(diskID, 0.0),
          SystemTag.DISK.toString(),
          diskID);
    }

    // metrics for datanode and config node
    metricService.createAutoGauge(
        SystemMetric.PROCESS_IO_OPS.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getReadOpsCountForProcess,
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        READ);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_IO_OPS.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getWriteOpsCountForProcess,
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        WRITE);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getActualReadDataSizeForProcess,
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ACTUAL_READ);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getActualWriteDataSizeForProcess,
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ACTUAL_WRITE);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getAttemptReadSizeForProcess,
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ATTEMPT_READ);
    metricService.createAutoGauge(
        SystemMetric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getAttemptWriteSizeForProcess,
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ATTEMPT_WRITE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // metrics for disks
    Set<String> diskIDs = diskMetricsManager.getDiskIds();
    for (String diskID : diskIDs) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_SIZE.toString(),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_SIZE.toString(),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_OPS.toString(),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_OPS.toString(),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_OPS.toString(),
          SystemTag.TYPE.toString(),
          AVG_READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_OPS.toString(),
          SystemTag.TYPE.toString(),
          AVG_WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_TIME.toString(),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_TIME.toString(),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_AVG_SIZE.toString(),
          SystemTag.TYPE.toString(),
          READ,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_AVG_SIZE.toString(),
          SystemTag.TYPE.toString(),
          WRITE,
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_BUSY_PERCENTAGE.toString(),
          SystemTag.DISK.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          SystemMetric.DISK_IO_QUEUE_SIZE.toString(),
          SystemTag.DISK.toString(),
          diskID);
    }

    // metrics for datanode and config node
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_IO_SIZE.toString(),
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ACTUAL_READ);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_IO_SIZE.toString(),
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ACTUAL_WRITE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_IO_SIZE.toString(),
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ATTEMPT_READ);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_IO_SIZE.toString(),
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        ATTEMPT_WRITE);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_IO_OPS.toString(),
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        READ);
    metricService.remove(
        MetricType.AUTO_GAUGE,
        SystemMetric.PROCESS_IO_OPS.toString(),
        SystemTag.FROM.toString(),
        processName,
        SystemTag.NAME.toString(),
        WRITE);
  }
}
