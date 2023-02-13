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

package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.service.metrics.io.AbstractDiskMetricsManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.Set;

public class DiskMetrics implements IMetricSet {
  private final AbstractDiskMetricsManager diskMetricsManager =
      AbstractDiskMetricsManager.getDiskMetricsManager();
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // metrics for disks
    Set<String> diskIDs = diskMetricsManager.getDiskIDs();
    for (String diskID : diskIDs) {
      metricService.createAutoGauge(
          Metric.DISK_IO_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadDataSizeForDisk().getOrDefault(diskID, 0L),
          Tag.TYPE.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_SIZE.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteDataSizeForDisk().getOrDefault(diskID, 0L),
          Tag.TYPE.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadOperationCountForDisk().getOrDefault(diskID, 0),
          Tag.TYPE.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteOperationCountForDisk().getOrDefault(diskID, 0),
          Tag.TYPE.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getMergedReadOperationForDisk().getOrDefault(diskID, 0L),
          Tag.TYPE.toString(),
          "merged_write",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_OPS.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getMergedWriteOperationForDisk().getOrDefault(diskID, 0L),
          Tag.TYPE.toString(),
          "merged_read",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadCostTimeForDisk().getOrDefault(diskID, 0L),
          Tag.TYPE.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteCostTimeForDisk().getOrDefault(diskID, 0L),
          Tag.TYPE.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_AVG_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgReadCostTimeOfEachOpsForDisk().getOrDefault(diskID, 0.0).longValue(),
          Tag.TYPE.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_AVG_TIME.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgWriteCostTimeOfEachOpsForDisk().getOrDefault(diskID, 0.0).longValue(),
          Tag.TYPE.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_SECTOR_NUM.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgSectorCountOfEachReadForDisk().getOrDefault(diskID, 0.0).longValue(),
          Tag.TYPE.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.createAutoGauge(
          Metric.DISK_IO_SECTOR_NUM.toString(),
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgSectorCountOfEachWriteForDisk().getOrDefault(diskID, 0.0).longValue(),
          Tag.TYPE.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
    }

    // metrics for datanode and config node
    metricService.createAutoGauge(
        Metric.PROCESS_IO_OPS.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        AbstractDiskMetricsManager::getReadOpsCountForProcess,
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "read");
    metricService.createAutoGauge(
        Metric.PROCESS_IO_OPS.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        AbstractDiskMetricsManager::getWriteOpsCountForProcess,
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "write");
    metricService.createAutoGauge(
        Metric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        AbstractDiskMetricsManager::getActualReadDataSizeForProcess,
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "actual_read");
    metricService.createAutoGauge(
        Metric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        AbstractDiskMetricsManager::getActualWriteDataSizeForProcess,
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "actual_write");
    metricService.createAutoGauge(
        Metric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        AbstractDiskMetricsManager::getAttemptReadSizeForProcess,
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "attempt_read");
    metricService.createAutoGauge(
        Metric.PROCESS_IO_SIZE.toString(),
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        AbstractDiskMetricsManager::getAttemptWriteSizeForProcess,
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "attempt_write");
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // metrics for disks
    Set<String> diskIDs = diskMetricsManager.getDiskIDs();
    for (String diskID : diskIDs) {
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_SIZE.toString(),
          Tag.NAME.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_SIZE.toString(),
          Tag.NAME.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_OPS.toString(),
          Tag.NAME.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_OPS.toString(),
          Tag.NAME.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_TIME.toString(),
          Tag.NAME.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_TIME.toString(),
          Tag.NAME.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_TIME.toString(),
          Tag.NAME.toString(),
          "avg_read",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_TIME.toString(),
          Tag.NAME.toString(),
          "avg_write",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_SECTOR_NUM.toString(),
          Tag.NAME.toString(),
          "read",
          Tag.NAME.toString(),
          diskID);
      metricService.remove(
          MetricType.AUTO_GAUGE,
          Metric.DISK_IO_SECTOR_NUM.toString(),
          Tag.NAME.toString(),
          "write",
          Tag.NAME.toString(),
          diskID);
    }

    // metrics for datanode and config node
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PROCESS_IO_SIZE.toString(),
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "actual_read");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PROCESS_IO_SIZE.toString(),
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "actual_write");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PROCESS_IO_SIZE.toString(),
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "attempt_read");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PROCESS_IO_SIZE.toString(),
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "attempt_write");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PROCESS_IO_OPS.toString(),
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "read");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PROCESS_IO_OPS.toString(),
        Tag.FROM.toString(),
        diskMetricsManager.getProcessName(),
        Tag.NAME.toString(),
        "write");
  }
}
