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
  private static final String TYPE = "type";
  private static final String NAME = "name";
  private static final String FROM = "from";
  private static final String DISK_IO_SIZE = "disk_io_size";
  private static final String DISK_IO_OPS = "disk_io_ops";
  private static final String DISK_IO_TIME = "disk_io_time";
  private static final String DISK_IO_AVG_TIME = "disk_io_avg_time";
  private static final String DISK_IO_AVG_SIZE = "disk_io_avg_size";
  private static final String DISK_IO_BUSY_PERCENTAGE = "disk_io_busy_percentage";
  private static final String DISK_IO_QUEUE_SIZE = "disk_io_avg_queue_size";

  private static final String PROCESS_IO_OPS = "process_io_ops";
  private static final String PROCESS_IO_SIZE = "process_io_size";

  public DiskMetrics(String processName) {
    this.processName = processName;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // metrics for disks
    Set<String> diskIDs = diskMetricsManager.getDiskIds();
    for (String diskID : diskIDs) {
      metricService.createAutoGauge(
          DISK_IO_SIZE,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadDataSizeForDisk().getOrDefault(diskID, 0.0),
          TYPE,
          READ,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_SIZE,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteDataSizeForDisk().getOrDefault(diskID, 0.0),
          TYPE,
          WRITE,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_OPS,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadOperationCountForDisk().getOrDefault(diskID, 0L),
          TYPE,
          READ,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_OPS,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteOperationCountForDisk().getOrDefault(diskID, 0L),
          TYPE,
          WRITE,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_OPS,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getMergedReadOperationForDisk().getOrDefault(diskID, 0L),
          TYPE,
          MERGED_READ,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_OPS,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getMergedWriteOperationForDisk().getOrDefault(diskID, 0L),
          TYPE,
          MERGED_WRITE,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_TIME,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getReadCostTimeForDisk().getOrDefault(diskID, 0L),
          TYPE,
          READ,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_TIME,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getWriteCostTimeForDisk().getOrDefault(diskID, 0L),
          TYPE,
          WRITE,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_AVG_TIME,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgReadCostTimeOfEachOpsForDisk().getOrDefault(diskID, 0.0),
          TYPE,
          READ,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_AVG_TIME,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgWriteCostTimeOfEachOpsForDisk().getOrDefault(diskID, 0.0),
          TYPE,
          WRITE,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_AVG_SIZE,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgSizeOfEachReadForDisk().getOrDefault(diskID, 0.0),
          TYPE,
          READ,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_AVG_SIZE,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getAvgSizeOfEachWriteForDisk().getOrDefault(diskID, 0.0),
          TYPE,
          WRITE,
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_BUSY_PERCENTAGE,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getIoUtilsPercentage().getOrDefault(diskID, 0.0),
          NAME,
          diskID);
      metricService.createAutoGauge(
          DISK_IO_QUEUE_SIZE,
          MetricLevel.IMPORTANT,
          diskMetricsManager,
          x -> x.getQueueSizeForDisk().getOrDefault(diskID, 0.0),
          NAME,
          diskID);
    }

    // metrics for datanode and config node
    metricService.createAutoGauge(
        PROCESS_IO_OPS,
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getReadOpsCountForProcess,
        FROM,
        processName,
        NAME,
        READ);
    metricService.createAutoGauge(
        PROCESS_IO_OPS,
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getWriteOpsCountForProcess,
        FROM,
        processName,
        NAME,
        WRITE);
    metricService.createAutoGauge(
        PROCESS_IO_SIZE,
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getActualReadDataSizeForProcess,
        FROM,
        processName,
        NAME,
        ACTUAL_READ);
    metricService.createAutoGauge(
        PROCESS_IO_SIZE,
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getActualWriteDataSizeForProcess,
        FROM,
        processName,
        NAME,
        ACTUAL_WRITE);
    metricService.createAutoGauge(
        PROCESS_IO_SIZE,
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getAttemptReadSizeForProcess,
        FROM,
        processName,
        NAME,
        ATTEMPT_READ);
    metricService.createAutoGauge(
        PROCESS_IO_SIZE,
        MetricLevel.IMPORTANT,
        diskMetricsManager,
        IDiskMetricsManager::getAttemptWriteSizeForProcess,
        FROM,
        processName,
        NAME,
        ATTEMPT_WRITE);
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // metrics for disks
    Set<String> diskIDs = diskMetricsManager.getDiskIds();
    for (String diskID : diskIDs) {
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_SIZE, TYPE, READ, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_SIZE, TYPE, WRITE, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_OPS, TYPE, READ, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_OPS, TYPE, WRITE, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_TIME, TYPE, READ, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_TIME, TYPE, WRITE, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_TIME, TYPE, AVG_READ, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_TIME, TYPE, AVG_WRITE, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_AVG_SIZE, TYPE, READ, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_AVG_SIZE, TYPE, WRITE, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_BUSY_PERCENTAGE, NAME, diskID);
      metricService.remove(MetricType.AUTO_GAUGE, DISK_IO_QUEUE_SIZE, NAME, diskID);
    }

    // metrics for datanode and config node
    metricService.remove(
        MetricType.AUTO_GAUGE, PROCESS_IO_SIZE, FROM, processName, NAME, ACTUAL_READ);
    metricService.remove(
        MetricType.AUTO_GAUGE, PROCESS_IO_SIZE, FROM, processName, NAME, ACTUAL_WRITE);
    metricService.remove(
        MetricType.AUTO_GAUGE, PROCESS_IO_SIZE, FROM, processName, NAME, ATTEMPT_READ);
    metricService.remove(
        MetricType.AUTO_GAUGE, PROCESS_IO_SIZE, FROM, processName, NAME, ATTEMPT_WRITE);
    metricService.remove(MetricType.AUTO_GAUGE, PROCESS_IO_OPS, FROM, processName, NAME, READ);
    metricService.remove(MetricType.AUTO_GAUGE, PROCESS_IO_OPS, FROM, processName, NAME, WRITE);
  }
}
