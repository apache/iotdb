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
import org.apache.iotdb.db.engine.TsFileMetricManager;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileMetrics implements IMetricSet {
  private static final Logger logger = LoggerFactory.getLogger(FileMetrics.class);
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private long walFileTotalSize = 0L;
  private long walFileTotalCount = 0L;
  private long sequenceFileTotalSize = 0L;
  private long sequenceFileTotalCount = 0L;
  private long unsequenceFileTotalSize = 0L;
  private long unsequenceFileTotalCount = 0L;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getWalFileTotalSize,
        Tag.NAME.toString(),
        "wal");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getSequenceFileTotalSize,
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getUnsequenceFileTotalSize,
        Tag.NAME.toString(),
        "unseq");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getWalFileTotalCount,
        Tag.NAME.toString(),
        "wal");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getSequenceFileTotalCount,
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getUnsequenceFileTotalCount,
        Tag.NAME.toString(),
        "unseq");

    // finally start to update the value of some metrics in async way
    if (null == currentServiceFuture) {
      currentServiceFuture =
          ScheduledExecutorUtil.safelyScheduleAtFixedRate(
              service,
              this::collect,
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
    if (currentServiceFuture != null) {
      currentServiceFuture.cancel(true);
      currentServiceFuture = null;
    }

    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "wal");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "unseq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "wal");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "unseq");
  }

  private void collect() {
    walFileTotalSize = WALManager.getInstance().getTotalDiskUsage();
    sequenceFileTotalSize = TsFileMetricManager.getInstance().getFileSize(true);
    unsequenceFileTotalSize = TsFileMetricManager.getInstance().getFileSize(false);
    walFileTotalCount = WALManager.getInstance().getTotalFileNum();
    sequenceFileTotalCount = TsFileMetricManager.getInstance().getFileNum(true);
    unsequenceFileTotalCount = TsFileMetricManager.getInstance().getFileNum(false);
  }

  public long getWalFileTotalSize() {
    return walFileTotalSize;
  }

  public long getWalFileTotalCount() {
    return walFileTotalCount;
  }

  public long getSequenceFileTotalSize() {
    return sequenceFileTotalSize;
  }

  public long getSequenceFileTotalCount() {
    return sequenceFileTotalCount;
  }

  public long getUnsequenceFileTotalSize() {
    return unsequenceFileTotalSize;
  }

  public long getUnsequenceFileTotalCount() {
    return unsequenceFileTotalCount;
  }
}
