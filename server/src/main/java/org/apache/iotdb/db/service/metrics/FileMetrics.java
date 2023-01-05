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
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileMetrics.class);
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private Future<?> currentServiceFuture;
  private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
  private final Runtime runtime = Runtime.getRuntime();
  private long walFileTotalSize = 0L;
  private long walFileTotalCount = 0L;
  private long sequenceFileTotalSize = 0L;
  private long sequenceFileTotalCount = 0L;
  private long unsequenceFileTotalSize = 0L;
  private long unsequenceFileTotalCount = 0L;

  private long innerSeqCompactionTempFileSize = 0L;
  private long innerUnseqCompactionTempFileSize = 0L;
  private long crossCompactionTempFileSize = 0L;
  private long innerSeqCompactionTempFileNum = 0L;
  private long innerUnseqCompactionTempFileNum = 0L;
  private long crossCompactionTempFileNum = 0L;

  private long openFileNumbers = 0L;
  private String[] getOpenFileNumberCommand;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindTsFileMetrics(metricService);
    bindWalFileMetrics(metricService);
    bindCompactionFileMetrics(metricService);
    bindSystemRelatedMetrics(metricService);
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

  private void bindTsFileMetrics(AbstractMetricService metricService) {
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
  }

  private void bindWalFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getWalFileTotalSize,
        Tag.NAME.toString(),
        "wal");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getWalFileTotalCount,
        Tag.NAME.toString(),
        "wal");
  }

  private void bindCompactionFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getInnerSeqCompactionTempFileNum,
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getInnerUnseqCompactionTempFileNum,
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getCrossCompactionTempFileNum,
        Tag.NAME.toString(),
        "cross-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getInnerSeqCompactionTempFileSize,
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getInnerUnseqCompactionTempFileSize,
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getCrossCompactionTempFileSize,
        Tag.NAME.toString(),
        "cross-temp");
  }

  private void bindSystemRelatedMetrics(AbstractMetricService metricService) {
    if ((METRIC_CONFIG.getSystemType() == SystemType.LINUX
            || METRIC_CONFIG.getSystemType() == SystemType.MAC)
        && METRIC_CONFIG.getPid().length() != 0) {
      this.getOpenFileNumberCommand =
          new String[] {
            "/bin/sh", "-c", String.format("lsof -p %s | wc -l", METRIC_CONFIG.getPid())
          };
      metricService.createAutoGauge(
          Metric.FILE_COUNT.toString(),
          MetricLevel.IMPORTANT,
          this,
          FileMetrics::getOpenFileNumbers,
          Tag.NAME.toString(),
          "open");
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    // first stop to update the value of some metrics in async way
    if (currentServiceFuture != null) {
      currentServiceFuture.cancel(true);
      currentServiceFuture = null;
    }
    unbindTsFileMetrics(metricService);
    unbindWalMetrics(metricService);
    unbindCompactionMetrics(metricService);
    unbindSystemRelatedMetrics(metricService);
  }

  private void unbindTsFileMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "unseq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "unseq");
  }

  private void unbindWalMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "wal");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "wal");
  }

  private void unbindCompactionMetrics(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "inner-seq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.FILE_COUNT.toString(),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "cross-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "inner-seq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.FILE_SIZE.toString(),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "cross-temp");
  }

  private void unbindSystemRelatedMetrics(AbstractMetricService metricService) {
    if ((METRIC_CONFIG.getSystemType() == SystemType.LINUX
            || METRIC_CONFIG.getSystemType() == SystemType.MAC)
        && METRIC_CONFIG.getPid().length() != 0) {
      metricService.remove(
          MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "open");
    }
  }

  private void collect() {
    // update TsFile Metrics
    TsFileMetricManager tsFileMetricManager = TsFileMetricManager.getInstance();
    sequenceFileTotalSize = tsFileMetricManager.getFileSize(true);
    unsequenceFileTotalSize = tsFileMetricManager.getFileSize(false);
    sequenceFileTotalCount = tsFileMetricManager.getFileNum(true);
    unsequenceFileTotalCount = tsFileMetricManager.getFileNum(false);
    // update wal Metrics
    walFileTotalSize = WALManager.getInstance().getTotalDiskUsage();
    walFileTotalCount = WALManager.getInstance().getTotalFileNum();
    // update compaction Metrics
    innerSeqCompactionTempFileSize = tsFileMetricManager.getInnerCompactionTempFileSize(true);
    innerUnseqCompactionTempFileSize = tsFileMetricManager.getInnerCompactionTempFileSize(false);
    crossCompactionTempFileSize = tsFileMetricManager.getCrossCompactionTempFileSize();
    innerSeqCompactionTempFileNum = tsFileMetricManager.getInnerCompactionTempFileNum(true);
    innerUnseqCompactionTempFileNum = tsFileMetricManager.getInnerCompactionTempFileNum(false);
    crossCompactionTempFileNum = tsFileMetricManager.getCrossCompactionTempFileNum();
    // update open file number
    try {
      if ((METRIC_CONFIG.getSystemType() == SystemType.LINUX
              || METRIC_CONFIG.getSystemType() == SystemType.MAC)
          && METRIC_CONFIG.getPid().length() != 0) {
        Process process = runtime.exec(getOpenFileNumberCommand);
        StringBuilder result = new StringBuilder();
        try (BufferedReader input =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line = "";
          while ((line = input.readLine()) != null) {
            result.append(line);
          }
        }
        openFileNumbers = Long.parseLong(result.toString().trim());
      }
    } catch (IOException e) {
      LOGGER.error("Failed to get open file number, because ", e);
    }
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

  public long getInnerSeqCompactionTempFileSize() {
    return innerSeqCompactionTempFileSize;
  }

  public long getInnerUnseqCompactionTempFileSize() {
    return innerUnseqCompactionTempFileSize;
  }

  public long getCrossCompactionTempFileSize() {
    return crossCompactionTempFileSize;
  }

  public long getInnerSeqCompactionTempFileNum() {
    return innerSeqCompactionTempFileNum;
  }

  public long getInnerUnseqCompactionTempFileNum() {
    return innerUnseqCompactionTempFileNum;
  }

  public long getCrossCompactionTempFileNum() {
    return crossCompactionTempFileNum;
  }

  public long getOpenFileNumbers() {
    return openFileNumbers;
  }
}
