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

public class FileMetrics implements IMetricSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileMetrics.class);
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final TsFileMetricManager TS_FILE_METRIC_MANAGER =
      TsFileMetricManager.getInstance();
  private static final WALManager WAL_MANAGER = WALManager.getInstance();
  private final Runtime runtime = Runtime.getRuntime();
  private String[] getOpenFileNumberCommand;

  @Override
  public void bindTo(AbstractMetricService metricService) {
    bindTsFileMetrics(metricService);
    bindWalFileMetrics(metricService);
    bindCompactionFileMetrics(metricService);
    bindSystemRelatedMetrics(metricService);
  }

  private void bindTsFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getFileSize(true),
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getFileSize(false),
        Tag.NAME.toString(),
        "unseq");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        TsFileMetricManager::getModFileSize,
        Tag.NAME.toString(),
        "mods");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getFileNum(true),
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getFileNum(false),
        Tag.NAME.toString(),
        "unseq");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        TsFileMetricManager::getModFileNum,
        Tag.NAME.toString(),
        "mods");
  }

  private void bindWalFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        WAL_MANAGER,
        WALManager::getTotalDiskUsage,
        Tag.NAME.toString(),
        "wal");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        WAL_MANAGER,
        WALManager::getTotalFileNum,
        Tag.NAME.toString(),
        "wal");
  }

  private void bindCompactionFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getInnerCompactionTempFileSize(true),
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getInnerCompactionTempFileSize(false),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        TsFileMetricManager::getCrossCompactionTempFileSize,
        Tag.NAME.toString(),
        "cross-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getInnerCompactionTempFileNum(true),
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        o -> o.getInnerCompactionTempFileNum(false),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        TS_FILE_METRIC_MANAGER,
        TsFileMetricManager::getCrossCompactionTempFileNum,
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
          MetricLevel.CORE,
          this,
          FileMetrics::getOpenFileHandlersNumber,
          Tag.NAME.toString(),
          "open_file_handlers");
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
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
        MetricType.AUTO_GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "mods");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "unseq");
    metricService.remove(
        MetricType.AUTO_GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "mods");
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
          MetricType.AUTO_GAUGE,
          Metric.FILE_COUNT.toString(),
          Tag.NAME.toString(),
          "open_file_handlers");
    }
  }

  private long getOpenFileHandlersNumber() {
    try {
      if ((METRIC_CONFIG.getSystemType() == SystemType.LINUX
              || METRIC_CONFIG.getSystemType() == SystemType.MAC)
          && METRIC_CONFIG.getPid().length() != 0) {
        Process process = runtime.exec(getOpenFileNumberCommand);
        StringBuilder result = new StringBuilder();
        try (BufferedReader input =
            new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          String line;
          while ((line = input.readLine()) != null) {
            result.append(line);
          }
        }
        return Long.parseLong(result.toString().trim());
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to get open file number, because ", e);
    }
    return 0L;
  }
}
