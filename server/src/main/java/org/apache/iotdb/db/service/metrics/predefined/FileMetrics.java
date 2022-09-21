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

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.enums.Metric;
import org.apache.iotdb.db.service.metrics.enums.Tag;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
    metricService.getOrCreateAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getWalFileTotalSize,
        Tag.NAME.toString(),
        "wal");
    metricService.getOrCreateAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getSequenceFileTotalSize,
        Tag.NAME.toString(),
        "seq");
    metricService.getOrCreateAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getUnsequenceFileTotalSize,
        Tag.NAME.toString(),
        "unseq");
    metricService.getOrCreateAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getWalFileTotalCount,
        Tag.NAME.toString(),
        "wal");
    metricService.getOrCreateAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getSequenceFileTotalCount,
        Tag.NAME.toString(),
        "seq");
    metricService.getOrCreateAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.IMPORTANT,
        this,
        FileMetrics::getUnsequenceFileTotalCount,
        Tag.NAME.toString(),
        "unseq");

    // finally start to update the value of some metrics in async way
    if (metricService.isEnable() && null == currentServiceFuture) {
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

    metricService.remove(MetricType.GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "wal");
    metricService.remove(MetricType.GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.GAUGE, Metric.FILE_SIZE.toString(), Tag.NAME.toString(), "unseq");
    metricService.remove(
        MetricType.GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "wal");
    metricService.remove(
        MetricType.GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "seq");
    metricService.remove(
        MetricType.GAUGE, Metric.FILE_COUNT.toString(), Tag.NAME.toString(), "unseq");
  }

  private void collect() {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    String[] walDirs = CommonDescriptor.getInstance().getConfig().getWalDirs();
    walFileTotalSize = WALManager.getInstance().getTotalDiskUsage();
    sequenceFileTotalSize =
        Stream.of(dataDirs)
            .mapToLong(
                dir -> {
                  dir += File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME;
                  return FileUtils.getDirSize(dir);
                })
            .sum();
    unsequenceFileTotalSize =
        Stream.of(dataDirs)
            .mapToLong(
                dir -> {
                  dir += File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME;
                  return FileUtils.getDirSize(dir);
                })
            .sum();
    walFileTotalCount =
        Stream.of(walDirs)
            .mapToLong(
                dir -> {
                  File walFolder = new File(dir);
                  if (walFolder.exists()) {
                    File[] walNodeFolders = walFolder.listFiles(File::isDirectory);
                    long result = 0L;
                    if (null != walNodeFolders) {
                      for (File walNodeFolder : walNodeFolders) {
                        if (walNodeFolder.exists() && walNodeFolder.isDirectory()) {
                          try {
                            result +=
                                org.apache.commons.io.FileUtils.listFiles(walFolder, null, true)
                                    .size();
                          } catch (UncheckedIOException exception) {
                            // do nothing
                            logger.debug(
                                "Failed when count wal folder {}: ",
                                walNodeFolder.getName(),
                                exception);
                          }
                        }
                      }
                    }
                    return result;
                  } else {
                    return 0L;
                  }
                })
            .sum();
    sequenceFileTotalCount =
        Stream.of(dataDirs)
            .mapToLong(
                dir -> {
                  dir += File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME;
                  File folder = new File(dir);
                  if (folder.exists()) {
                    try {
                      return org.apache.commons.io.FileUtils.listFiles(
                              new File(dir), new String[] {"tsfile"}, true)
                          .size();
                    } catch (UncheckedIOException exception) {
                      // do nothing
                      logger.debug("Failed when count sequence tsfile: ", exception);
                    }
                  }
                  return 0L;
                })
            .sum();
    unsequenceFileTotalCount =
        Stream.of(dataDirs)
            .mapToLong(
                dir -> {
                  dir += File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME;
                  File folder = new File(dir);
                  if (folder.exists()) {
                    try {
                      return org.apache.commons.io.FileUtils.listFiles(
                              new File(dir), new String[] {"tsfile"}, true)
                          .size();
                    } catch (UncheckedIOException exception) {
                      // do nothing
                      logger.debug("Failed when count unsequence tsfile: ", exception);
                    }
                  }
                  return 0L;
                })
            .sum();
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
