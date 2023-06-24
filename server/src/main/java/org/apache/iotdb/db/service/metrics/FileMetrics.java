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
import org.apache.iotdb.db.engine.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.MetricConstant;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("java:S6548") // do not warn about singleton class
public class FileMetrics implements IMetricSet {
  private static final Logger log = LoggerFactory.getLogger(FileMetrics.class);
  private static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static final WALManager WAL_MANAGER = WALManager.getInstance();
  private final Runtime runtime = Runtime.getRuntime();
  private String[] getOpenFileNumberCommand;

  private AbstractMetricService metricService = null;
  private static final String FILE_LEVEL_COUNT = "file_level_count";
  private static final String FILE_LEVEL_SIZE = "file_level_size";
  private static final String SEQUENCE = "sequence";
  private static final String UNSEQUENCE = "unsequence";
  private static final String LEVEL = "level";
  private final AtomicLong seqFileSize = new AtomicLong(0);
  private final AtomicLong unseqFileSize = new AtomicLong(0);
  private final AtomicInteger seqFileNum = new AtomicInteger(0);
  private final AtomicInteger unseqFileNum = new AtomicInteger(0);

  private final AtomicInteger modFileNum = new AtomicInteger(0);

  private final AtomicLong modFileSize = new AtomicLong(0);
  private final Map<Integer, Integer> seqLevelTsFileCountMap = new ConcurrentHashMap<>();
  private final Map<Integer, Integer> unseqLevelTsFileCountMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> seqLevelTsFileSizeMap = new ConcurrentHashMap<>();
  private final Map<Integer, Long> unseqLevelTsFileSizeMap = new ConcurrentHashMap<>();
  private long lastUpdateTime = 0;

  // compaction temporal files
  private final AtomicLong innerSeqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong innerUnseqCompactionTempFileSize = new AtomicLong(0);
  private final AtomicLong crossCompactionTempFileSize = new AtomicLong(0);
  private final AtomicInteger innerSeqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger innerUnseqCompactionTempFileNum = new AtomicInteger(0);
  private final AtomicInteger crossCompactionTempFileNum = new AtomicInteger(0);
  private AtomicBoolean hasRemainData = new AtomicBoolean(false);
  private final Map<Integer, Gauge> seqLevelCountGaugeMap = new ConcurrentHashMap<>();
  private final Map<Integer, Gauge> seqLevelSizeGaugeMap = new ConcurrentHashMap<>();
  private final Map<Integer, Gauge> unseqLevelCountGaugeMap = new ConcurrentHashMap<>();
  private final Map<Integer, Gauge> unseqLevelSizeGaugeMap = new ConcurrentHashMap<>();

  @SuppressWarnings("squid:S1075")
  private String fileHandlerCntPathInLinux = "/proc/%s/fd";

  private FileMetrics() {
    fileHandlerCntPathInLinux = String.format(fileHandlerCntPathInLinux, METRIC_CONFIG.getPid());
  }

  public static FileMetrics getInstance() {
    return FileMetricsInstanceHolder.INSTANCE;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    bindTsFileMetrics(metricService);
    bindWalFileMetrics(metricService);
    bindCompactionFileMetrics(metricService);
    bindSystemRelatedMetrics(metricService);
  }

  private void bindTsFileMetrics(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getFileSize(true),
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getFileSize(false),
        Tag.NAME.toString(),
        "unseq");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getModFileSize,
        Tag.NAME.toString(),
        "mods");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getFileNum(true),
        Tag.NAME.toString(),
        "seq");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getFileNum(false),
        Tag.NAME.toString(),
        "unseq");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getModFileNum,
        Tag.NAME.toString(),
        "mods");
    checkIfThereRemainingData();
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
        this,
        o -> o.getInnerCompactionTempFileSize(true),
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileSize(false),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_SIZE.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getCrossCompactionTempFileSize,
        Tag.NAME.toString(),
        "cross-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileNum(true),
        Tag.NAME.toString(),
        "inner-seq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        o -> o.getInnerCompactionTempFileNum(false),
        Tag.NAME.toString(),
        "inner-unseq-temp");
    metricService.createAutoGauge(
        Metric.FILE_COUNT.toString(),
        MetricLevel.CORE,
        this,
        FileMetrics::getCrossCompactionTempFileNum,
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
    long fdCount = 0;
    try {
      if (METRIC_CONFIG.getSystemType() == SystemType.LINUX) {
        // count the fd in the system directory instead of
        // calling runtime.exec() which could be much slower
        File fdDir = new File(fileHandlerCntPathInLinux);
        if (fdDir.exists()) {
          File[] fds = fdDir.listFiles();
          fdCount = fds == null ? 0 : fds.length;
        }
      } else if ((METRIC_CONFIG.getSystemType() == SystemType.MAC)
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
        fdCount = Long.parseLong(result.toString().trim());
      }
    } catch (IOException e) {
      log.warn("Failed to get open file number, because ", e);
    }
    return fdCount;
  }

  // following are update functions for tsfile metrics
  public void addFile(long size, boolean seq, String name) {
    updateGlobalCountAndSize(size, 1, seq);
    try {
      TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
      int level = tsFileName.getInnerCompactionCnt();
      updateLevelCountAndSize(size, 1, seq, level);
    } catch (IOException e) {
      log.warn("Unexpected error occurred when getting tsfile name", e);
    }
  }

  private void updateGlobalCountAndSize(long sizeDelta, int countDelta, boolean seq) {
    if (seq) {
      seqFileSize.getAndAdd(sizeDelta);
      seqFileNum.getAndAdd(countDelta);
    } else {
      unseqFileSize.getAndAdd(sizeDelta);
      unseqFileNum.getAndAdd(countDelta);
    }
  }

  private void updateLevelCountAndSize(long sizeDelta, int countDelta, boolean seq, int level) {
    int count = 0;
    long totalSize = 0;
    if (seq) {
      count =
          seqLevelTsFileCountMap.compute(level, (k, v) -> v == null ? countDelta : v + countDelta);
      totalSize =
          seqLevelTsFileSizeMap.compute(level, (k, v) -> v == null ? sizeDelta : v + sizeDelta);
    } else {
      count =
          unseqLevelTsFileCountMap.compute(
              level, (k, v) -> v == null ? countDelta : v + countDelta);
      totalSize =
          unseqLevelTsFileSizeMap.compute(level, (k, v) -> v == null ? sizeDelta : v + sizeDelta);
    }
    updateLevelFileInfoInMetricService(totalSize, count, seq, level);
  }

  private void updateLevelFileInfoInMetricService(
      long totalSize, int count, boolean seq, int level) {
    if (metricService != null) {
      updateCountGauge(
          level,
          count,
          seq ? seqLevelCountGaugeMap : unseqLevelCountGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE);
      updateSizeGauge(
          level,
          totalSize,
          seq ? seqLevelSizeGaugeMap : unseqLevelSizeGaugeMap,
          seq ? SEQUENCE : UNSEQUENCE);
      checkIfThereRemainingData();
    } else {
      // the metric service has not been set yet
      hasRemainData.set(true);
    }
  }

  private void checkIfThereRemainingData() {
    if (hasRemainData.get()) {
      synchronized (this) {
        if (hasRemainData.get()) {
          hasRemainData.set(false);
          updateRemainData();
        }
      }
    }
  }

  private void updateCountGauge(
      int level, int count, Map<Integer, Gauge> countGaugeMap, String orderStr) {
    countGaugeMap
        .computeIfAbsent(
            level,
            l ->
                metricService.getOrCreateGauge(
                    FILE_LEVEL_COUNT,
                    MetricLevel.CORE,
                    Tag.TYPE.toString(),
                    orderStr,
                    LEVEL,
                    String.valueOf(level)))
        .set(count);
  }

  private void updateSizeGauge(
      int level, long size, Map<Integer, Gauge> sizeGaugeMap, String orderStr) {
    sizeGaugeMap
        .computeIfAbsent(
            level,
            l ->
                metricService.getOrCreateGauge(
                    FILE_LEVEL_SIZE,
                    MetricLevel.CORE,
                    Tag.TYPE.toString(),
                    orderStr,
                    LEVEL,
                    String.valueOf(level)))
        .set(size);
  }

  public void deleteFile(long[] sizeList, boolean seq, List<String> names) {
    long totalSize = 0;
    for (long size : sizeList) {
      totalSize += size;
    }
    updateGlobalCountAndSize(-totalSize, -sizeList.length, seq);
    for (int i = 0, length = names.size(); i < length; ++i) {
      int level = -1;
      String name = names.get(i);
      long size = sizeList[i];
      try {
        TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(name);
        level = tsFileName.getInnerCompactionCnt();
        updateLevelCountAndSize(-size, -1, seq, level);
      } catch (IOException e) {
        log.warn("Unexpected error occurred when getting tsfile name", e);
      }
    }
  }

  private void updateRemainData() {
    for (Map.Entry<Integer, Integer> entry : seqLevelTsFileCountMap.entrySet()) {
      updateCountGauge(entry.getKey(), entry.getValue(), seqLevelCountGaugeMap, SEQUENCE);
    }
    for (Map.Entry<Integer, Long> entry : seqLevelTsFileSizeMap.entrySet()) {
      updateSizeGauge(entry.getKey(), entry.getValue(), seqLevelSizeGaugeMap, SEQUENCE);
    }
    for (Map.Entry<Integer, Integer> entry : unseqLevelTsFileCountMap.entrySet()) {
      updateCountGauge(entry.getKey(), entry.getValue(), unseqLevelCountGaugeMap, UNSEQUENCE);
    }
    for (Map.Entry<Integer, Long> entry : unseqLevelTsFileSizeMap.entrySet()) {
      updateSizeGauge(entry.getKey(), entry.getValue(), unseqLevelSizeGaugeMap, UNSEQUENCE);
    }
  }

  public long getFileSize(boolean seq) {
    return seq ? seqFileSize.get() : unseqFileSize.get();
  }

  public long getFileNum(boolean seq) {
    return seq ? seqFileNum.get() : unseqFileNum.get();
  }

  public int getModFileNum() {
    return modFileNum.get();
  }

  public long getModFileSize() {
    return modFileSize.get();
  }

  public void increaseModFileNum(int num) {
    modFileNum.addAndGet(num);
  }

  public void decreaseModFileNum(int num) {
    modFileNum.addAndGet(-num);
  }

  public void increaseModFileSize(long size) {
    modFileSize.addAndGet(size);
  }

  public void decreaseModFileSize(long size) {
    modFileSize.addAndGet(-size);
  }

  public long getInnerCompactionTempFileSize(boolean seq) {
    updateCompactionTempSize();
    return seq ? innerSeqCompactionTempFileSize.get() : innerUnseqCompactionTempFileSize.get();
  }

  private synchronized void updateCompactionTempSize() {
    if (System.currentTimeMillis() - lastUpdateTime <= MetricConstant.UPDATE_INTERVAL) {
      return;
    }
    lastUpdateTime = System.currentTimeMillis();

    innerSeqCompactionTempFileSize.set(0);
    innerSeqCompactionTempFileNum.set(0);
    innerUnseqCompactionTempFileSize.set(0);
    innerUnseqCompactionTempFileNum.set(0);
    crossCompactionTempFileSize.set(0);
    crossCompactionTempFileNum.set(0);

    List<AbstractCompactionTask> runningTasks =
        CompactionTaskManager.getInstance().getRunningCompactionTaskList();
    for (AbstractCompactionTask task : runningTasks) {
      CompactionTaskSummary summary = task.getSummary();
      if (task instanceof InnerSpaceCompactionTask) {
        if (task.isInnerSeqTask()) {
          innerSeqCompactionTempFileSize.addAndGet(summary.getTemporalFileSize());
          innerSeqCompactionTempFileNum.addAndGet(1);
        } else {
          innerUnseqCompactionTempFileSize.addAndGet(summary.getTemporalFileSize());
          innerUnseqCompactionTempFileNum.addAndGet(1);
        }
      } else {
        crossCompactionTempFileSize.addAndGet(summary.getTemporalFileSize());
        crossCompactionTempFileNum.addAndGet(summary.getTemporalFileNum());
      }
    }
  }

  public long getCrossCompactionTempFileSize() {
    updateCompactionTempSize();
    return crossCompactionTempFileSize.get();
  }

  public long getInnerCompactionTempFileNum(boolean seq) {
    updateCompactionTempSize();
    return seq ? innerSeqCompactionTempFileNum.get() : innerUnseqCompactionTempFileNum.get();
  }

  public long getCrossCompactionTempFileNum() {
    updateCompactionTempSize();
    return crossCompactionTempFileNum.get();
  }

  private static class FileMetricsInstanceHolder {
    private static final FileMetrics INSTANCE = new FileMetrics();

    private FileMetricsInstanceHolder() {}
  }
}
